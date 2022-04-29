#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan  7 12:12:25 2022

@author: piet
"""


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
##Feb 23 2022, version 0.99 Program wals through csv file and get info from websites identfied as drone website, 
##COUNTRY SPECIFIC CODE
##Add list of start of many Spanish street names, spaces in activity names and bool of drone comp included
##Imporved and more geeral address capture, deals with \xa0 in text (replaced by space now), removes soups in functions (try to reduce memory use)
##Adjusted check email, phone and social media functions
##Added incountry, is Drone and isCOmpnay function, tweaked address and activities functions
##Checked phone number regexs, imporved dealing with position Irisch zipcode
##Assed isSelected, for records of Drone companies in Country, updated VAT function and inCOuntry (to deal with Northern Ireland)

#Load libraries 
import os
import sys
import re
import time
import pandas
import nltk
##import random
import multiprocessing as mp
import numpy as np
import configparser
import langdetect
import collections
##import timeout_decorator
from operator import itemgetter

##Get directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Final")
localDir = os.getcwd()

##get regex for url matching in documents
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"
genMail2 = r"[a-zA-Z0-9_\-.]+@[a-zA-Z0-9_\-]+[.][a-z]{2,4}"
##zipIre = "([AC-FHKNPRTV-Y]{1}[0-9]{2}|D6W|D[1-9]{1})(\s|[0-9AC-FHKNPRTV-Y]{4})"
##this regex truely matches Irish zipcodes (AND NOT DRONE NUMBERS)
zipIre = r"([AC-FHKNPRTV-Y]\d{2}|D6W|D[1-9]{1})\s?([0-9AC-FHKNPRTV-Y]{4})\s"
##Define date regex for whois
regDate2 = "19[0-9]{2}-[0-9]{1,2}-[0-9]{1,2}|20[0-9]{2}-[0-9]{1,2}-[0-9]{1,2}"

##Factor to multiply the number of cores with for parallel scraping (not for search engine use)
multiTimes = 4 ##number to multiply number of parallel scraping sessions
##waitTime = 180 
logFile = ""

##Define tristate object inCountry
class inCountry(object):
    def __init__(self, value=None):
       if any(value is v for v in (True, False, None)):
          self.value = value
       else:
           raise ValueError("inCountry must be True, False, or None")
    def __eq__(self, other):
       return (self.value is other.value if isinstance(other, inCountry)
               else self.value is other)
    def __ne__(self, other):
       return not self == other
    def __nonzero__(self):   # Python 3: __bool__()
       raise TypeError("inCountry object may not be used as a Boolean")
    def __str__(self):
        return str(self.value)
    def __repr__(self):
        return "inCountry(%s)" % self.value

##Get html tags
def gethtmltags(soup):
    ''' search for all html tags used in this website '''
    list_html_tags = []
    for tag in soup.find_all():
        list_html_tags.append(tag.name)
    list_html_tags = set(list_html_tags)
    
    return list_html_tags

##Extract visible text from soup (remove any scripts and styles) and include DOT between lines
def visibleText(soup):
    ''' kill all the scripts and style and return texts '''
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    text = soup.get_text()
    ##split text
    lines = (line.strip() for line in text.splitlines())
    ##lines2 = (line.strip() for line in lines if not line.strip() == "")
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '. '.join(chunk for chunk in chunks if chunk)
    
    return text

def processText(text, tags):
    wordsText = ""
    text_lang = ""
    if len(text) != 0:
        ##Try to determine language
        try:
            ##determine language (2 options language as in ini-file or english)
            text_lang = langdetect.detect(text)
            
            ##convert lang reciebed
            if text_lang == "ie":
                text_lang = "english"
            else:
                text_lang = "english"

        except:
            text_lang = language 
        
        ##Convert text (KEEP CASE)
        words = text.split()
        for word in words:
            ##remove digits and punctuationmarks, but keep letters and diacretics ones
            word = re.compile('[^\D]').sub(' ', word) ##replace digits with space
            word = re.compile('[\W_]').sub(' ', word) ##replace punctiation marks
            ##remove stopwords for language
            if not word in nltk.corpus.stopwords.words(text_lang):                
                for w in word.split():
                    ##only keep words of 2 and more characters
                    if len(w) > 1:
                        if not w in tags:
                            wordsText += " " + w.strip()
    
    ##Remove leading and lagging texts
    wordsText = wordsText.strip()    
    
    ##return cleaned text and language detected
    return wordsText, text_lang

##Convert to text and store
def convertSoup(soup):
    text = ""
    text_lang = "" ##language ##default from ini-file
    
    ##Process content
    if len(str(soup)) > 1:
        try:
            ##Extract visible and relevan words from soup and determine language
            html_tags = gethtmltags(soup)
            text = visibleText(soup)
            text, text_lang = processText(text.lower(), html_tags)
                   
        except:
            ##An error occured
            text_lang = ""
    else:
        text_lang = ""
        
    return text, text_lang

##replace characters with character with leading laggign space
def preprocessText(text):
    text = text.replace('\xa0', ' ')
    test = ' '
    prevChar = ''
    for x in text:
        if x in "\'\".,;:()?!/&@#$%^*_+={}[]|\<>~`€":
            if prevChar == ' ':
                test += x + " "
            else:
                test += " " + x + " "
            prevChar = ' '
        else:
            if x == ' ' and prevChar == ' ':
                pass
            else:
                test += x
            prevChar = x
    test += ' '

    return(test)

##Needed by social media checks 
def PreProcessList(urls_list, country):
    urls_cleaned = []
    urls_exclude = []
    
    ##1. Check for multiple links in 1 url
    links =[]
    for url in urls_list:
        ##1a. Check for doubel http at start
        if url.startswith('http://http'):
            url = url[7:len(url)]
        if url.startswith('https://http'):
            url = url[8:len(url)]
        
        ##1b. Check for multiple links in 1 url 
        if url.count("http") > 1:
            urls = url.split("http")
            for u in urls:
                ##Ignore empty urls
                if len(u) > 0:
                    ##replace essential codes with correct : and / signs
                    u = u.replace('%3A', ':')
                    u = u.replace('%2F', '/')
                    u = "http" + u
                    us = df.checkUrls(u)
                    if not us == []:
                        for u1 in us:
                            if not u1 == '':
                                links.append(u1)                
        else:
            ##Check http correctness and correct if that is not the case
            us = df.checkUrls(url)
            if not us == []:
               for u1 in us:
                   if not u1 == '':
                       links.append(u1)
        ##1. sIt is ESSENTIAL TO FIRST SPLIT ANY MULTIPLE URL CONTAINING url

    ##2. Check each link found with regEx
    for l in links:
        ##Check if url is a valid url
        l2 = re.findall(genUrl, l)
        if not l2 == []: 
            l3 = max(l2[0], key=len) ##l2[0][0] ##get longest link in first list
            if len(l3) > 0:
                ##Remove last dot (if included)
                if l3.endswith("."):
                    l3 = l3[0:len(l3)-1]
                ##Check for missing http (just in case)
                if not l3.startswith("http"):
                    l3 = "http://" + l3
                ##Add to list
                urls_cleaned.append(l3)
                ##print(l3)
    
    ##3. remove
    ##3a. Remove duplicates
    urls_cleaned = list(set(urls_cleaned))

    ##3b. Remove links in exclusion list
    urls_cleaned2 = []
    for url in urls_cleaned:##Always add soup of first page
        ##Exclude any urls included in urls_exclude
        if not any(url.lower().find(x) > 0 for x in urls_exclude):
            urls_cleaned2.append(url)
        
    ##3c. ALso check for / or no slah variant and remove domains not referening to country being studied
    urls_cleaned2a, urlsNot = df.cleanLinks(urls_cleaned2, country, True)

    return(urls_cleaned2a)

##get relevant urls (those that may contain detailed info of company)
def getPageUrls(soup, vurl):
    links = []
    
    ##get external links
    inter, exter = df.extractLinks(soup, vurl, False)
    dom = df.getDomain(vurl)
                            
    ##In case inter is empty and exter not get dom part
    if len(inter) == 0 and len(exter) > 0:
        ##Check external links for domain
        for ex in exter:
            if ex.lower().startswith(dom):
                if not ex.lower() in inter:
                    inter.append(ex.lower())
    
    ##remove dom and dom + "/" links
    if dom in inter:
        inter.remove(dom)
    if dom + "/" in inter:
        inter.remove(dom + "/")
                           
    ##sort links (look at the short urls first)
    inter2 = list(sorted(inter, key = len))
    ##Make sure content words are available
    if len(cont_words) > 0:
        ##Check internal links for contact page                                
        for link in inter2:
            ##Search for contact of about etc.
            linkC = link.replace(dom, '')
            linkC = linkC.lower()
            ##Check if cont_words occur in linkC (lowercase)
            wCon = [w for w in cont_words if linkC.lower().find(w) > 0]
            ##Look for contact page about us page or who we are page (should contain contact info and location/city name)
            if len(wCon) > 0:
                if link.find("?") > -1:
                    ##get part for ?
                    posQ = link.find("?")
                    link = link[0:posQ]
                ##add url to links
                if not link in links:
                    links.append(link)
    
    ##Check number of links found
    if len(links) > 4:
        ##sort links (look at the short urls first)
        links2 = list(sorted(links, key = len))
        ##get first 4 shortest links
        links = links2[0:4]
                
    return(links)

##Get soups of all relevant pages
def getSoups(soup, vurl):
    ##Create soup list
    pageSoups = []
    
    ##Check if something is provided
    if len(str(soup)) > 1:
        ##Always add soup of first page
        pageSoups.append(soup)
    
        ##Get all relevant urls from first soup (will be contact pages)    
        pageUrls = getPageUrls(soup, vurl)
    
        ##Get soups of pageUrls
        if len(pageUrls) > 0:
            ##get soups
            for link in pageUrls:
                ##Get soup
                soup2, vurl2 = df.createsoup(link)
                
                ##Check soup content
                if not soup2 == 0:
                    ##Add to soup list
                    pageSoups.append(soup2)

    ##Check content
    if len(pageSoups) == 1:
        if pageSoups[0] == 0:
            pageSoups = []
            
    return(pageSoups)
    
##Check for name of company
def checkName(soups):
    ##Check for name of company
    names = []
    names1 = ""
    count = 0
    foundBool = False
    
    ##Extract from page
    for soup in soups:    
        ##Check for meta content
        if str(soup).find("meta content") > -1:
            ##Check for met content title or descirption
            try:
                ##get title from meta data
                name = soup.find("meta", property="og:title")
                if not name == None:
                    ##get content
                    names1 = name['content']
            except:
                ##An error occurred
                pass
            finally:
                if len(names1) > 0:
                    foundBool = True
                    
        ##Check alternatve wya, in case met info is not available
        if not foundBool:
            ##print(count)
            ##1. In case object has a title tag
            try:
                ##get title of page
                name = soup.title.getText()  
                ##check result
                if len(name) > 0:
                    name = name.replace("\t", " ")
                    name = name.replace("\r", " ")
                    name = name.replace("\n", " ")
                    name = name.strip()
                    name = name.replace("  ", " ")
            
                    ##add to names
                    names.append(name)
                
                    if count == 0 and len(name) > 0 and names1 == "":
                        ##store title
                        names1 = name
                        ##end search
                        foundBool = True
            except:
                ##Ann error has occured, ignore
                pass 

            ##2. In case object has no title
            try:
                ##get title of page
                name = soup.select("h1")
            except:
                ##Ann error has occured, ignore
                pass 
            finally:            
                for nam in name:
                    if len(nam) > 0:
                        nam = str(nam)
                        nam = nam.replace('<h1>', '')
                        nam = nam.replace('</h1>', '')                
                        nam = nam.replace("\r", " ")
                        nam = nam.replace("\n", " ")
                        nam = nam.replace("\t", " ")
                        nam = nam.strip()
                        nam = nam.replace("  ", " ")
            
                    ##add to names
                    names.append(nam)
            ##Add 1 to count
            count += 1
        else:
            break
    
    if not foundBool and len(names) > 0:
        ##Count and sort occriding to frequency    
        ##Count occurences
        namesC = collections.Counter(names)
        ##sort words in collection
        sorted_namesC = sorted(namesC.items(), key=lambda kv: kv[1], reverse=True)
        ##get maxOcc
        maxOcc = max(x[1] for x in sorted_namesC)
        ##Check max occurence
        if maxOcc >= 3:
            maxNames = [x[0] for x in sorted_namesC if x[1] == maxOcc]
            ##sort len maxNames (increasing)
            namesMaxSort = sorted(maxNames, key = len)
            names1 = namesMaxSort[0]
        
        del namesC
        del sorted_namesC
        del maxOcc
    
    del names
    del soups
    ##return shortest text with name (with highest score)
    names1 = names1.replace(";", " ")
    return(names1)
 
def getFirstCities(text1, included2, number = 5):
    cities = []
    cityPosList = []
    ##Check input
    if len(text1) > 0 and len(included2) > 0 and number > 0:
        ##Get city names in sequence of occurence in text
        for incl in included2:
            ##get position of first occurence of incl in text1
            posIncl = text1.index(incl)
            ##deal with city names in uppercase 
            if posIncl == -1:
                posIncl = text1.index(incl.upper())
            ##Add to cityPosList
            cityPosList.append([incl, posIncl])
        
        ##sort list on position values
        sortedCityList = sorted(cityPosList, key=itemgetter(1))
        ##get number of cities
        sortedCityList = sortedCityList[0:number]
        ##get city names as list
        cities = [x[0] for x in sortedCityList]
        
    return(cities)
    
##function needed by address (reduces potetnual addresses returned)
def overlap(a, b):
    return max(i for i in range(len(b)+1) if a.endswith(b[:i]))

##@timeout_decorator.timeout(waitTime) ## If execution takes longer than 180 sec, TimeOutError is raised
##get addresses from texts, IMPROVED FUNCTION
def checkAddress2(soups, vurl):
    address = []
    
    try:
        ##Add any additonal location soups
        inter, exter = df.extractLinks(soups[0], vurl, False)
        ##Check for location info and add soup
        locsW = ['location', 'suíomh', 'suiomh', 'address', 'seoladh']
        urlsAdd = []
        ##Get additona links
        for link in inter:
            incl = list(filter(lambda x: re.findall(x, link), locsW))
            if len(incl) > 0:
                if not link in urlsAdd:
                    urlsAdd.append(link)
        ##Add soups
        if len(urlsAdd) > 0:
            ##Get all if 4 or less links
            if len(urlsAdd) <= 4:
                for link in urlsAdd:
                    ##Exclude northern irelaind pages
                    if not (link.lower().find("northern") > -1 and link.lower().find("ireland") > -1): 
                        ##Get soup of link (and vurl)
                        soup2, vurl2 = df.createsoup(link)
                        ##Add to soups
                        if len(str(soup2)) > 0:
                            if not soup2 in soups:
                                soups.append(soup2)    
                        del soup2
            else:
                ##many links found
                urlsB = []
                ##sort urls
                urlsAdd2 = list(sorted(urlsAdd, key = len))
                ##Check for country specific links
                for link in urlsAdd2:
                    ##Exclude northern irelaind pages
                    if (link.lower().find("northern") == -1 and link.lower().find("ireland") == -1): 
                        ##Check if link contains country specific link
                        res = [link for x in countryNames if link.find(x.lower()) > -1]
                        if len(res) > 0:
                            ##Add first link
                            if not res[0] in urlsB:
                                urlsB.append(res[0])
                
                ##Check effect of country filtering            
                if len(urlsB) > 4:
                    ##sort urls
                    urlsAdd2 = list(sorted(urlsB, key = len))
                    ##select first 4
                    urlsAdd2 = urlsAdd2[0:5]
                elif len(urlsB) == 0:
                    ##Get first four (Sorted)
                    urlsAdd2 = urlsAdd2[0:5]
                    
                ##Add soups of max 4 links
                for link in urlsAdd2:
                    ##Get soup of link (and vurl)
                    soup2, vurl2 = df.createsoup(link)
                    ##Add to soups
                    if len(str(soup2)) > 0:
                        if not soup2 in soups:
                            soups.append(soup2)    
                    del soup2
                
        ##Define words that indicate the start of a streetname in Spain
        ##Get addresses from soups, list of Irish names located at beginning
        ##singsLIE = ['Sráid', 'Sraid', 'Bóthar', 'Bothar', 'Búlbhard', 'Bulbhard', 'Faiche', 'Plás', 'Plas', 'Cearnóg', 'Cearnog', 'Árdan', 'Ardan', 'Páirc', 'Pairc', 'Lána', 'Lana', 'Ascaill', 'Cuardbóthar', 'Cuardbothar', 'Barra', 'Rae', ', ', '.', ':', ';', ',', '\n', '\t', '\r']
        ##Address list of english names (does not have to start at beginning)
        signsL = ['Sráid', 'Sraid', 'Bóthar', 'Bothar', 'Búlbhard', 'Bulbhard', 'Faiche', 'Plás', 'Plas', 'Cearnóg', 'Cearnog', 'Árdan', 'Ardan', 'Páirc', 'Pairc', 'Lána', 'Lana', 'Ascaill', 'Cuardbóthar', 'Cuardbothar', 'Barra', 'Rae', 'Street', 'Road', 'Boulevard', 'Green', 'Place', 'Square', 'Terrace', 'Park', 'Lane', 'Avenue', 'Bar', 'Row', 'Crescent', 'Way', 'Unit', ', ', '.', ':', ';', ',', '\n', '\t', '\r']
        ##A letter followed by 2 digits or D6W or D1 - D9 (without 1 zero) with a left space and on the right either a space or [0-9AC-FHKNPRTV-Y]{4}
            
        ##List with city names found (in acse no address can be located)
        includedCities = []
        zipCodePresence = False
        ##NOT ALL CODE THAT MATCH ARE ZIPCODE D28 is a dronr productionnumber
        ##1. check for inclusion of Irish zipcode    
        for soup in soups:
            ##get text
            text = df.visibletext(soup, False)
            text = preprocessText(text)
            textS = ""
        
            ##Check for Irish zipcode in text
            included = re.findall(" " + zipIre, text)
            ##Select left part (3 chars)
            included = [x[0].strip() for x in included]
            ##remove any duplicates
            included = list(set(included))
        
            if len(included) > 0:
                zipCodePresence = True
                if len(included) > 10:
                    included = included[0:10]
                ##get start and end of string around position of zipcode
                for incl in included:
                    ##City Name may be included multiple times, get all positions
                    posZips = [i for i in range(len(text)) if text.startswith(incl, i)]
                    ##Check amount, get first 5
                    if len(posZips) > 5:
                        posZips = posZips[0:5]
                    
                    ##for each position
                    for posZip in posZips:
                        posBeg = -1
                        ##get part left of zip code (CHECK FOR COMMON STRAAT NAMES?)
                        text0 = text[0:posZip -1]
                
                        ##get begin of streetname
                        for s in signsL:
                            posBeg = text0.rfind(s)
                            if posBeg > -1:
                                ##print(str(s) + " : " + str(posDot))
                                ##distance should not be to long
                                if len(text0) - posBeg < 100:
                                    ##get first within distance
                                    break
                    
                        ##get part right of zipcode
                        text1 = text[posZip + len(incl):]
                        ##detect if a country muncipality name occur right of zip code text, will likely be country
                        included2 = list(filter(lambda x: re.findall(" " + x + " ", text1), municL)) 
                        ##Check if something is found
                        if len(included2) == 0:
                            ##If not name might be fully capatilized
                            included2 = list(filter(lambda x: re.findall(" " + x.upper() + " ", text1.upper()), municL))
                        ##reduce included2
                        included2 = list(set(included2))
                        if "Ireland" in included2:
                            ##Check it its from Northern ireland
                            if text1.lower().find("northern ireland") > -1:
                                ##remove Ireland
                                included2.remove("Ireland")
                                
                        if len(included2) > 5:
                            ##Get first five occurences of cityNames in text1
                            included2 = getFirstCities(text1, included2, 5)
                        
                        for incl2 in included2:                        
                            ##Add to included cities
                            if not incl2 in includedCities and not incl2 == "":
                                includedCities.append(incl2)
                        
                            textS = ""
                            ##Get position of incl2 in text1
                            posCity = text1.find(incl2)
                            ##Make sure to check for fuly capital names
                            if posCity == -1:
                                posCity = text1.find(incl2.upper())
                            ##Check distance
                            if posCity < 100 and posCity >= 0:
                                ##Get position of incl2 in original text
                                posCity += len(incl) + len(text0) + 1
                                textS = text[posBeg:posCity + len(incl2)].strip()

                                ##Add address string to list (longest will be selecteat the endd)
                                if not textS in address and not textS == "":
                                    textS = textS.replace(";", " ")
                                    textS = textS.replace("\t", " ")
                                    textS = textS.replace("\r", " ")
                                    textS = textS.replace("\n", " ")
                                    address.append(textS)
             
        ##2a. Check if zipcode was fund but no address (zipcode is likely located at the end of address string)
        if len(address) == 0 or zipCodePresence:
            ##Check soups fro address
            for soup in soups:
                ##get text
                text = df.visibletext(soup, False)
                text = preprocessText(text)
                
                ##Redo zipCode search for each soup
                included = re.findall(" " + zipIre, text)
                ##Select left part (3 chars)
                included = [x[0].strip() for x in included]
                ##remove any duplicates
                included = list(set(included))
                ##print(included)
 
                ##get start and end of string around position of zipcode
                if len(included) > 0:
                    ##Limit to 10 zipcodes max
                    if len(included) > 10:
                        included = included[0:10]
               
                    for incl in included:
                        ##City Name may be included multiple times, get all positions
                        posZips = [i for i in range(len(text)) if text.startswith(incl, i)]
                        ##Check amount, get first 5
                        if len(posZips) > 5:
                            posZips = posZips[0:5]
                    
                        ##for each position
                        for posZip in posZips:
                            posBeg = -1
                            ##get part left of zip code (CHECK FOR COMMON STRAAT NAMES?)
                            text0 = text[0:posZip -1]
                
                            ##get begin of streetname
                            for s in signsL:
                                posBeg = text0.rfind(s)
                                if posBeg > -1:
                                    ##print(str(s) + " : " + str(posDot))
                                    textS = ""
                                    ##Check distance
                                    if (posZip - posBeg) < 100: ##and (posZip - posBeg) > 0:
                                        ##Get text between posZip and posBeg
                                        textS = text[posBeg:posZip + len(incl)].strip()
                                        ##print(textS)
                                        ##Add address string to list (longest will be selecteat the endd)
                                        if not textS in address and not textS == "":
                                            textS = textS.replace(";", " ")
                                            textS = textS.replace("\t", " ")
                                            textS = textS.replace("\r", " ")
                                            textS = textS.replace("\n", " ")
                                            address.append(textS)
 
        ##2b. Check findings based on municipality/city name (if no zipcode has been found)
        if len(address) == 0:
            ##Check soups for city names first
            for soup in soups:
                ##get text
                text = df.visibletext(soup, False)
                text = preprocessText(text)
                textS = ""

                ##Find city names (or country)
                ##detect if a country muncipality name occur right of zip code text
                included2 = list(filter(lambda x: re.findall(" " + x + " ", text), municL)) 
                ##Check if something is found
                if len(included2) == 0:
                    ##If not name maight be fully capatilized
                    included2 = list(filter(lambda x: re.findall(" " + x.upper() + " ", text1.upper()), municL)) 
                ##remove duplicates
                included2 = list(set(included2))
                if "Ireland" in included2:
                    ##Check it its from Northern ireland
                    if text.lower().find("northern ireland") > -1:
                        ##remove Ireland
                        included2.remove("Ireland")
                       
                if len(included2) > 5:
                    ##Get first five occurences of cityNames in text1
                    included2 = getFirstCities(text1, included2, 5)
                        
                ##get end positions
                for incl2 in included2:
                    ##Add to included cities
                    if not incl2 in includedCities and not incl2 == "":
                        includedCities.append(incl2)

                    ##City Name may be included multiple times, get all positions
                    posIncl2 = [i for i in range(len(text)) if text.startswith(incl2, i)]
                    if len(posIncl2) == 0:
                        ##Deal with fully upper names
                        posIncl2 = [i for i in range(len(text)) if text.startswith(incl2.upper(), i)]
                    
                    if len(posIncl2) > 5:
                        posIncl2 = posIncl2[0:5]
                        
                    ##For each position check for start of streetname
                    for posCity in posIncl2:
                        ##get part left of city
                        text0 = text[0:posCity-1]                    
                        ##check text0 for most right streetname start
                        ##get begin of streetname
                        for s in signsL:
                            posBeg = text0.rfind(s)
                            if posBeg > -1:
                                ##print(posBeg)
                                ##distance should not be to long
                                if posCity - posBeg < 100:                                                        
                                    ##Get text between positions
                                    textS = text[posBeg:posCity + len(incl2)].strip()
                                    #Add address string to list (longest will be selecteat the endd)
                                    if not textS in address and not textS == "":
                                        textS = textS.replace(";", " ")
                                        textS = textS.replace("\t", " ")
                                        textS = textS.replace("\r", " ")
                                        textS = textS.replace("\n", " ")
                                        address.append(textS)
                       
        ##3. postprocessing
        ##A. Remove subsets of longer text and Northern ireland addresses
        address2 = []
        for add in address:
            if not add.lower().find("northern ireland") > -1:
                inn = [s for s in address if add in s]
                if len(inn) == 1:
                    if not inn[0] in address2:
                        address2.append(inn[0])
    
        ##B. Address should have a number in text (is this the case in Ireland? May not be)
        address2b = []
        for add in address2:
            res = re.findall("\d+", add)
            if len(res) > 0:
                address2b.append(add) 
            ##elif add.find("s/n") > -1:  ##Each rirsich address should contain a number (at least from zipcode)
            ##    address2b.append(add)
        ##Check if address remain after numeric selection, if nothing rmean
        if len(address2b) == 0 and len(address2) > 0:
            address2b = address2
    
        ##C. Combine overlapping texts, so few texts remain    
        address3 = []
        if len(address2b) > 1:
            ##sort with longest text first
            address2b = sorted(address2b, key=len, reverse = True)
            ##get first text
            res = address2b[0]
            for s in address2b[1:]:
                ##determine overlap
                o = overlap(res, s)
                ##if overlap process text and add
                if o > 0:
                    res += s[o:]
                else: ##No overlap save and check next
                    if not res in address3:
                        address3.append(res)
                    ##Get next s
                    res = s
            ##add remainin res
            if not res in address3:
                address3.append(res)
        else:
            for add in address2b:
                address3.append(add)
    
        ##D. Check address for Ireland or Eire (removes any etxra stuff added after that)
        ##Make sure Northern Ireland addresses are excluded
        address4 = []
        IrelandW = ['Ireland', 'Éire', 'Eire', 'IRELAND', 'ÉIRE', 'EIRE']
        for add in address3:
            ##Check for Irish name
            included = list(filter(lambda x: re.findall(" " + x.upper() + " ", add), IrelandW))
            ##if ireland is in address
            if len(included) > 0:
                posSpa = -1
                ##Check occurences
                for incl in included:
                    ##get position and make sure its not in Northern Ireland
                    if add.find(incl) > -1 and not add.lower().find("northern ireland") > -1:
                        if add.find(incl) > posSpa:
                            ##Save highest value
                            posSpa = add.find(incl)##Werk Pim
                if posSpa > -1:
                    add = add[0:posSpa+len(incl)]
                address4.append(add)
            else:
                ##Make sure the address is in Ireland and not northern Ireland
                if not add.lower().find("northern ireland") > -1:
                    address4.append(add)
    
        ##4. WHat about text with no street names or zipcodes (check for city names alone)?
        if len(address4) == 0:
            cityNames = []
            inIreland = False
            ##Check included city names
            for incl2 in includedCities:
                ##Check for Ireland occurences BUT NOT Northern Ireland
                if incl2 in IrelandW:
                    inIreland = True
                else:
                    ##Only add city names in local list (not the country)
                    if not incl2 in cityNames:
                        cityNames.append(incl2)

            ##Check if somethng is found
            if len(cityNames) > 0:            
                ##Get most occuring city name
                dictCityNames = dict.fromkeys(cityNames , 1)
                for soup in soups:
                    #get text
                    text = df.visibletext(soup, False)
                    text = preprocessText(text)
        
                    ##Count occurences of words
                    for city in cityNames:
                        ##Count occurences in text
                        cnt = text.count(city)
                        ##Deal with uppercase
                        if cnt == 0:
                            cnt = text.count(city.upper())
                        if cnt > 0:
                            ###Add valu to docitonary
                            dictCityNames[city] += cnt
                    
                ##sort dictionary decending        
                sort_dictCityNames = sorted(dictCityNames.items(), key=lambda x: x[1], reverse=True)
                ##get maxValue
                maxVal = max(k[1] for k in sort_dictCityNames)
                ##Get city max name
                address4 = [k[0] for k in sort_dictCityNames if k[1] == maxVal]
                del dictCityNames
                del sort_dictCityNames
        
            ##if Ireland is mentiond add the countris name (always) AND WE NOW KNOW ITS NOT NORTHERN IRELAND
            if inIreland:
                address4.append("Ireland")
        
        del address
        del address2
        del address2b
        del address3
        del soups    
        del includedCities
        ##return reduced LIST of potential address texts    
        return(address4)
    except:
        ##An error occurred
        return([])

##Check for email addresses
def checkEmail(soups):
    emails = []
    
    ##Check all pages for emails
    for soup in soups:
        ##get text
        text = df.visibletext(soup, False)
    
        ##check text
        mails = re.findall(genMail2, text)
    
        ##Check if something is found in text
        if len(mails) == 0:
            ##Check for emails in soup
            mails = re.findall(genMail2, str(soup))
        
        ##check findings and check ending with .jpg or .png
        for m in mails:
            ##remove identifiers of pictures
            if not m.lower().endswith(".jpg") and not m.lower().endswith(".jpeg") and not m.lower().endswith(".png") and not m.lower().endswith(".webp"):  ###deal with .ico?
                ##Check if already included
                if not m in emails:
                    emails.append(m)
        
    ##Check if ico occures combined with other emails
    emails1b = []
    ##remove any ico email
    for m in emails:
        if not m.lower().endswith(".ico"):
            emails1b.append(m)
    ##use cleaned emails if other than ico mails are found
    if len(emails1b) > 0:
        ##use copy of emails1b
        emails = emails1b.copy()
            
    del soups
    del emails1b
    
    ##Flatten to string
    emails2 = ", ".join(em.replace(",", " ") for em in emails)
    emails2 = emails2.replace(";", " ")        
    return(emails2)
    
##Check for phone number sequences, vurl is needed for whatsapp internal links
def checkPhone(soups, vurl):
    phone = []
    
    ##Check all soups
    for soup in soups:
        ##Get text
        text = df.visibletext(soup, False)   
        ##remove ( and )
        text1 = text.replace("(", "")
        text1 = text1.replace(")", "")
        ##deal with hard spaces, make sure numbers are NOT stuck together´
        text1 = text1.replace('\xa0', ' ; ')
        
        ##0. Preprocess numbers in text
        text1 = re.sub(r'(\d)[\.\s]+(\d)', r'\1\2', text1)
        ##Preprocess numbers (remove space dots and /)
        text1 = re.sub(r'(\d)[\.\s]+(\d)', r'\1\2', text1)
        ##remove annoying chars (/ and -) in numbers
        text1 = re.sub('[\-\/]+', '', text1)    
        ##replace : and , and . with spaces (so begin and end are clear)
        text1 = re.sub('[:\.,]', ' ', text1)
        ##Add spaces for and after
        text1 = " " + text1 + " "
    
        ##1. Check land line numbers in Irleand +353 and 7 digits (may contain a (0))
        irLandline1 = '\+353?\d{7,9}[\s]{1}|\+353?0\d{7,9}[\s]{1}|00353?\d{7,9}[\s]{1}|00353?0\d{7,9}[\s]{1}'
        phoneL = re.findall(irLandline1, text1)
        ##If nothing i found check soup
        for ph in phoneL:
            ph = ph.strip()
            if not ph in phone:
                phone.append(ph)
    
        ##2. Check mobile phone numbers in Spain (start with +34 or with 0)
        irMobile1 = '\+353[8]{1}\d{8}[\s]{1}|00353[8]{1}\d{8}[\s]{1}|[0]{1}[8]{1}\d{8}[\s]{1}'
        phoneM = re.findall(irMobile1, text1)    
        ##If nothing i found check soup
        ##Add anything found to phone list
        for ph in phoneM:
            ph = ph.strip()
            if not ph in phone:
                phone.append(ph)

        ##3. Check for any numeric sequence starting with a + between spaces 
        phoneGen1 = '[\s]{1}\+?[0-9]{6,14}[\s]{1}'  ##CAN BE FROM OTHER COUNTRIES
        phoneG = re.findall(phoneGen1, text1)
        ##If nothing i found check soup
        ##Add anything found to phone list
        for ph in phoneG:
            ph = ph.strip()
            if not ph in phone:
                phone.append(ph)
    
        ##compare findings, check if number starting with a +, 00 or 08 is found (indicative for Ireland)
        phone1b = []
        for ph in phone:
            ##Include numbers starting with + and zero
            if ph.startswith("+"):
                phone1b.append(ph)
            elif ph.startswith("00") or ph.startswith("08"): ##Could be a 1 for toll free numbers
                phone1b.append(ph)
        ##Check if any number starting with a + is found
        if len(phone1b) > 0:
            ##Use only those nubners
            phone = phone1b.copy()

        ##4. Subsequently check for whatapps links (will inlcude phonenumber) 
        ##get external links
        inter, exter = df.extractLinks(soup, vurl, False)    
        ##Try whatsapp number in extern
        for link in exter:
            if link.lower().find('whatsapp') > -1:
                if not link in phone:
                    phone.append(link)
                    
    ##Remove duplicates
    phone = list(set(phone))
    ##Flatten to string
    phone2 = ", ".join(ph.replace(",", " ") for ph in phone)
    del phone
    del phone1b
    del soups
    
    ##return result
    phone2 = phone2.replace(";", " ")
    return(phone2)

##Check for Irish VAT numbers
def checkVAT(soups):
    VAT = []
    
    for soup in soups:
        ##get text
        text = df.visibletext(soup, False)
        text1 = preprocessText(text)
        ##Preprocess numbers (remove spaces and dots between digits)
        text1 = re.sub(r'(\d)[\.\s]+(\d)', r'\1\2', text1)
        ##Preprocess numbers (remove space dots and /)
        text1 = re.sub(r'(\d)[\.\s]+(\d)', r'\1\2', text1)
        ##remove anoying chars (/ and -)
        text1 = re.sub('[\-\/]+', '', text1)    
        ##Make sure characters are lower
        text1 = text1.upper()
    
        ##search for 7 digits and 1 char, 7 digits with 2 chars and 1 digit, 1 char followed by 6 digits and 1 char
        ##with left and right a space
        irVAT = '\s{1}[0-9]{7}[A-Z]{1,2}\s{1}|\s{1}[0-9]{1}[A-Z]{1}[0-9]{5}[A-Z]{1}\s{1}'
        VATs = re.findall(irVAT, text1)
        ##Check if text contains VATs
        if len(VATs) == 0:
            ##check soup
            VATs = re.findall(irVAT, str(soup)) ##risque as some Google codes may be found!
            text1 = str(soup)
        
        ##Check if findings are preceded by VAT or CBL
        if len(VATs) > 0:
            for v in VATs:
                v = v.strip()
                start = 0
                while not start == -1:
                    posV = text1.find(v, start)                    
                    if posV > -1:
                        ##find VAT or CBL to the left
                        text0 = text1[0:posV]
                        ##Get position of VAT CBL
                        posVAT = text0.rfind("VAT")
                        if posVAT == -1:
                            posVAT = text0.rfind("CBL")
                        ##Check distance
                        if posVAT > -1 and (posV - posVAT) < 20:
                            ##ony keep VAT numbers indicated as VAT or CBL
                            VAT.append(v)
                        else:
                            ##Could be behind number, Check to the right
                            text0 = text1[posV+len(v):]
                            ##Get position of VAT CBL
                            posVAT = text0.find("VAT")
                            if posVAT == -1:
                                posVAT = text0.find("CBL")
                            ##Check distance
                            if posVAT > -1 and (posVAT - posV) < 20:
                                ##ony keep VAT numbers indicated as VAT
                                VAT.append(v)                        
                        ##Set new startposition
                        start = posV + len(v)
                    else:
                        start = -1
    
    ##remove duplicates from VAT
    VAT = list(set(VAT))
    ##Flatten to string
    VAT2 = ", ".join(vt.replace(",", " ") for vt in VAT)
    del VAT
    del soups
    VAT2 = VAT2.replace(";", " ")
    return(VAT2)

##Get activities from plain text
def getActivitiesText(text):
    activ = []
    boolDrone = False
    
    if len(text) > 0:
        ##Add space to text and make lower
        text = " " + text.lower() +  " " 
        
        ##0. Always check for drones synonyms in text
        if not boolDrone:
            dronew  = ['drone', ' rpas ', ' uas ', ' uav ', ' unmanned ', ' aerial ', ' uaai ']
            for w in dronew:
                if text.find(w) > -1:
                    boolDrone = True ##just to check iis a drone company (is this needed?)
                    break
            
        ##Check for manufacturing
        if not "manufacturing" in activ:
            manuf = [' manufactu', ' fabricat']
            for w in manuf:
                if text.find(w) > -1:
                    activ.append("manufacturing")
                    break
                
        ##distribution
        if not "distributing" in activ:
            distr = [' distribu', ' dealer ']
            for w in distr:
                if text.find(w) > -1:
                    activ.append("distributing")
                    break
            
        ##maintenanmce repair
        if not "maintaining" in activ:
            maint = [' maintenan', " repair "]
            for w in maint:
                if text.find(w) > -1:
                    activ.append("maintaining")
                    break
            
        ##sales of drones
        if not "sales" in activ:
            sal = [" retail ", " shop "]
            for w in sal:
                if text.find(w) > -1:
                    activ.append("sales")
                    break
                    
        ##rental
        if not "renting" in activ:
            rent = [" rent ", " renting "]
            for w in rent:
                if text.find(w) > -1:
                    activ.append("renting")
                    break
                    
        ##Check for training
        if not "training" in activ:
            train = [" training ", " course "]
            for w in train:
                if text.find(w) > -1:
                    activ.append("training")
                    break
                
        ##Check for filming and more
        if not "filming/imaging" in activ: ##photo
            film = ["filming ", " video", " audiovisual", " photo", " imaging "] ##?media include or not?
            for w in film:
                if text.find(w) > -1:
                    activ.append("filming/imaging")
                    break
                    
        ##Checkf for inspection
        if not "inspection" in activ:
            insp = [" survey", " mapping ", " inspection"] ##solutions, solutiones?
            for w in insp:
                if text.find(w) > -1:
                    activ.append("inspection")
                    break
            
        ##Check for components for drones
        if not "components" in activ:
            payl = [" component", " payload ", " parachut", " camera", " accessor"]
            for w in payl:
                if text.find(w) > -1:
                    activ.append("components")
                    break
            
        ##Check for consultancy    
        if not "consultancy" in activ:
            mang = [" consultan", " consulting "] 
            for w in mang:
                if text.find(w) > -1:
                    activ.append("consultancy")
                    break
                
        #Check for shows entertainment
        if not "entertainment/race" in activ:
            ent = [" race ", " show ", " entertainment ", " theat", " dancing "]
            for w in ent:
                if text.find(w) > -1:
                    activ.append("entertainment/race")
                    break
    
        ##desigin and intergation
        if not "design"in activ:
            des = [" design ", " integration"]
            for w in des:
                if text.find(w) > -1:
                    activ.append("design")
                    break

        ##include ?Delivery of medicines with drones == Reparto de medicamentos con drones
        ##include ?Ponemos en contacto a pilotos y operadoras con empresas y clientes” == this firm is a network which “connects drone’s operators with companies and clients
        ##include ?Seguros == Insurance (is also security in Spanish)
        ##include soluciones guido = guide solutions (?)
        ##include ?navegación y control = navigation and control (of RPAS)
        ##insurance?
        
    return(activ, boolDrone)        
    
##Get activities from text by checking the surroundings of dorne words at specific characterlength distances (left and right)
def getActivitiesRange(text, area = 200):
    activ = []
    ##activMax = ""
    boolDrone = False
    
    if len(text) > 0:
        ##Add space to text and make lower
        text = " " + text.lower() +  " " 
        
        ##0. Always check for drones synonyms in text
        dronew  = ['drone', ' rpas ', ' uas ', ' uav ', ' unmanned ', ' aerial ', ' uaai ']
        included = list(filter(lambda x: re.findall(x, text), dronew))  
            
        ##if dorne words are found search afrea surrounding them
        if len(included) > 0:
            boolDrone = True
            for incl in included:
                endReached = False
                start = 0
                while not endReached:               
                    ##Get locations of incl and soearc surrondings
                    posIncl = text.find(incl, start)
                    
                    if not posIncl == -1:
                        ##get left
                        posLeft = posIncl - area
                        if posLeft < 0:
                            posLeft = 0
                        posRight = posIncl + area
                        if posRight > len(text):
                            posRight = len(text)
                            endReached = True
                        ##get text surroinding posInlc
                        textS = text[posLeft:posRight]
                        ##Check activities
                        activ2, boolDrone2 = getActivitiesText(textS)
                        if len(activ2) > 0:
                            for act in activ2:
                                if not act in activ:
                                    activ.append(act)
                        ##Check next part of text
                        start = posIncl + len(incl)
                    else:
                        endReached = True
          
    return(activ, boolDrone)        
   
##get activities surrounding drone words by checking a range of words (left and right)
def getActivitiesWords(text, words = 10):
    activ = []
    ##activMax = ""
    boolDrone = False
    
    if len(text) > 0:
        ##preprocess text
        text = preprocessText(text)
        ##split text
        textL = text.lower().split(" ")
        ##remove single characters and emtpy items
        textL = [x for x in textL if len(x) > 1]
        
        ##0. Always check for drones synonyms in text
        dronew  = ['drone', ' rpas ', ' uas ', ' uav ', ' unmanned ', ' aerial ', ' uaai ']
        included = list(filter(lambda x: [x for w in textL if w.find(x) > -1], dronew))
            
        ##if dorne words are found search afrea surrounding them
        if len(included) > 0:
            boolDrone = True
            for incl in included:
                ##Get word or words to look for (as incl may be partially matched)
                Ws = [x for x in textL if x.find(incl) > -1]
                ##for each word
                for w in Ws:
                    ##Get locations of incl and soearc surrondings
                    posIncl = textL.index(w)
                    
                    if not posIncl == -1:
                        ##get left
                        posLeft = posIncl - words
                        if posLeft < 0:
                            posLeft = 0
                        posRight = posIncl + words
                        if posRight > len(textL):
                            posRight = len(textL)
                        ##get text surroinding posInlc as flattened string
                        textS = " ".join(textL[posLeft:posRight])
                        ##Check activities
                        activ2, boolDrone2 = getActivitiesText(textS)
                        ##result of boolDrone2 is not used here
                        if len(activ2) > 0:
                            for act in activ2:
                                if not act in activ:
                                    activ.append(act)         
    return(activ, boolDrone)        
    
##Get activities of company
def getActivities(soups, vurl, descrip, name):
    activ = ""
    activ2 = []
    boolDrone = False
    
    if len(descrip) > 0 or len(name) > 0:        
        ##Check decrip and name first, combne tem
        activ2, boolDrone2 = getActivitiesText(descrip.lower() + " " + name.lower())
        
        ##Check boolean
        if boolDrone2:
            boolDrone = True

    ##See if more needs to be checked
    if len(activ2) == 0:
        ##words to detect activities
        ##words to detect activities (engish and irish)
        words = ['services', 'seirbhísí', 'seirbhisi', 'activities', 'gníomhaíochtaí', 'gniomhaiochtai', ['what', 'do'], ['cad', 'dhéanaimid'], ['cad', 'dheanaimid']]
    
        ##Add soups from pages words in link (On main page (=first so,up))
        inter, exter = df.extractLinks(soups[0], vurl)
        ##Add links with services word in it
        links = []
        links2 = []    
        for link in inter:
            for w in words:
                if not type(w) == list:
                    if link.find(w) > -1:
                        ##print(link)
                        if not link in links:
                            links.append(link)
                else:
                    ##Do both words occur
                    ##get occurneces
                    res = sum(1 for w1 in w if link.find(w1) > -1)
                    if res >= len(w):
                        ##print(link)
                        if not link in links:
                            links.append(link)
        
        ##sort links and only keep main pages of subdirs, ignore those below that link)
        if len(links) > 0:
            ##sort links
            links.sort(key = len)         
            links2not = []
            for link in links:
                ##Check if its a subset of the rest, only add first or unique
                inn = [s for s in links if link in s]
                inn.sort(key = len)
                if len(inn) > 1:
                    if not inn[0] in links2 and not inn[0] in links2not:
                        links2.append(inn[0])
                    ##add the rest to ignore list
                    for n in inn[1:]:
                        links2not.append(n)
                        
                elif len(inn) == 1:
                    if not inn[0] in links2not:
                        links2.append(inn[0])
            del links2not     
        
        ##Scrape extra links found                    
        if len(links2) > 0:
            if len(links2) > 4:
                links2 = list(sorted(links2, key = len))
                links2 = links2[0:5]
            ##Scrape links selected
            for link in links2:
                ##get soup
                soup2, vurl2 = df.createsoup(link)
                ##Add to soups
                if len(str(soup2)) > 1:
                    if not soup2 in soups:
                        soups.append(soup2)
                del soup2
            
        ##Check soups for types of services
        for soup in soups:
            ##get text
            text = df.visibletext(soup, True)
            text = preprocessText(text)
    
            ##Check if text is extracted
            if len(text) > 0:
                activ2a, boolDrone2 = getActivitiesWords(text, 20)
                
                if boolDrone2:
                    boolDrone = True
           
                if len(activ2a) > 0:
                    for act in activ2a:
                        if not act in activ2:
                            activ2.append(act)
                        
    ##Add value of boolDron
    ##activ.append("Drone=" + str(boolDron))
    
    ##Flatten to string
    activ = ", ".join(ac.replace(",", " ") for ac in activ2)
    del activ2
    del soups
    activ = activ.replace(";", " ")
    return(activ, boolDrone)
    
##funtion that check the occurrence of ecommerce words     
def checkEcommerce(soups):
    eCom = False
    
    ##list of Spanish ecommerce words
    ##words = ['ecommerce', 'e-commerce', 'comercio electrónico', 'comercio electronico', 'shopping cart', 'carrito', "tienda"]
    ###  [ "winkel", "shop", "cart", "wagen", "bag", "mand", "basket", "warenkorb", "klant" ]
    words = ['shop', 'cart', 'siopadóireacht', 'siopadoireacht', 'basket', 'customer', 'custaiméir', 'custaimeir', 'client', 'cliant'] ##carro ?
    
    ##Check soups
    for soup in soups:
        ##get text
        text = df.visibletext(soup)
        text = preprocessText(text)
        text = " " +  text +  " "
        
        ##Check words in text
        for w in words:
            w = " " + w + " "
            if w in text:
                eCom = True
                break
                
        ##Check results
        if not eCom:
            ##Check soup for ecom words ?IS THIS NEEDED?
            for w in words:
                if w in str(soup):
                    eCom = True
                    break
    
        ##Check if detected
        if eCom:
            break
    
    del soups
    ##return True or False    
    return(str(eCom))

##Check occurence of social media links, incl. youtube
def checkSocMedia(soups, vurl):
    socMedia = []
    
    ##Check soups
    for soup in soups:
        ##get external links
        inter, exter = df.extractLinks(soup, vurl, False)
    
        ##Try location via Social media
        for link in exter:
            ##check and remove ? part
            if link.lower().find("?") > -1:
                posQ = link.lower().find("?")
                link = link[0:posQ]
            
            ##Check if ends with slash (needs to be removed; see below, may be multiple //)
            while link.endswith("/"):
                link = link[0:-1]
            
            ##Check if a slash is present after domain name
            ##remove http:// or https:// part
            link1 = link.replace("http://", "")
            link1 = link1.replace("https://", "")
            ##Check if an addtional part is included
            if link1.count("/") > 0:
                ##Check via social media links
                if link.lower().find('twitter.com') > -1:
                    if not link.lower().find("tweet") > -1 and not link.lower().find("twitter.com/share") > -1 and not link.lower().find("/hashtag/") > -1 and not link.lower().find("/share") > -1 and not link.lower().find("/status") > -1 and not link.lower().find("/search") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find('facebook.com') > -1:
                    if not link.lower().find("posts") > -1 and not link.lower().find("share") > -1 and not link.lower().find("/group") > -1 and not link.lower().find("photos") > -1 and not link.lower().find("timeline") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find('linkedin.com') > -1:
                    if not link.lower().find("/share") > -1 and not link.lower().find("/jobs/") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find('instagram.com') > -1:
                    if not link.lower().find("/tv/") > -1 and not link.lower().find("/p/") > -1 and not link.lower().find("/v/") > -1 and not link.lower().find("/explore/tags/") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find("pinterest.com") > -1:
                    if not link.find("/pin/create/") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find("youtube.com") > -1: ##Added additionaly ?vimeao?
                    if not link.lower().find("watch") > -1 and not link.lower().find("/playlist") > -1 and not link.lower().find("/results") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                elif link.lower().find("vimeo.com") > -1:
                    if not link in socMedia:
                        socMedia.append(link)
                elif link.lower().find("plus.google.com") > -1:
                    if not link.lower().find("/share") > -1 and not link.lower().find("/posts") > -1:
                        if not link in socMedia:
                            socMedia.append(link)
                
    ##Flatten to string
    socMedia2 = ", ".join(sm.replace(",", " ") for sm in socMedia)
    del socMedia
    del soups
    socMedia2 = socMedia2.replace(";", " ")          
    ##return all unique findings
    return(socMedia2)
    
##get description of company based on text on webpage and top10 words scores
def getDescription(soups):    
    top10 = []
    sent_select = ""
    foundBool = False
    
    ##Check soups
    for soup in soups:
        ##Check for precense of meta tag
        if str(soup).find("meta content") > -1:            
            ##Check for description meta tags in head of soup
            head = soup.head
            for item in head:
                if str(item).find("description") > -1 and str(item).find("meta") > -1 and str(item).find("content") > -1:
                    ##get content of metatag
                    sent_select = item['content']
                    sent_select = sent_select.strip()
                    ##Check if it contains text (no url)
                    if not sent_select.startswith("http:"):
                        #remove any returns
                        sent_select = sent_select.replace("\n", " ")                       
                    else:
                        sent_select = ""
            ##descrip = soup.find("meta", property="og:description")
            ##if not descrip == None:
            ##    sent_select = descrip['content']
            if len(sent_select) > 0:
                foundBool = True
        
            del head
        
        if not foundBool:
            ##1. get top 10 words? alternative ways?
            text1, lang = convertSoup(soup)
            text1 = preprocessText(text1) ##remove stopwords of language detected
    
            ##Check if text remains
            if len(text1) > 0:
                ##get words as list
                wordL = text1.split()
                ##Count occurences
                wordC = collections.Counter(wordL)
                ##sort words in collection
                sorted_wordC = sorted(wordC.items(), key=lambda kv: kv[1], reverse=True)
        
                ##get top 10 (or less)
                if len(sorted_wordC) >= 10:
                    ##Get top 10
                    sorted_wordC = sorted_wordC[0:10]
        
                ##get top words as list
                for w in sorted_wordC:
                    w0 = w[0]
                    top10.append(w0)
    
                ##Check occurence of top10 words
                if len(top10) > 0:
                    ##2. Get text in lowercase with dots
                    ##text = df.visibletext(soup, True)
                    text = visibleText(soup).lower()
                    ##make sure they are lowercase
                    sentences = text.lower().split(".")
                    ##create equally long score list
                    scores = [0] * len(sentences)
                    for i in range(len(sentences)):
                        score = 0
                        ##get sentence without strange signs
                        sent = preprocessText(sentences[i])
                        ##score sentence for top10 words
                        score = sum(1 for w in top10 if sent.count(w) > 0)
                        if score > 0:
                            scores[i] = score    
    
                    ##3. get max scoring sentence
                    maxScore = max(scores)
                    ##get sentences with max score
                    maxSent = [sentences[i].strip() for i in range(len(scores)) if scores[i] == maxScore]
                    ##sort on descending length (shortest first)
                    sorted_maxSent = list(sorted(maxSent, key = len))
                    ##Select first (shortes sentence with highest score)
                    sent_select = sorted_maxSent[0]
                
                    del text
                    del sentences
                    del scores
                    del maxSent
                    del sorted_maxSent
                    
                del wordL
                del wordC
                del sorted_wordC
                
            del text1
        
        ##Check if sentence is selected
        if len(sent_select) > 0:
            break
    
    del top10
    del soups
    
    sent_select = sent_select.replace(";", " ")
    sent_select = sent_select.replace("\t", " ")
    sent_select = sent_select.replace("\r", " ")  
    sent_select = sent_select.replace("\n", " ")  
    return(sent_select)
    
##Are jobsvacancies advertised on website
def checkJobs(soups):
    jobs = False
    text = ""
    words = ['jobs', 'vacancy', 'vancancies']
    
    for soup in soups:
        ##get text
        text = df.visibletext(soup, True)
        text = preprocessText(text)
        text = " " + text + " "
        
        ##Check for job ads occurence in text
        for w in words:
            if w in text:
                jobs = True
                break
            
        ##Check if the occure in soup as string
        if not jobs:
            ##IS THIS NEEDEDE?
            for w in words:
                if w in str(soup):
                    jobs = True
                    break
    
        ##Check if loop can be quited
        if jobs:
           break
     
    del text
    del soups
    ##return string  
    return(str(jobs))
    
##Get region of locations info in text (First locatin identified is selected)
def getRegionText(text, level = 'nuts2'):
    region= ""
    if level == 'province':
        level = 'county'
    if not level == "nuts2" and not level == "nuts3" and not level == "county":
        level = "nuts2"
        ##only nuts2, nuts3 and province allowed
             
    ##get locatons from text provided, exclude northern ireland
    if len(text) > 0 and not text.lower().find("northern ireland") > -1:
        ##process text, replace . and , and :
        text = preprocessText(text)
        
        ##Check for Irish zipcode in text
        included = re.findall(" " + zipIre + " ", text)
        included = [x[0].strip() for x in included]
        included = list(set(included))
    
        ##1. Check if zipcode is included
        if len(included) > 0:
            for incl in included:
                ##CHeck length (may be length 2, should be 3)
                if len(incl) == 2:
                    ##Add zero in between r
                    incl = incl[0] + '0' + incl[1]
                ##get rows from table
                rowM = municLC.loc[municLC['Zipcode'] == incl]
    
                ##Check if something is found
                if len(rowM) > 0:
                    ##Get level requested
                    if level == "nuts2":
                        ##get NUTS2 as string
                        region = list(rowM['NUTS2'])[0]
                    elif level == "county":
                        ##Get county as string
                        region = list(rowM['County'])[0]
                    else:
                        ##One option remains
                        region = list(rowM['NUTS3'])[0]
                
                    ##if region is found
                    if not region == "":
                        ##quit loop
                        break            
                    
                del rowM
        ##2. check for names
        else:
            ##2a. get municipality names in text
            municL1 = list(municLC['Municipality'])
            ##Check if city names are included in text
            included = list(filter(lambda x: re.findall(" " + x + " ", text), municL1)) 
            if len(included) == 0:
                included = list(filter(lambda x: re.findall(" " + x.upper() + " ", text.upper()), municL1)) 
            included = list(set(included))
            ##Check if cities are found
            if len(included) > 0:
                ##lookup region of municipality
                for incl in included:
                    incl = incl.lower().capitalize()
                    ##get row
                    rowM = municLC.loc[municLC['Municipality'] == incl]
                    ##Check if something is found
                    if len(rowM) > 0:
                        if level == "nuts2":
                            ##get NUTS2 as string
                            region = list(rowM['NUTS2'])[0]
                        elif level == "county":
                            ##Get county as string
                            region = list(rowM['County'])[0]
                        else:
                            ##Other options
                            region = list(rowM['NUTS3'])[0]
                
                        ##if region is found
                        if not region == "":
                            ##quit after first assignment
                            break
                        
                    del rowM
            ##2b Check for region names (nuts2 or nuts3)
            else:
                ##Nothing is found, check for name of region at level requested or lower
                if level == "nuts2":
                    ##Get list of unique nuts2 names
                    nuts2 = list(set(list(municLC['NUTS2'])))        
                    nuts2 = list(set(nuts2))
                    ##deal with capital letters
                    textL = [x.lower().capitalize() for x in text.split(" ")]
                    text2 = " ".join(x for x in textL)
                    ##get nuts2 from text
                    included = list(filter(lambda x: re.findall(x, text2), nuts2)) 
                    ##Check findings
                    if len(included) > 0:
                        for incl in included:
                            region = incl
               
                            if not region == "":
                                ##quit loop
                                break
                elif level == "county":
                    ##Get list of unique nuts2 names
                    counties = list(set(list(municLC['County'])))

                    ##get nuts2 from text
                    included = list(filter(lambda x: re.findall(x, text), counties)) 
                    ##Check findings
                    if len(included) > 0:
                        for incl in included:
                            region = incl
               
                            if not region == "":
                                ##quit loop
                                break
                else:
                    ##Must be nuts3 names
                    nuts3 = list(set(list(municLC['NUTS3'])))
                    ##make sure all names occur once
                    nuts3 = list(set(nuts3))                    
                    ##deal with capital letters
                    textL = [x.lower().capitalize() for x in text.split(" ")]
                    text2 = " ".join(x for x in textL)                    
                    ##get nuts3 from text
                    included = list(filter(lambda x: re.findall(x, text2), nuts3)) 

                    ##Check findings
                    for incl in included:
                        ##Select rows
                        rowM = municLC.loc[municLC['NUTS3'] == incl]
                        ##Check if something is found
                        region = ""
                        if len(rowM) > 0:
                            ##Could be two options, one can be empty
                            region = list(rowM['NUTS3'])[0]
                        
                        del rowM
                        if not region == "":
                            ##quit loop
                            break
                    del textL
                    del text2
            del municL1
    ##return string
    return(region)

##get region from soups
def getRegionAddress(addresses, level = 'nuts2'):
    regions = []
    
    ##process soups
    for text in addresses:
       ##get region from text
       reg = getRegionText(text, level)
       ##print(reg)
       if not reg == "" and not reg in regions:
           regions.append(reg)
    
    ##Flatten findings
    region = ", ".join(reg for reg in regions)
    return(region)

##get region from soups
def getRegionSoups(soups, level = 'nuts2'):
    regions = []
    
    ##process soups
    for soup in soups:
       text = df.visibletext(soup, True)
       ##get region from text
       reg = getRegionText(text, level)
       ##print(reg)
       if not reg == "" and not reg in regions:
           regions.append(reg)
        
    ##Flatten findings
    region = ", ".join(reg for reg in regions)
    del soups

    return(region)

##detect Spain in text
def checkInCountry(soups, address, phone, vurl):
    ##Set inCountry to None
    inCountry = None
       
    ##start with phone
    if len(phone) > 0:
        ##Check for occurence of + or 00 Countrycode of Ireland
        phones = phone.split(",")
        for ph in phones:
            ph = ph.strip()           
            ##Check for Irish phone countrycode
            if ph.startswith("+353") or ph.startswith("00353"):
                ##Country found
                inCountry = True
                ##Stop search
                break
            elif ph.startswith("+") or ph.startswith("00"):
                ##Other countrycode found
                inCountry = False
                
    ##Check if more needs to be checked
    if inCountry == None:
        ##get countrynames from ini file in lower case
        countryN = [" " + x.lower() + " " for x in countryNames]

        ##start with address, ignore single added España
        if len(address) > 0:
            for add in address:
                ##process address
                add = preprocessText(add.lower())
                add = " " +  add.strip() + " "
                ##print(add)
                ##Check for occurence of ireland in text, but beware of add with single ireland word!!!(see address4 part of checkAddress2 function)
                included = list(filter(lambda x: re.findall(x, add), countryN))         
                if len(included) > 0 and not add == " ireland " and not add.find("northern ireland") > -1:
                    inCountry = True
                    break
            
            ##When Ireland is not there but the address ends with a correct Irish cityname (this is a valid address), its in the country
            if not inCountry:
                ##Check the end of address for inclusion of city name
                for add in address:
                    ##process address
                    add = preprocessText(add)
                    ##remove any leading and lagging non-letter cheracters
                    add = re.sub('(.*?)([a-z].*[a-z])(.*)', '\\2', add, flags=re.IGNORECASE)                    
                    ##Check how many words are in address (should be at least 3 parts; otherwise its a single name)
                    addL = add.split(" ")
                    addL = [x for x in addL if len(x) > 1]
                    if len(addL) >= 3:
                        ##check last word for cityname
                        included = included = [x for x in municL if add.endswith(x)]
                        if len(included) > 0 and not add.find("northern ireland") > -1:
                            inCountry = True
                            break
                    del addL
        else:
            ##process soups (RISQUE OF FINDING SPAIN WHEN NOT IN SPAIN!) INCLUDE DETECTING OTHER COUNTRIES?
            for soup in soups:
                ##Get text
                text = df.visibletext(soup, True)
                ##Add space between signs
                text = preprocessText(text)
                text = " " + text + " "
    
                ##Check for occurence of ireland in text
                included = list(filter(lambda x: re.findall(x, text), countryN)) 
                if len(included) > 0 and not text.find("northern ireland") > -1:
                    inCountry = True
                    break
    
    ##Check inCOuntry with vurl
    if inCountry == None:
        ##get dom of vurl
        dom = df.getDomain(vurl)
        ##check end of dom
        if dom.endswith(country.lower()):
            inCountry = True
        else:
            inCountry = False
    
    ##Return as string
    return(str(inCountry))
 
##Determine if website is a company (TEST FUNCTION)
def checkIfCompany(soups, address, phone, VAT, descrip, vurl):
    boolComp = False
    
    ##start with VAT, if found must be a company
    if len(VAT) > 0:
        ##Has a VAT number (it a Spanish one!)
        boolComp = True
    
    ##Check if already solved
    if not boolComp:
        ##FIrst check for blog indicators in vurl and descrip
        if vurl.find("blog.") > -1 or vurl.find(".blog") > -1 or descrip.lower().find(" blog ") > -1:
            ##its a blog
            boolComp = False
        ##What is typical for a company?, MUST have an address, email and phone AND CONTACT PAGE OR ABOUT US PAGE (ergo multiple soups)
        elif (len(address) > 0 and len(phone) > 0 and len(soups) > 1) or (len(address) > 0 and len(phone) > 0 and len(descrip) > 0):
            ##Seems to be a company, as it has an address, email, phone and multiple soups (indicating at least a main page and a contact or about-us page)
            ##Could be a company, check description
            compW = ["company", "business", "cuideachta", " gnó ", " gno "]
            ##Check occurence
            included = list(filter(lambda x: re.findall(x, descrip), compW))
            if len(included) > 0:
                boolComp = True
            else:
                ##Could still be a company, check the content of the main page
                ##get soup of main page
                soup = soups[0]
                ##Get text
                text = df.visibletext(soup, True)                       
                ##Check occurence of company words
                included = list(filter(lambda x: re.findall(x, text), compW))
                ##Check occurence
                if len(included) > 0:
                    ##count occurences of company word in text
                    for incl in included:
                        ##if it occurs multiple times probably a news of blog
                        if text.count(incl) >= 5 and (text.find("news") > -1 or text.find("blog") > -1):
                            ##to much, looks like a news site or blog
                            break
                        else:
                            boolComp = True
                ##Defintaly NOT a company sitm cOuld be news site, blog etc about companies!

        else:
            ##NOT A COMPANY (no address, phone number and descrip found)
            boolComp = False   
    
    return(boolComp)

##Functions local getDomain
def getDomain4(url):
    dom = ""
    
    if len(url) > 0:
        ##get domain from url without prefix
        dom = df.getDomain(url, False)
    
        ##remove leading www
        if dom.startswith("www."):
            dom = dom.replace("www.", "")
        
    return(dom)
 
##Function te get date of website creation
def getDates(text):
    dateStr = ""

    ##Check text
    if len(text) > 0:
        ##Convert to lower
        text = text.lower()
        
        ##Find dates, is first date ok?
        if text.find("creation date") > -1:
        
            ##Get first date after text
            posText = text.find("creation date")
            text2 = text[posText:]
            ##get dates
            dates = re.findall(regDate2, text2)
            
            ##Check dates
            if len(dates) > 0:
                ##get first date
                dateStr = str(dates[0])
            else:
                dateStr = "0"
            
        elif text.find("registered on") > -1:
            ##Get first date after text
            posText = text.find("registered on")
            text2 = text[posText:]
            ##get dates
            dates = re.findall(regDate2, text2)
        
            if len(dates) > 0:
                ##get first date
                dateStr = str(dates[0])
            else:
                dateStr = "0"
        
        else:
            ##Get all dates and store
            dates = re.findall(regDate2, text2)
            if len(dates) > 0:  
                ##get all dates in string
                dateStr = ", ".join(str(x) for x in dates)
            else:
                dateStr = "0"

    return(dateStr)

def getStartDate(url):
    dateStr = ""
    
    ##Get domain
    if len(url) > 0:
        ##get domain, with www. removed (if included)
        dom = getDomain4(url)
    
        try:
            ##Construct url2 for who.is
            url2 = "https://who.is/whois/" + dom
            ##get soup
            soup, vurl = df.createsoup(url2)
            ##get text
            text = df.visibletext(soup, True)
            
            ##somethng is extracted
            if len(text) > 0:
                ##check if any dates are included
                dates = re.findall(regDate2, text)
 
                ##Check occurence of dates
                if len(dates) > 0:
                    ##Get creation date or similar dates
                    dateStr = getDates(text)
                else:
                    ##No dates found, indicated that (profile might be protected) or there is a part before domain name that should be removed
                    ##Remove part before domain and country code 
                    url3 = df.getDomain3(url)
                    ##Check if its different
                    if not url3 == url:
                        ##Construct url2 for who.is
                        url2 = "https://who.is/whois/" + url3
                        soup, vurl = df.createsoup(url)
                        text = df.visibletext(soup, True)
                        ##Check text
                        if len(text) > 0:
                            ##Get creation date or similar
                            dateStr = getDates(text)                            
                        else:
                            dateStr = "0"
        except:
            ##An error occured
            dateStr = "0"
            
    return(dateStr)

##new processLinks version, run in parallel
def ProcessLinks2(urls_found):
    ##Init vars
    result = []
    urls_Error = []
    count = 0
    
    ##get urls as list
    urlsC = list(urls_found['url_visited'])
    
    ##Chek and visite websites of url_viited
    for url in urlsC:    
        ##init vars
        vurl = ''
        text = ""
        soup = 0 ##Is this needed
        action = ""
        
        ##remove last / if included
        if url.endswith("/"):
            url = url[0:-1] 
 
        ##Clear vars
        name = ""
        addresses = ""
        email = ""
        phone = ""
        VAT = ""
        descrip = ""
        activities = ""
        eComm = ""
        socMedia = ""
        region = ""
        jobs = ""  
        startDate = ""
        
        ##Set bools and more
        boolDrone = False
        strInCountry = 'False'
        boolIsCompany = False
        isSelected = 0
    
        ##Check if url needs to be chekced
        if not url == '' and not url == 'nan':
            try:                
                ##1. Check url
                ##First attempt
                soup, vurl = df.createsoup(url)        
        
                ##1b. Check if something is found##get relevant urls
                if soup == 0:
                    ##Get redirected url
                    url2 = df.getRedirect(url, True) 
                    if not url2 == url:
                        ##Check for url2 (second attempt)
                        soup, vurl = df.createsoup(url2)                                                 
                    else:
                        ##No vurl
                        soup = 0
                        action = "web site does not exist"
            
                ##1c. Check if something is found
                if len(str(soup)) > 1:            
                    ##something is found, get text and the soups of interesting links
                    
                    ##0a. get text (keep case as is)
                    text = df.visibletext(soup, False) ##Is this step needed?            
                    ##0b. Get relevant urls and soup them
                    pageSoups = getSoups(soup, vurl)
                                        
                    ##print(str(count) + " url: " + vurl)
                    
                    ##Check if soups are extracted
                    if len(pageSoups) > 0:
                        
                        ##A. Find required info
                        ###########################################################
                        ##1 Name of entreprise
                        name = checkName(pageSoups)
                        ##print("name: " + name)
                    
                        ##2 Contact address (PIM's work)  ##NEED TO ADD COUNTRY IN ADDRESSES FOUND!
                        address = checkAddress2(pageSoups, vurl) ##Include PIMś work
                        ##print("address: " + address)  ##Flatten text
                    
                        ##3 Email address
                        email = checkEmail(pageSoups) ##Check all soups included
                        ##print("email: " + email)
                    
                        ##4 Phone numnber
                        phone = checkPhone(pageSoups, vurl) ##Checks all soups included
                        ##print("phone: " + str(phone))
                    
                        ##5 VAT number
                        VAT = checkVAT(pageSoups) ##Checks all soups included
                        ##print("VAT: " + str(VAT))
                    
                        ##6 Shortest sentence on webpage that contains the highest number of top 10 words
                        descrip = getDescription(pageSoups) ##Selects sentences with highest score of top10 words
                        ##print("description: " +  descrip)
                    
                        ##7 Get creation date/registratin date as proxy for age of website
                        startDate = getStartDate(vurl) ##url of page visited
                         
                        ##8 E-commerce activity (binary)
                        eComm = checkEcommerce(pageSoups) ##Check all sopus provided
                    
                        ##9 Social Media presesnce
                        socMedia = checkSocMedia(pageSoups, vurl) ##Checks all pages
                    
                        ##10 Job advertisements (binary)
                        jobs = checkJobs(pageSoups)
                         
                        ##Next part are the more complicated functions
                        
                        ##11 NACE, and/or check activities of company (instructions Blanca)
                        activities, boolDrone = getActivities(pageSoups, vurl, descrip, name)
                        ##If boolDrone = False, NOT A drone company!!
                        
                        ##12 get inCountry (checks for Spain Espana in address +34 in phone other wise in pageSoups)
                        strInCountry = checkInCountry(pageSoups, address, phone, vurl)
 
                        ##13 Regional level (from location in address)
                        ##If in country is True, Make sure fundtion deals with potential input correctly
                        if strInCountry == 'True' and not address == []:
                            region = getRegionAddress(address, "nuts3")
                        elif strInCountry == 'True':
                            region = getRegionSoups(pageSoups, "nuts3")
                        else:
                            region = ""
                            
                        ##14 check if its a company
                        boolIsCompany = checkIfCompany(pageSoups, address, phone, VAT, descrip, vurl)
                        
                        
                        ##B. Store findings, convert address list to string
                        if len(text) > 0:
                            action = "text extracted and checked"
                            ##flatten potential multiple output (only addresses)
                            addresses = ", ".join(add.replace(",", " ") for add in address)
                            ##Should url be selected?
                            if boolIsCompany and strInCountry == 'True' and boolDrone:
                                isSelected = 1
                                action += " and selected"                        
                        else:
                            ##Soup created with no text
                            action = "no text could be extracted"
                    else:
                        if action == "":
                            action = "No soups obtained from website"
                            
                    del pageSoups
                else:
                    ##No soup created
                    if action == "": ##May be redirected to social media
                        action = "web site does not respond"

            except:
                print("Error occured in: " + str(url) + " " + str(vurl))
                action = "an error has occured"
                ##Check url and vurl
                if not url in urls_Error and not url == "" and not vurl in urls_Error and not vurl == "":
                    ##Recheck url again
                    urlsC.append(url)
                    if not vurl == url:
                        ##Recheck vurl again
                        urlsC.append(vurl)
                    ##Add to error list
                    urls_Error.append(url)
                    urls_Error.append(vurl)                

        else:
            ##url is empty of nan
            action = "url does not have to be checked"

        ##Always add findings to result
        result.append([url, vurl, name, addresses, email, phone, VAT, descrip, activities, boolDrone, eComm, socMedia, region, jobs, startDate, strInCountry, boolIsCompany, str(isSelected), action])                                                
        
        ##Store result in queu so it can be added to logfile
        q.put([url, vurl, action])
            
        ##show progress
        count += 1        
        if count % 100 == 0:
            print("processed " + str(count) + " links")

    ##return result for list of urls
    print("processed " + str(count) + " links")
    return(result)

##Queue listener to store data in log file for paralle version
def listener(q):
    '''listens for messages on the q, writes to file. '''
    with open(logFile, 'a') as f:
        while 1:
            m = q.get() 
            if m == "kill":
                break
            else:                
                f.write(str(m[0]) + ";" + str(m[1]) + ";" + str(m[2]) + '\n')
                f.flush()

### START #####################################################################
Continue = False
fileName = ''
fileName1 = ''
startPos = 0

##A. Check input variables
if(len(sys.argv) > 1):
    ##additional input provided
    if(len(sys.argv) >= 2):
        fileName1a = str(sys.argv[1])
        fileName1 = localDir + "/" + fileName1a
        if(not(os.path.isfile(fileName1))):
            print("File " + fileName1 + " does NOT exists")
        else:
            ##Continue and load Ini file
            Continue = True
else:
    print("Use 'python3 Script7_IniIE.py <filename.csv> to run program") 

##B. load ini-file if Continue
if Continue and not fileName1 == '':
    ## LOAD SETTING 
    fileName = "IE_en3.ini"
    ##enable ini file and load
    config = configparser.ConfigParser()
    config.read(fileName)

    ##get vars and values
    try:
        country = config.get('MAIN', 'country')
        lang = config.get('MAIN', 'lang')
        language = config.get('SETTINGS4', 'language')
        countryW1 = config.get('SETTINGS4', 'countryW1').split(',')
        countryW2 = config.get('SETTINGS4', 'countryW2').split(',')
        drone_words = config.get('SETTINGS4', 'drone_words').split(',')
        cont_words = config.get('SETTINGS4', 'cont_words').split(',')
        runParallel = config.getboolean('SETTINGS4', 'runParallel')
        saveHtml = config.getboolean('SETTINGS4', 'saveHtml')
        socialSearch = config.getboolean('SETTINGS4', 'socialSearch')        
        cityNameFile = config.get('SETTINGS4', 'cityNameFile')
        countryNames = config.get('SETTINGS4', 'countryNames').split(',')
        
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(str(countryW1)) > 0 and len(str(countryW2)) > 0 and len(str(drone_words)) > 0 and len(str(runParallel)) > 0 and len(cityNameFile) > 0:
            print("All variables from ini-file checked")
            
        ##Add international (english) contact words, if not already included
        if not "impress" in cont_words:
            cont_words.append("impress") ##impressum generic word
        
    except:
        ##An erro has occured
        print("An error has occured while reading ini-file")
        print("Check ini-files content and retry")
        Continue = False
        ##Check vars needed
else:
    print("Please provide a valid filename")
    Continue = False

##C. Prepare city names, domain name and world country names files
if Continue:
    ##import Functions
    import Drone_functions as df 
        
    try: 
        ##Check for city names zipcode file
        if os.path.isfile("IE_city_prov_names.csv"):
            ##Get city and nuts region list
            municLC = pandas.read_csv("IE_city_prov_names.csv", sep = ";")
            ##Get municipalities
            municL = list(municLC['Municipality'])
            ##Append country name to municL
            municL.append("Ireland")
            municL.append("Éire")
            municL.append("Eire")

        else:
            print("City, nuts and zipcode file is not found; name = IE_city_prov_names.csv")
            Continue = False
    except:
        ##An error occured
        print("An error occured during preparation of city name, domain or world file names loading")
        Continue = False


##D. process first part of script 4
if Continue:
    
    ##get cores of machine used
    cores = mp.cpu_count()
    
    if not runParallel:
        cores = 1        
    
    ##Create logfile
    logFile = "7_results_" + country.upper() + "_1.txt"
    if os.path.isfile(logFile):
        ##Append info to existing file
        f = open(logFile, 'a')
    else:
        ##Create new file
        f = open(logFile, 'w')
    
    ##1. Obtain data                   
    ##1a. Get urls, from script 4a diferent files 4_Result_ES_2FEC.csv
    urls_found = pandas.read_csv(fileName1, sep = ";")

    ##Create list of urls to be checked, ONLY those predicted as drone
    urls_foundC = urls_found[urls_found['pred_prob'] > 0.6]
    ##remove index
    urls_foundC = urls_foundC.reset_index(drop=True)
    ##prepare to add domain name and url length
    urls_foundC['dom'] = ""
    urls_foundC['url_len'] = -1
    ##process urls
    for i in range(urls_foundC.shape[0]):
        ##get url
        url = urls_foundC.loc[i, 'url_visited']
        ##add domain
        urls_foundC.loc[i, 'dom'] = df.getDomain(url, True)
        ##Add url length
        urls_foundC.loc[i, 'url_len'] = len(url)
    
    ##sort on length of url visited and domain name
    urls_foundC = urls_foundC.sort_values(["dom", "url_len"], ascending=True)
    ##remove index
    urls_foundC = urls_foundC.reset_index(drop=True)
    
    ##Check for duplicates and keep shortest
    urls_foundC['selected'] = -1
    ##check
    for i in range(urls_foundC.shape[0]):
        ##Only proceed if value is minus 1
        if urls_foundC.loc[i, 'selected'] == -1:
            ##get current domain
            dom1 = urls_foundC.loc[i, "dom"]
            ##get all urls with same domain
            urls = urls_foundC[urls_foundC['dom'] == dom1]
        
            ##Check what has been selected
            if urls.shape[0] == 1:
                urls_foundC.loc[i, 'selected'] = 1
            else:
                ##break
                ##set all urls found with dom1 to zero!!
                urls_foundC.loc[urls_foundC['dom'] == dom1, 'selected'] = 0
                ##must be more than 1, het one with shortest urls or first
                minUrl = min(urls['url_len'])
                ##Check how many have min value
                if len(urls[urls['url_len'] == minUrl]) > 1:
                    ##break
                    ##get subset of urls
                    urls2 = urls[urls['url_len'] == minUrl]
                    ##get first uniek url
                    url = urls2.iloc[0, 0]
                    urls_foundC.loc[urls_found['url'] == url, 'selected'] = 1
                elif len(urls[urls['url_len'] == minUrl]) == 1:
                    ##get unique url
                    url = list(urls[urls['url_len'] == minUrl]['url'])[0]
                    urls_foundC.loc[urls_foundC['url'] == url, 'selected'] = 1
             
    ##Only keep selected
    urls_foundC2 = urls_foundC[urls_foundC['selected'] == 1]
    ##remove index
    urls_foundC2 = urls_foundC2.reset_index(drop=True)

    ##Show progress
    print(str(urls_foundC.shape[0]) + " total number of drone links loaded")
    f.write(str(urls_foundC.shape[0]) + " total number of drone links loaded\n")
    print(str(urls_foundC2.shape[0]) + " total number of drone links will be checked")
    f.write(str(urls_foundC2.shape[0]) + " total number of drone links will be checked\n")

    ##Process links
    ##define 
    resultsF = []
    
    ##Multicore or not
    if runParallel:
        ##Muliticore test version
        print("Parallel search option is used")
        f.write("Parallel search option is used\n")

        ##randomize dataframe
        urls_foundC2 = urls_foundC2.sample(frac=1).reset_index(drop=True)
    
        ##split links list in chunks
        chunks = np.array_split(urls_foundC2, cores*multiTimes, axis = 0)
    
        #must use Manager queue here, or will not work
        manager = mp.Manager()
        q = manager.Queue()
       
        ##Use all cores to process file
        pool = mp.Pool(cores*multiTimes)
        
        #put listener to work first
        watcher = pool.apply_async(listener, (q,))
        
        resultP = pool.map(ProcessLinks2, [c for c in chunks])
        time.sleep(5)    
        
        ##Kill the listener
        q.put('kill')            
        pool.close()
        pool.join()
    
        ##Add all links to resultsF of lists in list
        for res in resultP:
            for l in res:
                if not l in resultsF:
                    resultsF.append(l)
     
    else:
        print("Serial search option is used")
        f.write("Serial search option is used") 
        ##exclude social media checks
        resultsF = ProcessLinks2(urls_foundC2) 
    
    ##Show progress
    print("Link list completely processed")
    print("Number of links remaining " + str(len(resultsF)))
    f.write("Number of links remaining " + str(len(resultsF)) + "\n")

    ##Store results
    ##Convert results to DataFrame
    resultDF = pandas.DataFrame(data = resultsF, columns = ["org_url", "url_visited", "name", "address", "email", "phone", "VAT", "description", "activities", "boolDrone", "eCommerce", "socialMedia", "region", "jobs", "startDate", "inCountry", "isCompany", "isSelected", "action"])            

    ##Store findings
    fileNameR = "4_Result_" + country.upper() + "_3_extracted_info.csv"
    ##Save DataFrame as file
    resultDF.to_csv(fileNameR, sep = ";", index=False)    
    print("All urls processed, file saved")
    f.write("All urls processed, file saved\n")
    print("Script 7 finished")
    
##END of script 7
