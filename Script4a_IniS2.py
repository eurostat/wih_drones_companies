#!/usr/bin/env python3
# -*- coding: utf-8 -*-
## Oct 27 2021, version 1.2, added final cleaning step for nan and empty spaces
## changed pandas read_csv to df.loadCSVfile fundtionc whihc is more generic and solves pands read iseu for some files
## Check urls found, input is a list of urls, first part of script4 only goes as afar as to check social media for additional urls
##with updated location search (inlcude text preprocessing and spaceses added to names)
##reduced duplicate http splitting, pandas empty frames and option to in- or exclude social media search
##Include results of Name based URLsearch and a very thorough social media link cleaning (focussing in the end on drone acronym including links)
##Multicore version to quicker ProProcess huge numbers of links (rough 2x)

#Load libraries 
import os
import sys
import re
##import time
import pandas
import numpy
##import nltk
import multiprocessing as mp
import configparser

##Get current directory
localDir = os.getcwd()

##get regex for url matching in documents
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##Factor to multiply the number of cores with for parallel scraping (not for search engine use)
multiTimes = 1 ##number to multiply number of parallel scraping sessions

##Exclude vurl for often occuring not relevant urls 
urls_exclude = ["doi.org", "google.com", "youtube.com", "youtu.be", "worldcat.org", "b.tracxn.com", "gmail.com", "cookiedatabase.org", "twitter.com/share?", "twitter.com/hashtag", "twitter.com/intent/", "facebook.com/sharer.php?", "instagram.com/p/", "addtoany.com/add_to/", "whatsapp://send?", "pinterest.com/pin/", "t.me/share/url?", "change-this-email-address.com"]

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

## DEFINE FUNCTIONS
##replace characters with character with leading laggign space
def preprocessText(text):
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

def detectLocationCountry(text):
    ##preprocess text
    text1 = preprocessText(text)
    ##2. Check location mentioned
    inCountry = None
    ##Check if country name is mentioned on web page
    if any(" " + x + " " in text1.lower() for x in countryW2):
        ##Country detected
        inCountry = True
    elif any(" " + x + " " in text1 for x in wCountries1):
        ##Other country name detected
        inCountry = False    
    else:
        ##detect if a country muncipality names occur in text (difficult for Dutch city names)
        included = list(filter(lambda x: re.findall(" " + x + " ", text1), municL)) 
        ##Check findings 
        if len(included) > 0:
            inCountry = True
    ##return findings
    return(inCountry)

##Process Twitter
def Twitterchecks(vurl, brows = 1):
    ##purpose is find the url to the webpage of this firm
    vurl2 = ''
    inCountry = None
    ##Check content, ignore irrelavnt twitter links
    if not vurl.endswith("twitter.com") and not vurl.endswith("twitter.com/home") and not vurl.find('twitter.com/share?') > 0:
        ##0. Remove status part after username if present
        if vurl.find('/status/') > 0:
            vurl = vurl[0:vurl.find('/status/')]
        
        ##1. Use chromedriver to get webpage
        soup, vurl3 = df.chromesoup(vurl)
        ##get text (in lowercase)
        text = df.visibletext(soup, False)
                
        ##Check if no text is extracted
        if text == '':
            if brows == 1:
                soup, vurl3 = df.browsersoup(vurl)
            else:
                soup, vurl3 = df.browsersoup2(vurl)                
            text = df.visibletext(soup, False)
                
        if len(text) > 0:
            ##Cut out relevant part (ignore tweet content)
            if text.lower().find('lid geworden in'):
                text = text[0:text.lower().find('lid geworden in')]
            ##2. Check location    
            inCountry = detectLocationCountry(text)
            
            ##3. Check for any urls in text
            pot_url = re.findall("((https?://)?(www\.)?[a-z0-9_-]+\.[a-z]{2,})", text.lower())
            ##remove twitter.com, and keep first match in each sublist (they match the most)
            pot_url = [x[0] for x in pot_url if not x[0] == "twitter.com"]        
       
            ##Decide what to do
            if len(pot_url) > 0:
                ##Add url found to end of list of urls to be checked
                for url in pot_url:
                    if url.startswith('http'):
                        pass
                    elif url.startswith('www'):
                        url = "http://"+ url
                    else:
                        url = "http://www." + url
                            
                    ##Get actual url
                    url = df.getRedirect(url)
                    
                    ##Check redirect result
                    if not url == '':
                        if url.endswith("/"):
                            url = url[0:-1]
                                
                        ##Append url to url searched
                        vurl2 = url
                        break                       
                ##Preprocess and check link
                vurl2 = PreProcessList([vurl2])
                if vurl2 == []:
                    vurl2 = ''
                else:
                    vurl2 = vurl2[0]
             
    return(vurl2, inCountry)
    
##Process Facebook
def Facebookchecks(vurl, brows = 1):
    vurl2 = ''
    inCountry = None
    
    ##Check content, ignore irrelavnt facebook links
    if not vurl.endswith("facebook.com") and not vurl.find('facebook.com/sharer.php?') > 0:
               
        ##1. Use localborwser
        if brows == 1:
            soup, vurl3 = df.browsersoup(vurl)
        else:
            soup, vurl3 = df.browsersoup2(vurl)                
        ##get text
        text = df.visibletext(soup, False)

        ##Check is something is found
        if len(text) > 0:
            ##2. check for ireland
            inCountry = detectLocationCountry(text)
                
            ##3. Check for any urls in text
            pot_url = re.findall("((https?://)?(www\.)?[a-z0-9_-]+\.[a-z]{2,})", text.lower())
            ##remove twitter.com, and keep first match in each sublist
            pot_url = [x[0] for x in pot_url if not x[0] == "facebook.com"]        
            ##Clear urls
            pot_url = df.checkCountries(pot_url, country)
            ##remove shortened .ly and .gy links
            pot_url = [x for x in pot_url if x.endswith('.ly') == 0 and x.endswith('.gy') == 0]
            ##Sort accoridng to length (decreasing)
            sorted_list = list(sorted(pot_url, key = len, reverse = True))
                    
            ##Decide what to do
            if not inCountry == False and len(sorted_list) > 0:
                ##Add longest url found to end of list of urls to be checked
                for url in sorted_list:
                    if url.startswith("http"):
                        pass
                    elif url.startswith('www'):
                        url = "http://"+ url
                    else:
                        url = "http://www." + url
                    ##Get actual url
                    url = df.getRedirect(url)
                    ##Append url to url searched
                    if not url == '':
                        vurl2 = url
                        break                
                ##Preprocess and check link
                vurl2 = PreProcessList([vurl2])
                if vurl2 == []:
                    vurl2 = ''
                else:
                    vurl2 = vurl2[0]
            
    return(vurl2, inCountry)

##Process Linkedin
def Linkedinchecks(vurl, brows = 1):
    vurl2 = ''
    inCountry = None
    
    ##Check link, must be of a company or person (groups? can these be read?)
    if vurl.find("linkedin.com/company/") > 0 or vurl.find("linkedin.com/in/") > 0:
        ##1. Use localborwser
        if brows == 1:
            soup, vurl3 = df.browsersoup(vurl)
        else:
            soup, vurl3 = df.browsersoup2(vurl)                
        ##get text
        text = df.visibletext(soup, False)

        ##Check result
        if len(text) > 0:
            ##Select the most interesting part of the text
            if text.lower().find('see all details') > 0:
                text = text[0:text.lower().find('see all details')]                
            ##2. check for ireland
            inCountry = detectLocationCountry(text) ##All are positive ??
            ##3. check for website in soup code            
            if not inCountry == False and str(soup).find("companyPageUrl") > 0:
                ##find all positions of company Url location
                pos = [m.start() for m in re.finditer('companyPageUrl', str(soup))]
                ##Check each occurence
                for p in pos:
                    ##get text + 100 chars
                    text1 = str(soup)[p:(p+100)]
                    ##extract any urls
                    link1 = re.findall(genUrl, text1.lower()) ##Need to convert to lower?
                    link = ''
                    if not link1 == []:
                        for l in link1:
                            for l1 in l:
                                ##get largest link but no linkedin link
                                if l1.startswith('http') and not l1.find('linkedin.com') > 0:
                                    link = l1
                                    break
                    ##Check if url is found
                    if not link == '':
                        print(link)
                        vurl2 = link
                ##Preprocess and check link
                vurl2 = PreProcessList([vurl2])
                if vurl2 == []:
                    vurl2 = ''
                else:
                    vurl2 = vurl2[0]
  
    return(vurl2, inCountry)

##Process Instagram
##Links mau contain a link to the webpage of a company
def Instagramchecks(vurl, brows = 1):
    vurl2 = ''
    inCountry = None
    
    ##Check if NOT refering to a single picture
    if not vurl.find("instagram.com/p/") > 0:
        ##Check if link to a username and picture
        if vurl.count("/") > 3:
            url = ""
            res = vurl.rsplit("/") 
            for i in range(len(res)):
                url += res[i] + "/"
                if i >= 3:
                    break
            vurl = url
    
        ##1. Use localborwser
        if brows == 1:
            soup, vurl3 = df.browsersoup(vurl)
        else:
            soup, vurl3 = df.browsersoup2(vurl)                
        ##get text
        text = df.visibletext(soup, False)
        
        if len(text) > 0:
            ##Select the most interesting part of the text
            if text.lower().find('berichten getagd') > 0:
                text = text[0:text.lower().find('berichten getagd')]                
            
            ##2. check for country
            inCountry = detectLocationCountry(text) ##All are positive ??
            
            ##3. check for website in soup code
            link1 = re.findall(genUrl, text.lower())
            link = ''
            if not link1 == []:
                for l in link1:
                    for l1 in l:
                        if not l1.find('instagram.com') > 0:
                            if len(l1) > len(link):
                                link = l1.strip()
            if not link == "":
                if not link.startswith("http"):
                    link = "http://" + link
                print(link)
                vurl2 = link
            
            ##Preprocess and check link
            vurl2 = PreProcessList([vurl2])
            if vurl2 == []:
                vurl2 = ''
            else:
                vurl2 = vurl2[0]

    return(vurl2, inCountry)
    
##Process Pinterest
##Links mau contain a link to the webpage of a company
def Pinterestchecks(vurl, brows = 1):
    vurl2 = ''
    inCountry = None
    
    ##Check if NOT refering to a single picture
    if not vurl.find("pinterest.com/pin/") > 0:
        ##Check if link to a username and picture
    
        ##1. Use localborwser
        if brows == 1:
            soup, vurl3 = df.browsersoup(vurl)
        else:
            soup, vurl3 = df.browsersoup2(vurl)                
        ##get text
        text = df.visibletext(soup, False)
        
        if len(text) > 0:
            ##Select the most interesting part of the text
            if text.lower().find('de beste borden van') > 0:
                text = text[0:text.lower().find('de beste borden van')]                
            
            ##2. check for country
            inCountry = detectLocationCountry(text) ##All are positive ??
            
            ##3. check for website in soup code
            link1 = re.findall(genUrl, text.lower())
            link = ''
            if not link1 == []:
                for l in link1:
                    for l1 in l:
                        if not l1.find('pinterest.com') > 0:
                            if len(l1) > len(link):
                                link = l1.strip()
            if not link == "":
                if not link.startswith("http"):
                    link = "http://" + link
                print(link)
                vurl2 = link

            ##Preprocess and check link
            vurl2 = PreProcessList([vurl2])
            if vurl2 == []:
                vurl2 = ''
            else:
                vurl2 = vurl2[0]

    return(vurl2, inCountry)

def reduceLinks(urls_list):
    urls_list2 = []    
    ##process urls in list
    for url in urls_list:
        ##Check end
        if url.endswith("/"):
            ##remove final slash
            url = url[0:-1]
        
        ##Count number of slashes
        num = url.count("/")
        
        ##Check count of slashes
        if num >= 4:
            url1 = ""
            ##Check Social media options
            if url.lower().find("twitter.com/") > 0 or url.lower().find("facebook.com/") > 0 or url.lower().find("instagram.com/") > 0 or url.lower().find("pinterest.com/") > 0:
                ##cut out part before the 4th slash
                count = 0                
                for i in range(len(url)-1):
                    if url[i] == "/":
                        count +=1
                    if count == 4:
                        url1 = url[0:i]
                        break                    
                ##get url1    
                url = url1
            elif url.lower().find("linkedin.com/") > 0:
                ##cut out part before the 5th slash
                count = 0                
                for i in range(len(url)-1):
                    if url[i] == "/":
                        count +=1
                    if count == 5:
                        url1 = url[0:i]
                        break                    
                ##get url1    
                url = url1       
            else:
                url = df.getDomain(url)
        
        ##Add to urls_list2
        if not url == '':
            if not url in urls_list2:
                urls_list2.append(url)
    
    return(urls_list2)

def PreProcessList(urls_list):
    urls_cleaned = []
    
    ##1. Check for multiple links in 1 url
    links =[]
    for url in urls_list:
        ##1a. Check for doubel http at start
        if url.startswith('http://http'):
            url = url[7:len(url)]
        if url.startswith('https://http'):
            url = url[8:len(url)]
        
        ##1b. Check urls (a.o. for multiple links in 1 url)
        us = df.checkUrls(url)
        if not us == []:
            for u1 in us:
                if not u1 == '' and not u1 in links:
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
    for url in urls_cleaned:
        ##Exclude any urls included in urls_exclude
        if not any(url.lower().find(x) > 0 for x in urls_exclude):
            urls_cleaned2.append(url)
        
    ##3c. ALso check for / or no slah variant and remove domains not referening to country being studied
    urls_cleaned2a, urlsNot = df.cleanLinks(urls_cleaned2, country, True)
    
    ##3d remove links with 4 or more slashes, but keep domain name, take care of social media (4/5 slashes allowed)
    urls_cleaned2b = reduceLinks(urls_cleaned2a)

    return(urls_cleaned2b)

##Check for links on Social media after preprocessing
def ProcessSoc(urls_found, brows = 1):
    urls_extra = []
    
    for url in urls_found:    
        vurl2 =''
        inCountry = None
            
        ##Check Social media options
        if url.lower().find("twitter.com") > 0:
            ##check twitter options
            print("Twitter link")  
            vurl2, inCountry = Twitterchecks(url, brows)
                    
        elif url.lower().find("facebook.com") > 0:
            ##Check facebook options
            print("Facebook link")
            vurl2, inCountry = Facebookchecks(url, brows)

        elif url.lower().find("linkedin.com") > 0:
            ##Check facebook options
            print("LinkedIn link")
            vurl2, inCountry = Linkedinchecks(url, brows)
       
        elif url.lower().find("instagram.com") > 0:
            ##Check instagram options
            print("Instagram link")
            vurl2, inCountry = Instagramchecks(url, brows)
    
        elif url.lower().find("pinterest.com") > 0:
            ##Check pinterest pagina
            print("Pinterest link")               
            vurl2, inCountry =  Pinterestchecks(url, brows)

        ##Add vurl2 if inCountry
        if not inCountry == False and not vurl2 == '':
            ##Check for socila media link
            if vurl2.lower().find("twitter.com") > 0 or vurl2.lower().find("facebook.com") > 0 or vurl2.lower().find("linkedin.com") > 0 or vurl2.lower().find("instagram.com") > 0 or vurl2.lower().find("pinterest.com") > 0:
                if not vurl2 in urls_found:
                    urls_found.append(vurl2)
            else: 
                ##Must be a link to a non-social media website
                if not vurl2 in urls_found2b and not vurl2 in urls_extra:
                    urls_extra.append(vurl2)

    return(urls_extra)         

##Remove links on Social media that are similar to domain names of urls already included
def PreProcessSoc2(urls_found, urls_doms2, brows = 1):
    ##Remove any duplicates and sort
    urls_found = list(set(urls_found))
    urls_found.sort()
    
    ##1. remove any non-country links (have a non-country string before domain name)
    for i in range(len(urls_found)):
        ##get url
        url = urls_found[i].strip()
        ##Check part preceding domain name (can be domain name when this part is not present)
        sec = url.split(".")[0]
        if sec.startswith("http"):
            sec = sec.replace("http://", "")
            sec = sec.replace("https://", "")

        ##Check if url needs to br removed
        if sec == "instagram" or sec == "twitter" or sec == "facebook" or sec == "linkedin" or sec == "pinterest":
            ##Do nothing
            pass
        elif not sec == country and not sec == "www" and not sec == "":
            ##Clear link
            urls_found[i] = ""
    
    ##remove empty strings        
    urls_found = [x for x in urls_found if not x == ""]
    urls_found.sort()
    
    ###2. remove any links with _ between to number series for facebook
    for i in range(len(urls_found)):
        url = urls_found[i].strip()        
        if url.find("facebook.com") > -1 and url.find("_") > -1:
            ##Check for potential message type
            urlL = url.split("/")
            ext = urlL[len(urlL)-1]
            if re.match("[0-9]+_[0-9]+", ext):
                ##extract part before _ and see if already included
                loc = url.index("_")
                url2 = url[0:loc]
                if not url2 == "" and not url2 in urls_found:
                    urls_found.append(url2)
                else:
                    urls_found[i] = ""
    
    ##remove empty strings        
    urls_found = [x for x in urls_found if not x == ""]
    urls_found.sort()
       
    ##3. Remove any names ending with different country codes
    for i in range(len(urls_found)):
        url = urls_found[i].strip()
        ##Check if url ends with . and 3 letters
        if re.search("\.[a-z]{2}$", url):
            ##Check if extension is ie
            if not url.endswith(country):
                urls_found[i] = ""

    ##remove empty strings        
    urls_found = [x for x in urls_found if not x == ""]
    urls_found.sort()
             
    ##4. Check extensions (part after social media domain)  
    ## AND
    ##5 Check for duoplicate usernames on different platforms
    for i in range(len(urls_found)):        
        ##get url
        url = urls_found[i].strip()
        ##Check url
        if not url == "":
            ##get extension (part after social media domain + com; if no extension is there domain names is selected)
            urlL = url.split("/")
            ext = urlL[len(urlL)-1]
            ##remove number and - or _ at end
            ext = re.sub("[\-\_]{1}[0-9]+$", "", ext)
            
            ##Check if ext is included in already collected urls (urls_found2) domains
            if any(x for x in urls_doms2 if x.find(ext) > -1):
                urls_found[i] = ""
            
            else:
                ##Check for duplicate extensions on multiple platforms (or multiple http and https links)
                ##get urls with the same ext
                links = [x for x in urls_found if x.endswith(ext)]
                links.sort(key = len)
                ##Check how many were found, only remove if more than 1 has been found
                if len(links) > 1:       
                    ## 1. Choose twitter if available
                    twt = [x for x in links if x.find("twitter.com") > -1]
                    twt.sort(key = len)
                    if len(twt) >= 1:
                        ##Remove longest twitter link from links                        
                        links.remove(twt[len(twt)-1])
                    else: ##2. No twitter links
                        ##Just remove the longest link from links
                        links.remove(links[len(links)-1])
                
                    ##Subsequently remove the remaining links
                    for l in links:
                        if not l == "":
                            ##urls_found = ["" if x == l else x for x in urls_found]
                            for j in range(i,len(urls_found)):
                                if urls_found[j] == l:
                                    ##Clear
                                    urls_found[j] = ""
                                    ##stop for loop for current j
                                    break
                       
    ##remove empty strings        
    urls_found = [x for x in urls_found if not x == ""]
    
    ##6. Keep only the ones with a drone acronym in it
    for i in range(len(urls_found)):
        url = urls_found[i].strip()
        
        if re.search("\/[0-9]+$", url):
            ##url ends with number (keep)
            pass
        elif not any(x for x in drone_words if url.find(x) > -1):
            urls_found[i] = ""
    
    ##remove empty strings        
    urls_found = [x for x in urls_found if not x == ""]
    
    ##remove any duplciates and sort
    urls_found = list(set(urls_found))
    urls_found.sort()

    return(urls_found)         
 
##Multicore version of ProcessSoc
def ProcessSocmp(urls_found, brows, outputQueue):
    links = []
    try:
        ##get results
        links = ProcessSoc(urls_found, brows)
    except:
        ##Ann error occured
        pass
    finally:        
        outputQueue.put(links)

def getAllUrlsSearched(urlSearchDF):
    urls = []
    
    ##get column with urls as list
    urlsList = list(urlSearchDF.iloc[:,1])
    
    ##extract all urls (sperated by a komma) in each element of list 
    for u in urlsList:
        ##print(u)
        ##Split urls at komma
        urls2 = u.strip().split(",")
        for u2 in urls2:
            ##remove any leading and lagging spaces 
            u2 = u2.strip()
            ##Check if link strats with http and is not included yet
            if u2.startswith("http") and not u2 in urls:
                urls.append(u2)
    
    del urlsList
    del urls2
    
    return(urls)

### START #####################################################################
Continue = False
fileName = ''

##A. Check input variables
if(len(sys.argv) > 1):
    ##additional input provided
    if(len(sys.argv) >= 2):
        fileName = str(sys.argv[1])
        if(not(os.path.isfile(fileName))):
            print("File " + fileName + " does NOT exists")
        else:
            ##Continue and load Ini file
            Continue = True         
else:
    print("Use 'python3 Script4a_Ini.py <filename.ini>' to run program") 


##B. load ini-file if Continue
if Continue and not fileName == '':
    ## LOAD SETTING 
    ##enable ini file and load
    config = configparser.ConfigParser()
    config.read(fileName)

    ##get vars and values
    try:
        country = config.get('MAIN', 'country')
        lang = config.get('MAIN', 'lang')
        countryW1 = config.get('SETTINGS4', 'countryW1').split(',')
        countryW2 = config.get('SETTINGS4', 'countryW2').split(',')
        drone_words = config.get('SETTINGS4', 'drone_words').split(',')
        mem_words = config.get('SETTINGS4', 'mem_words').split(',')
        runParallel = config.getboolean('SETTINGS4', 'runParallel')
        socialSearch = config.getboolean('SETTINGS4', 'socialSearch')
        cityNameFile = config.get('SETTINGS4', 'cityNameFile')
        countryNames = config.get('SETTINGS4', 'countryNames').split(',')
        
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(str(countryW1)) > 0 and len(str(countryW2)) > 0 and len(str(drone_words)) > 0 and len(str(mem_words)) > 0 and len(str(runParallel)) > 0 and len(cityNameFile) > 0:
            print("All variables from ini-file checked")
        
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
        #get municipalities of country
        municl = df.loadCSVfile(cityNameFile)
        municl2 = list(municl.iloc[:,0])
        ##Remove any leading and lagging spaces
        municL = [x.strip() for x in municl2]
        if municL[0] == '0':
            municL = municL[1:len(municL)]
    
        ##Get domain list
        euDom1 = df.euDom.copy()
        euDom1.remove(country) ##Remove country extension from list of domain extensions

        ##get countryNames list
        wCountries1 = df.worldCountries.copy()
        for name in countryNames:
            if name in wCountries1:
                wCountries1.remove(name)

        ##Check for existence of essential files
        fileName1E = localDir + "/1_external_" + country.upper() + lang.lower() + "1.csv"
        fileName2E = localDir + "/2_external_" + country.upper() + lang.lower() + "1.csv"
        fileName3Ea = localDir + "/3_externalPDF_" + country.upper() + lang.lower() + "1.csv" ##Check name
        if not os.path.isfile(fileName1E):
            print(fileName1E + " file was not found, make sure its available. Script halted")
            Continue = False
        if not os.path.isfile(fileName2E):
            print(fileName2E + " file was not found, make sure its available. Script halted")
            Continue = False
        if not os.path.isfile(fileName3Ea):
            print(fileName3Ea + " file was not found, make sure its available. Script halted")
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
    logFile = "4a_results_" + country.upper() + lang.lower() + "1.txt"
    f = open(logFile, 'w')

    ##1. Obtain data                   
    ##1a. Get urls, from 4 diferent files
    
    ##Get results of script 1
    urls_found1 = df.loadCSVfile(fileName1E)    
    ##get results of script 2
    urls_found2 = df.loadCSVfile(fileName2E) 
    
    ##Get intermediat result of script 2 (may not be created when empty)
    fileName2Eb = localDir + "/2_external_" + country.upper() + lang.lower() + "_drone_low_1.csv"
    if os.path.isfile(fileName2Eb):
        urls_found2b = df.loadCSVfile(fileName2Eb) ##
    else:
        print(fileName2Eb + " was not found, is this OK?")
        urls_found2b = pandas.DataFrame([])
    
    ##Get intermediat result of script 2 (may not be created when empty)    
    fileName2Ec = localDir + "/2_external_" + country.upper() + lang.lower() + "_drone_high_1.csv"
    if os.path.isfile(fileName2Ec):
        urls_found2c = df.loadCSVfile(fileName2Ec) ##May also contain relevant URLs
    else:
        print(fileName2Ec + " was not found, is this OK?")
        urls_found2c = pandas.DataFrame([])
    
    ##Get result from PDF extraction
    urls_found3a = df.loadCSVfile(fileName3Ea)

    ##Check if result of URLsearch for country is avalable
    fileName3Eb = localDir + "/URLsearch_result_" + country.upper() + "_def1.csv" ##Check name
    if os.path.isfile(fileName3Eb):
        urls_found3b = df.loadCSVfile(fileName3Eb) ##Contains urls found by URLsearch based on name detected (for both languages)
        ##deal with na/nan 
        urls_found3b.fillna("", inplace = True)
        ##Make sure all urls are included
        urls_found3bList = getAllUrlsSearched(urls_found3b)
    else:
        print(fileName3Eb + " was not found, is this OK?")
        urls_found3bList = []
        
    ##Combine found dataframes (not 3Eb!)
    frames = [urls_found1, urls_found2, urls_found2b, urls_found2c, urls_found3a]
    ##frames = [urls_found1, urls_found3]
    urls_foundDF = pandas.concat(frames)
    ##Drop duplicates (in first column)
    urls_foundDF = urls_foundDF.drop_duplicates()

    ##Create list of urls to be checked
    urls_found = list(urls_foundDF.iloc[:,0])
    if urls_found[0] == '0':
        urls_found = urls_found[1:len(urls_found)]
        
    if len(urls_found3bList) > 0:
        ##Add 3EbList
        urls_found = urls_found + urls_found3bList
        ##Remove any duplicates
        urls_found = list(set(urls_found))

    print(str(len(urls_found)) + " total number of links loaded")
    f.write(str(len(urls_found)) + " total number of links loaded\n")

        
    ##1b. Do cleaning and checking of all urls (removes any clearly erronious links, which seriously reduces processing later on)
    urls_found2 = []
    if cores >= 2:
        ##process multicore, by first clearing chunks
        
        ##create chunks
        chunks = numpy.array_split(urls_found, cores*multiTimes)
        ##Use all cores to process file
        pool = mp.Pool(cores*multiTimes)
        resultsP = pool.map(PreProcessList, [list(c) for c in chunks])        
        pool.close()
        pool.join()
    
        ##combine results
        for res in resultsP:
            for r in res:
                if not r in urls_found2:
                    urls_found2.append(r)
        
        ##Subsequently clear complete list
        urls_found2 = PreProcessList(urls_found2)
        del resultsP
        
    else:       
        ##process serial (as a whole)
        urls_found2 = PreProcessList(urls_found)        
        
    urls_found2.sort()
    print(str(len(urls_found2)) + " number of uniqe links will be processed and checked")
    f.write(str(len(urls_found2)) + " number of uniqe links will be processed and checked\n")


    ##1c seperate social media messages from the rest
    urls_foundSoc = []
    urls_found2b = []
    for url in urls_found2:
        ##Check Social media options
        if url.lower().find("twitter.com") > 0 or url.lower().find("facebook.com") > 0 or url.lower().find("linkedin.com") > 0 or url.lower().find("instagram.com") > 0 or url.lower().find("pinterest.com") > 0:
            ##If only the main page is reffered to, clear
            if url.lower().endswith("twitter.com") or url.lower().endswith("instagram.com") or url.lower().endswith("linkedin.com") or url.lower().endswith("facebook.com") or url.lower().endswith("pinterest.com"):
                url = ""
            ##Check for sttaus twitter link (should be reduced)
            if url.lower().find("twitter.com") > 0 and (url.lower().find("/status/") > 0 or url.lower().find("/statuses/") > 0):
                url = url[0:url.find('/status/')]
                url = url[0:url.find('/statuses/')]
            ##Check linked link which to exclude
            if url.lower().find("linkedin.com") > 0 and (url.lower().find("/groups/") > 0 or url.lower().find("/feed/") > 0 or url.lower().find("/help/") > 0):
                url = ""
           ##final check if need to add 
            if not url == "" and not url in urls_foundSoc:
                urls_foundSoc.append(url)
        else:
            urls_found2b.append(url)

    ##Reduce social media links as much as possible prior to checking online
    ##Get doms of urls_found2b
    urls_doms2b = [df.getDomain(x, False) for x in urls_found2b]
    ##remove www.
    urls_doms2b = [x.replace("www.", "") for x in urls_doms2b]
    ##remove any duplicates
    urls_doms2b = list(set(urls_doms2b))
    urls_doms2b.sort()
    ##Preprocess social media link
    urls_foundSoc = PreProcessSoc2(urls_foundSoc, urls_doms2b)
    ##These social media links MUST be checked     
    del urls_doms2b
    
    ##show results
    print(str(len(urls_foundSoc)) + " unique social media links found")
    f.write(str(len(urls_foundSoc)) + " unique social media links found\n")
    print(str(len(urls_found2b)) + " unique other websites links found")
    f.write(str(len(urls_found2b)) + " unique other websites links found\n")

    ##Save preprocessed social media links (intermediate results)
    fileName4S = "4_external_"+ country.upper() + lang.lower() + "_intSoc12.csv"
    totalSocUrls = pandas.DataFrame(urls_foundSoc)
    totalSocUrls.to_csv(fileName4S, index=False) 
    ##fileName4U = "4_external_"+ country.upper() + lang.lower() + "_intUrls1.csv"
    ##totalUrlsU = pandas.DataFrame(urls_found2b)
    ##totalUrlsU.to_csv(fileName4U, index=False) 

    ##2. Proccess social media data, in parallel or serial
    if socialSearch:
        print("Serial social media search option is used (1 process)")
        f.write("Serial social media search option is used (1 process)\n")

        ##Check for country and extra link (compared to urls_found2b)
        urls_extra_found = ProcessSoc(urls_foundSoc)
        ##urls_extra_found = list(set(urls_extra_found))
        ##Add anyrthong thats new
        for url in urls_extra_found:
            if not url in urls_found2b:
                urls_found2b.append(url) 

        print(str(len(urls_found2b)) + " total number of urls found")
        f.write(str(len(urls_found2b)) + " total number of urls found\n")
    
    else:
        print("No additional search on social media derived links is performed")
        f.write("No additional search on social media derived links is performed\n")

    ##3. Reduce urls by including domain of urls with 4 or more slashes
    dom_found2b = []
    for url in urls_found2b:
        ##Ending slash (if present)
        if url.endswith("/"):
            url = url[0:-1] 
        ##get number of //
        numS = url.count("/")
        if numS >= 4:
            ##get dom
            dom = df.getDomain(url)
            ##Only add if new
            if not dom in dom_found2b:
                dom_found2b.append(dom)
        else:
            ##only add if new
            if not url in dom_found2b:
                dom_found2b.append(url)
            ##Also add dom
            dom = df.getDomain(url)
            ##Only add if new
            if not dom in dom_found2b:
                dom_found2b.append(dom)
    
    ##Do final cleanup
    ##remove nanś and empty strings
    dom_found2c = [x for x in dom_found2b if not str(x) == 'nan' and not str(x) == '']
    dom_found2c = list(set(dom_found2c))
    dom_found2c.sort()
    
    ##Save list of urls
    fileName4 = "4_external_"+ country.upper() + lang.lower() + "1.csv"
    totalUrls = pandas.DataFrame(dom_found2c)
    totalUrls.to_csv(fileName4, index=False) 

    ##Indiacte how many doms are included in checking
    print(str(len(dom_found2c)) + " total number of domain based urls saved (after slash reduction)")
    f.write(str(len(dom_found2c)) + " total number of domain based urls saved (after slash reduction)\n")
    f.close()
    
    print("4 results file complete prepared for final step")
    print("Script 4a finished")

## ENd of script 4 a


