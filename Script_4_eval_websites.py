#!/usr/bin/env python3
# -*- coding: utf-8 -*-

## Check urls found, input is a list of urls

#Load libraries 
import io
import os
import re
import time
import pandas
from random import randint
import requests
import bs4
import nltk

##Set directory
os.chdir("/home/piet/R/Drone")

##Max wait time for website to react
waitTime = 60
##get regex for url matching in documents
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##Exclude vurl for often occuring not relevant urls 
urls_exclude = ["doi.org", "google.com", "youtube.com", "worldcat.org", "b.tracxn.com"]

##Set country
country = 'ie'

##import Functions
import Drone_functions as df 

##get municipalities of NL
if country == 'nl':
    municnl = pandas.read_csv("NLcity_names.csv", sep = ";", header=None) ##Need plaatsnamen lijst hier
    municL = list(municnl.iloc[:,0])
elif country == 'ie':
    ##municL = [" " + x.lower() +  " " for x in municL]
    municie = pandas.read_csv("IEcity_names.csv", sep = ";", header=None) ##Need plaatsnamen lijst hier
    municIE = list(municie.iloc[:,0])
    municIEl = [x.lower().strip() for x in municIE] ##lowercase stripped version
else:
    ##Not implemented yet
    pass

##Get domain list
euDom1 = df.euDom.copy()
euDom1.remove(country) ##Remove country extension from list of domain extensions

##1. Obtain data                   
##Get urls
fileNameE = "external_" + country.upper() + ".csv"
urls_foundEx = pandas.read_csv(fileNameE, sep = ";", header=None)
##Add variables to dataframe
urls_foundEx['inIreland'] = 'Unknown'
urls_foundEx['def_url'] = ''

##Create list of urls to be checked
urls_found = list(urls_foundEx.iloc[:,0])
if urls_found[0] == '0':
    urls_found = urls_found[1:len(urls_found)]

##functions
def detectLocationIreland(text):
    ##2. Check location mentioned
    included = []
    if text.find('ireland') > 0 or text.find('irish') > 0:
        ##print("Ireland found")
        included = ['ireland']
    else:
        ##detect if any irisch muncipality names occur in text
        included = list(filter(lambda x: re.findall(x, text), municIEl)) ##ireland is not included here
    if len(included) > 0:
         return(True)
    else:
        return(False)

def Twitterchecks(vurl):
    ##purpose is find the url to the webpage of this firm
    vurl2 = ''
    inIreland = False
    ##Check content, ignore irrelavnt twitter links
    if not vurl.endswith("twitter.com") and not vurl.find('twitter.com/share?') > 0:
        ##1. Use chromedriver to get webpage
        soup, vurl3 = df.chromesoup(vurl)
        ##get text (in lowercase)
        text = df.visibletext(soup, True)
                
        ##Check if no text is extracted
        if text == '':
            ##use browser approack
            soup, vurl3 = df.browsersoup(vurl)
            text = df.visibletext(soup, True)
                
        if len(text) > 0:
            ##Cut out relevant part (ignore tweet content)
            if text.find('lid geworden in'):
                text = text[0:text.find('lid geworden in')]
            ##2. Check location    
            inIreland = detectLocationIreland(text)
            
            ##3. Check for any urls in text
            pot_url = re.findall("((https?://)?(www\.)?[a-z0-9_-]+\.[a-z]{2,})", text)
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
                        if not url in urls_found:
                            vurl2 = url
                            break                       

    return(vurl2, inIreland)

def Facebookchecks(vurl):
    vurl2 = ''
    inIreland = False
    
    ##Check content, ignore irrelavnt facebook links
    if not vurl.endswith("facebook.com") and not vurl.find('facebook.com/sharer.php?') > 0:
               
        ##1. Use localborwser
        soup, vurl3 = df.browsersoup(vurl)
        ##get text
        text = df.visibletext(soup, True)

        ##Check is something is found
        if len(text) > 0:
            ##2. check for ireland
            inIreland = detectLocationIreland(text)
                
            ##3. Check for any urls in text
            pot_url = re.findall("((https?://)?(www\.)?[a-z0-9_-]+\.[a-z]{2,})", text)
            ##remove twitter.com, and keep first match in each sublist
            pot_url = [x[0] for x in pot_url if not x[0] == "facebook.com"]        
            ##Clear urls
            pot_url = df.checkCountries(pot_url, country)
            ##remove shortened .ly and .gy links
            pot_url = [x for x in pot_url if x.endswith('.ly') == 0 and x.endswith('.gy') == 0]
            ##Sort accoridng to length (decreasing)
            sorted_list = list(sorted(pot_url, key = len, reverse = True))
                    
            ##Decide what to do
            if inIreland and len(sorted_list) > 0:
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
                        if not url in urls_found:
                            vurl2 = url
                            break
                        
    return(vurl2, inIreland)

def Linkedinchecks(vurl):
    vurl2 = ''
    inIreland = False
    
    ##Check link
    if vurl.find("linkedin.com/company/") > 0:
        ##1. Use localborwser
        soup, vurl3 = df.browsersoup(vurl)
        ##get text
        text = df.visibletext(soup, True)

        ##Check result
        if len(text) > 0:
            ##Select the most interesting part of the text
            if text.find('see all details') > 0:
                text = text[0:text.find('see all details')]                
            ##2. check for ireland
            inIreland = detectLocationIreland(text) ##All are positive ??
            ##3. check for website in soup code
            link = ''
            if inIreland and str(soup).find("companyPageUrl") > 0:
                ##find all positions of company Url location
                pos = [m.start() for m in re.finditer('companyPageUrl', str(soup))]
                ##Check each occurence
                for p in pos:
                    ##get text + 100 chars
                    text1 = str(soup)[p:(p+100)]
                    ##extract any urls
                    link1 = re.findall(genUrl, text1)
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
                        if not link in urls_found:
                            vurl2 = link

    return(vurl2, inIreland)


##Create empty results list to include with org_url, url_checked, inIreland, drone scores  
result = []
##init lists
urls_ireland = []
urls_not_ireland = []
inIreland  = False
##Continue = False
count = 0
## STEP 1 SCRAPE website
for i in urls_found:
    ##init vars
    vurl = ''
    dom = ''
    inIreland  = False
    score = "0"
    action = ""
    
    ##show progress
    print(count)
    
    ##Check url(s) for correctness and adjust
    urls = df.checkUrls(i)
    ##Check findings and act accordingly
    if len(urls) > 1:
        for j in range(len(urls)):
            if j == 0:
                ##Make sure to process first url returned
                url = urls[0]
            else:
                ##Stor the others if they are new
                if not url[j] in urls_found:
                    urls_found.append(urls[j])            
    elif len(urls) == 1:
        url = urls[0]
    else:
        url = ''
                   
    ##Check content
    if not url == '':
        ##remove last / if included
        if url.endswith("/"):
            url = url[0:-1] 
        ##Get domain name
        dom = df.getDomain(url)    
        ##SHow progress
        print(url + " : " + dom)
    
    ##Check if this urls needs to be checked
    if not url == '' and not url in urls_ireland and not url in urls_not_ireland and not dom in urls_ireland and not dom in urls_not_ireland:
        ##print("In loop")
        ##Check options
        if url.find("twitter.com") > 0:
            ##check twitter options
            print("Twitter link")            
            
            vurl2, inIreland = Twitterchecks(url)
            
            ##Add vurl2 if inIreland
            if inIreland and not vurl2 == '':
                if not vurl2 in urls_found:
                    urls_found.append(vurl2)
                    print(vurl2 + " added Twitter link")
                    
        elif url.find("facebook.com") > 0:
            ##Check facebook options
            print("Facebook link")
            
            vurl2, inIreland = Facebookchecks(url)

            ##Add vurl2 if inIreland
            if inIreland and not vurl2 == '':
                if not vurl2 in urls_found:
                    urls_found.append(vurl2)  
                    print(vurl2 + " added Facebook link")

        elif url.find("linkedin.com") > 0:
            ##Check facebook options
            print("LinkedIn link")

            vurl2, inReland = Linkedinchecks(url)

            ##Add vurl2 if inIreland
            if inIreland and not vurl2 == '':
                if not vurl2 in urls_found:
                    urls_found.append(vurl2)  
                    print(vurl2 + " added LinkedIn link")
    
        else:           
            ##Other route (main webpage urls)
            ##Exclude url for often occuring not relevant urls 
            if not any(url.find(string) > 0 for string in urls_exclude):                        
                
                print("Checking website")            
                ##Get url to which url ultimately reffers to, NEVER redirect social media links as they may return error links
                vurl = df.getRedirect(url)   ##Website that do not exists or respond will return as ''     
                ##Check result, if empty
                if vurl == '':
                    print(url + " Website does not exists")        
                    action = "website does not exists"
                else:
                    ##Get domain of vurl (may differ from first)
                    dom = df.getDomain(vurl) ##INCLUDE DOMAIN CHECKING (in general)
            
                ##Check if already checked (vurl does not have to be identical to url)
                if not vurl == '' and not vurl in urls_ireland and not vurl in urls_not_ireland and not dom in urls_ireland and not dom in urls_not_ireland:
                    ##Country url check (exclude any obvious non-ireland domains)
                    vurl = df.checkCountry(vurl, euDom1)
            
                    ##Check finding
                    if vurl == '':
                        print(url + " can not be Irish")
                        action = "website domain is definitely not irish"
                    else:
                        print(vurl + " is being checked")
                        ##Check slash at end
                        if vurl.endswith('/'):
                            vurl = vurl[0:-1]
                
                        ##Excluding often mentioned urls that are certainly not relevant
                        if not any(vurl.find(string) > 0 for string in urls_exclude):                        
                            
                            ##0. Count slashes, To many to be a drone website
                            num = vurl.count("/")
                            print(str(num) + " number of slashes")
                            action = str(num) + " number of slashes"                    
                            if num >= 4:
                                ##do nut anayise further, so many slashes do not refer to a company website
                                ##Add vurl to not ireland, prevents checking it again
                                if not vurl in urls_not_ireland:
                                    urls_not_ireland.append(vurl)
                                ##Get and check domain
                                dom = df.getDomain(vurl)
                                ##Check if already checked somewhere
                                if not dom in urls_found and not dom in urls_ireland and not dom in urls_not_ireland:
                                    vurl = dom
                                else:
                                    vurl = ''
                                    
                            ##0b. Check for emtpy vurl (4 and more slahses and domain already included)        
                            if not vurl == '':
                                ##for 2 and 3 slashes vurl and for extracted dom of vurl with 4 or more slashes
                                ##print(vurl)
                        
                                ##1. get content (try 3 different methods)
                                soup, vurl2 = df.createsoupAllIn1(vurl)
                                text = df.visibletext(soup, True)            
                            
                                ##Only contunie when content is found
                                if len(text) > 0:
                                    action = "text extracted and checked"
                                    
                                    ##2. Check location mentioned
                                    inIreland = detectLocationIreland(text)
                                    if inIreland:
                                        print("in ireland")
                                    else:
                                        ##WHen NO location is found, check via twitter link (if included)
                                        inter, exter = df.extractLinks(soup, vurl, False)
                                
                                        ##Check internal links for contact page                                
                                        for link in inter:
                                            ##Search for contact of about etc.
                                            linkC = link.replace(dom, '')
                                            linkC = linkC.lower()
                                            if linkC.find('contact') > 0 or linkC.find('about') > 0:
                                                ##get content of page as it may contain the address
                                                soup, vurl2 = df.createsoupAllIn1(link)
                                                text = df.visibletext(soup, True)
                                                ##Check location
                                                inIreland = detectLocationIreland(text)
                                                if inIreland:
                                                    print("in ireland")
                                                break
                                                ##Check multiple pages?
                                        
                                        ##Only continue when no location is found yet
                                        if not inIreland:
                                            for link in exter:
                                                if link.find('twitter.com') > 0:
                                                    ##print(link)
                                                    vurl2, inIreland = Twitterchecks(vurl)                                        
                                                    if inIreland:
                                                        print("in ireland")
                                                    break
                                                elif link.find('facebook.com') > 0:
                                                    vurl2, inIreland = Facebookchecks(vurl)
                                                    if inIreland:
                                                        print("in ireland")
                                                    break
                                                elif link.find('linkedin.com') > 0:
                                                    vurl2, inIreland = Linkedinchecks(vurl)
                                                    if inIreland:
                                                        print("in ireland")
                                                    break
                                        ##ultimate check
                                        if not inIreland:
                                            if vurl.endswith(".ie") or vurl.find('.ie/') > 0:
                                                inIreland = True
                                                print("in ireland")
                            
                                    ##Only continue when location is in ireland
                                    if inIreland:        
                                        ##3. Word inspection     
                                        ##drone
                                        droneW = text.count('drone') + text.count('rpas') + text.count('uav') + text.count('uas') + text.count('unmanned')
                                        aerialW = text.count('fly') + text.count('aerial') + text.count('air')
                                        waterW = text.count('underwater') + text.count('uuv') ##+ text.count('uus') ##+ text1.count('')            
                                        busW = text.count('business') + text.count('company') + text.count('operator') + text.count('enterprise')
                                        contactW = text.count('contact') + text.count('about')
            
                                        ##drone compa droneW > 0, aerialW > 0 busW > 0 contactW > 0
                                        print("d a w b c")
                                        print(str(droneW) + " " + str(aerialW) + " " + str(waterW) + " " + str(busW) + " " + str(contactW))
                                        score = str(droneW) + " " + str(aerialW) + " " + str(waterW) + " " + str(busW) + " " + str(contactW)
                                
                                        ##Check findings
                                        if droneW > 1 and aerialW > 1 and waterW == 0 and busW > 0 and contactW > 0:  ##Ofte droneW ~aerialW
                                            ##is drone comapny, add to ireland list
                                            if not vurl in urls_ireland:
                                                urls_ireland.append(vurl)
                                        else:
                                            if not vurl in urls_not_ireland:
                                                urls_not_ireland.append(vurl)
                                        ##words = text1.split(" ")
                                        ##words = [x for x in words if len(x) > 1 and not x in nltk.corpus.stopwords.words(lang)]
                                        ##get wordfrequency
                                        ##wordfreq = [words.count(w) for w in words]
                                        ##wordDict = dict(list(zip(words,wordfreq)))            
                                        ##sort wordFerq
                                        ##wordDict2 = [(wordDict[key], key) for key in wordDict]
                                        ##wordDict2.sort()
                                        ##wordDict2.reverse()
                                    else:
                                        print("Not located in Ireland")
                                        if not vurl in urls_not_ireland:
                                                urls_not_ireland.append(vurl)
                                else:
                                    print("No text extracted")
                                    action = "no text could be extracted"
                            else:
                                print("url with 4 or more slashes and dom already included")
                                action = "url with 4 or more slashes and domain name already included"
                        else:
                            print("url excluded")
                            action = "website excluded"
                else:
                    print("already checked")
                    action = "already checked"
            else:
                print("url excluded")
                action = "website excluded"
            
            ##Add findings to result list (only first occurences of websites will be included)
            result.append([url, vurl, inIreland, score, action])            
            ##break
    else:
        if not url == '':
            print("Website already checked")
    
    count += 1
    print(" ")
    
    ##if count > 100:
    ##    break
    
##Convert result to dataframe
resultDF = pandas.DataFrame(data = result, columns = ["org_url", "url_checked", "inReland", "score", "action"])
##End store
fileNameR = "Result_" + country.upper() + "_1_2.csv"
resultDF.to_csv(fileNameR, sep = ";", index=False)
    