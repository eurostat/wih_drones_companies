#!/usr/bin/env python3
# -*- coding: utf-8 -*-

## Check urls found, input is a list of urls, first part of script4 only goes as afar as to check social media for additional urls
## Version with startPosition as input 
##with updated location search (inlcude text preprocessing and spaceses added to names)

#Load libraries 
import os
import sys
import re
import time
import pandas
import nltk
import multiprocessing as mp
import configparser

##Set directory
localDir = "/home/piet/R/Drone/Ini_scripts"
##Set directory
os.chdir(localDir)


##get regex for url matching in documents
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##Exclude vurl for often occuring not relevant urls 
urls_exclude = ["doi.org", "google.com", "youtube.com", "youtu.be", "worldcat.org", "b.tracxn.com", "gmail.com", "cookiedatabase.org", "twitter.com/share?", "twitter.com/hashtag", "twitter.com/intent/", "facebook.com/sharer.php?", "instagram.com/p/", "addtoany.com/add_to/", "whatsapp://send?", "pinterest.com/pin/", "t.me/share/url?"]

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
                vurl2 = PreProcessList([vurl2], country)
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
                vurl2 = PreProcessList([vurl2], country)
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
                vurl2 = PreProcessList([vurl2], country)
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
            vurl2 = PreProcessList([vurl2], country)
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
            vurl2 = PreProcessList([vurl2], country)
            if vurl2 == []:
                vurl2 = ''
            else:
                vurl2 = vurl2[0]

    return(vurl2, inCountry)

def PreProcessList(urls_list, country):
    urls_cleaned = []
    
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
    for url in urls_cleaned:
        ##Exclude any urls included in urls_exclude
        if not any(url.lower().find(x) > 0 for x in urls_exclude):
            urls_cleaned2.append(url)
        
    ##3c. ALso check for / or no slah variant and remove domains not referening to country being studied
    urls_cleaned2a, urlsNot = df.cleanLinks(urls_cleaned2, country, True)

    return(urls_cleaned2a)

##Check links after preprocessing
def ProcessLinks(urls_found, brows = 1):
    result = []
    ##init lists
    urls_in_country = []
    urls_not_in_country = []
    ##urls_extraScrape = [] ##list of sites that could not be scraped without browsersoup option

    count = 0
    ##2b. SCRAPE website
    for url in urls_found:
        ##init vars
        vurl = ''
        dom = ''
        ##set tristate of Country to unknonw (None)
        inCountry = None
        score = "0"
        action = ""
        words10 = ""
    
        ##show progress
        print(count)
    
        ##2b-1 Check url(s) for correctness and adjust
        ##ALL urls in urls_found have been thoroughly checked (also the new ones) via PreprocessingList function
           
        ##2b-2 Check content
        ##remove last / if included
        if url.endswith("/"):
            url = url[0:-1] 
        ##Get domain name
        dom = df.getDomain(url)    
        ##SHow progress
        print(url + " : " + dom)
    
        ##Check if this urls needs to be checked
        if not url == '' and not url in urls_in_country and not url in urls_not_in_country and not dom in urls_in_country and not dom in urls_not_in_country:

            ##Check for Social media options (shouldn o´ccur anymore, but you never know)
            if url.lower().find("twitter.com") > 0 or url.lower().find("facebook.com") > 0 or url.lower().find("linkedin.com") > 0 or url.lower().find("instagram.com") > 0 or url.lower().find("pinterest.com") > 0:
                ##Check country an link on cosial media
                urls_extra = ProcessSoc([url], brows)
                ##if something is found
                if not urls_extra == []:
                    for url1 in urls_extra:
                        if not url1 in urls_found:
                            urls_found.append(url1)
             
            else:           
                ##Other route (main webpage urls)
                print("Checking website")            
                ##Get url to which url ultimately reffers to, NEVER redirect social media links as they may return error links
                vurl = df.getRedirect(url)   ##Website that do not exists or respond will return as ''     
                ##Only websites that respond will be analysed below!!!!
                ##Check result, if empty
                if vurl == '':
                    print(url + " Website does not exists")        
                    action = "website does not exists"
                else:
                    ##Check vurl if different from url
                    if not vurl == url: 
                        ##Preprocess 
                        vurls = PreProcessList([vurl], country) ##returns a list
                        if not vurls == []:
                            vurl = vurls[0] ##get first from list
                        else:
                            vurl = ''
                    ##Check preprocessed vurl
                    if not vurl == '':
                        ##Get domain of vurl (may differ from first)
                        dom = df.getDomain(vurl) ##INCLUDE DOMAIN CHECKING (in general)?
                    else:
                        dom = ''
                        action = "not located in country studied"
                
                ##Check if already checked (vurl does not have to be identical to url)
                if not vurl == '' and not vurl in urls_in_country and not vurl in urls_not_in_country and not dom in urls_in_country and not dom in urls_not_in_country:
                    print(vurl + " is being checked")
                    ##Check slash at end
                    if vurl.endswith('/'):
                        vurl = vurl[0:-1]
                
                    ##0. Count slashes, To many to be a drone website
                    num = vurl.count("/")
                    ##print(str(num) + " slashes")
                    action = str(num) + " slashes"                                   
                    if num >= 4:
                        ##do nut anayise further, so many slashes do not refer to a company website
                        ##Add vurl to not country, prevents checking it again
                        if not vurl in urls_not_in_country:
                            urls_not_in_country.append(vurl)
                        ##Get and check domain
                        dom = df.getDomain(vurl)
                        ##Check if already checked somewhere
                        if not dom in urls_found and not dom in urls_in_country and not dom in urls_not_in_country:
                            vurl = dom ##Check domain if it has not been checked before
                        else:
                            vurl = ''
                    ##else, keep vurl and scrape it
                    ##for 2 and 3 slashes vurl and for extracted dom of vurl with 4 or more slashes
                    
                    ##0b. Check for emtpy vurl and do Country check   
                    if not vurl == '':
                        ##1. get content (try 3 different methods), always needed
                        soup, vurl2 = df.createsoupAllIn1(vurl, True, brows) ##Couldn this be ´without browsersoup option? Difficult multiprocessing part, seperate approach here?
                        text = df.visibletext(soup, False) ##was True            
                            
                        ##Only contunie when content is found
                        if len(text) > 0:
                            action = "text extracted and checked"
                                                
                            ##2. Check location mentioned, do easiest country check
                            if vurl.endswith("."+ country.lower()) or vurl.find("." + country.lower() + "/") > 0:
                                ##In country studied
                                inCountry = True
                            else:
                                ##Check text
                                inCountry = detectLocationCountry(text)
                                         
                                ##If No answer yet, check soup                                        
                                if inCountry == None:
                                    ##WHen NO location is found, check via links on page (if included)
                                    inter, exter = df.extractLinks(soup, vurl, False)
                                    ##sort links (look at the short urls first)
                                    inter2 = list(sorted(inter, key = len))
                                    interCount = 0
                                    ##Check internal links for contact page                                
                                    for link in inter2:
                                        ##Search for contact of about etc.
                                        linkC = link.replace(dom, '')
                                        linkC = linkC.lower()
                                        ##Check if cont_words occur in linkC
                                        wCon = [w for w in cont_words if linkC.find(w) > 0]
                                        ##Look for contact page about us page or who we are page (should contain contact info and location/city name)
                                        if len(wCon) > 0:
                                            ##get content of page as it may contain the address
                                            soup, vurl2 = df.createsoupAllIn1(link, True, brows)
                                            text2 = df.visibletext(soup, False) ##was True
                                            ##Check location
                                            inCountry = detectLocationCountry(text2)
                                            if inCountry == True or inCountry == False:
                                                break                                            
                                            else: ##None case
                                                interCount += 1 ##count number of visits
                                                pass ##Check all links until inCountry is either True or False
                                            ##Check numberof times visited (prevents infinite loop)
                                            if interCount > 10:
                                                break
                                    ##Only continue when no location is found yet
                                    if inCountry == None:
                                        print("Social media location check")
                                        exterCount = 0
                                        ##Try location via Social media
                                        for link in exter:
                                            ##Check via social media links
                                            if link.lower().find('twitter.com') > 0:
                                                vurl2, inCountry = Twitterchecks(link, brows) 
                                                exterCount += 1
                                            elif link.lower().find('facebook.com') > 0:
                                                vurl2, inCountry = Facebookchecks(link, brows)
                                                exterCount += 1
                                            elif link.lower().find('linkedin.com') > 0:
                                                vurl2, inCountry = Linkedinchecks(link, brows)
                                                exterCount += 1
                                            elif link.lower().find('instagram.com') > 0:
                                                vurl2, inCountry = Instagramchecks(link, brows)
                                                exterCount += 1
                                            elif link.lower().find("pinterest.com") > 0:
                                                vurl2, inCountry = Pinterestchecks(link, brows)
                                                exterCount += 1
                                            
                                            ##Set a max to the number of links to visit (10??)
                                            ##Show finding and break loop if True or False
                                            if inCountry == True or inCountry == False:
                                                break                                            
                                            ##Check number of soc media links checked
                                            if exterCount > 10:
                                                break
                                    ##Only contune when unknwon, as a last resort
                                    if inCountry == None:
                                        print("language specific check")
                                        ##Is this needed?
                                        if lang == "nl":##Sice stopwords are still included
                                            if text.lower().count(" het ") > 0:
                                                ##could be Dutch
                                                inCountry = True
                                                
                                            
                            ##Only continue when location is in country or unknown
                            if not inCountry == False:        
                                ##3. Word inspection of text  
                                ##drone
                                droneW1 = sum([text.lower().count(w) for w in droneW])
                                aerialW1 = sum([text.lower().count(w) for w in aerialW])
                                waterW1 = sum([text.lower().count(w) for w in waterW])            
                                busW1 = sum([text.lower().count(w) for w in busW])
                                contactW1 = sum([text.lower().count(w) for w in contactW])
            
                                ##drone compa droneW > 0, aerialW > 0 busW > 0 contactW > 0
                                print("d a w b c")
                                print(str(droneW1) + " " + str(aerialW1) + " " + str(waterW1) + " " + str(busW1) + " " + str(contactW1))
                                score = str(droneW1) + " " + str(aerialW1) + " " + str(waterW1) + " " + str(busW1) + " " + str(contactW1)
                                
                                ##Get top 10 words
                                words = text.lower().split(" ")
                                words = [x for x in words if len(x) > 1 and not x in nltk.corpus.stopwords.words(language)]
                                ##get wordfrequency
                                wordfreq = [words.count(w) for w in words]
                                wordDict = dict(list(zip(words,wordfreq)))            
                                ##convert to list and sort wordFerq
                                wordDict2 = [(wordDict[key], key) for key in wordDict]
                                wordDict2.sort()
                                wordDict2.reverse()
                                ##get top 10 as string
                                words10 = ' '.join(str(x) for x in wordDict2[0:10])
                                
                                if inCountry == True:
                                    ##print location result
                                    print("located in country studied (" + country.upper() + ")")
                                    action ="located in country studied"
                                    ##Add to country list
                                    if not vurl in urls_in_country:
                                        urls_in_country.append(vurl)
                                else:
                                    print("Location unknown")
                                    action = "Location unknown" 
                                    ##Add to list to exclude
                                    if not vurl in urls_not_in_country:
                                        urls_not_in_country.append(vurl)
                            else:
                                print("Not located in country studied (" + country.upper() + ")")
                                action = "not located in country studied"
                                ##Add to list to exclude
                                if not vurl in urls_not_in_country:
                                    urls_not_in_country.append(vurl)
                        else:
                            print("No text extracted")
                            action = "no text could be extracted"
                            ##Add vurl to extra list                            
                    else:
                        print("4 or more slash url and dom already included")
                        action = "4 or more slash url and domain name already included"
                else:
                    if action == "":
                        print("url or dom already checked or excluded")
                        action = "url or domain already checked or excluded"
        else:
            print("no check needed, already checked or website does not exist")
            if action == "":
                action = "already checked"
            ##else website does not exist
    
        ##Add findings to result list (only first occurences of websites will be included)
        result.append([url, vurl, inCountry, score, words10, action])            
        ##break
    
        count += 1
        print(" ")
        
        if not count == 0 and count % 200 == 0:
            ##Store intermediat result
            resultDF = pandas.DataFrame(data = result, columns = ["org_url", "url_checked", "inCountry", "score", "words10", "action"])
            fileNameR = "4_Result_" + country.upper() + lang.lower() + "_inter1.csv"
            resultDF.to_csv(fileNameR, sep = ";", index=False)
    
    
    ##if count > 20:
    ##    break
    ##return Df
    ##Convert result to dataframe
    resultDF = pandas.DataFrame(data = result, columns = ["org_url", "url_checked", "inCountry", "score", "words10", "action"])
    return(resultDF)

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
                if not vurl2 in dom_found and not vurl2 in urls_extra:
                    urls_extra.append(vurl2)

    return(urls_extra)         
 
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

##Multicore version of ProcessSoc
def ProcessLinksmp(urls_found, brows, outputQueue):
    links = []
    try:
        ##get results
        links = ProcessLinks(urls_found, brows)
    except:
        ##Ann error occured
        pass
    finally:        
        outputQueue.put(links)

### START #####################################################################
Continue = False
fileName = ''
startPos = 0

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
    if(len(sys.argv) >= 3):
        ##get record number to strat with
        startPos = int(sys.argv[2])
else:
    print("Use 'python3 Script4a_Ini.py <filename.ini> <OPT: startNumber'> to run program") 


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
        language = config.get('SETTINGS4', 'language')
        countryW1 = config.get('SETTINGS4', 'countryW1').split(',')
        countryW2 = config.get('SETTINGS4', 'countryW2').split(',')
        drone_words = config.get('SETTINGS4', 'drone_words').split(',')
        mem_words = config.get('SETTINGS4', 'mem_words').split(',')
        cont_words = config.get('SETTINGS4', 'cont_words').split(',')
        droneW = config.get('SETTINGS4', 'droneW').split(',')
        aerialW = config.get('SETTINGS4', 'aerialW').split(',')
        waterW = config.get('SETTINGS4', 'waterW').split(',')
        busW = config.get('SETTINGS4', 'busW').split(',')
        contactW = config.get('SETTINGS4', 'contactW').split(',')
        
        runParallel = config.getboolean('SETTINGS4', 'runParallel')
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
        #get municipalities of NL
        municl = pandas.read_csv(cityNameFile, sep = ";", header=None) ##Need plaatsnamen lijst hier
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

    ##if country == 'ie':
    ##    wCountries1.remove('Ireland')
    ##    wCountries1.remove('Iere')
    ##elif country == 'nl':
    ##    wCountries1.remove('Nederland')
    ##    wCountries1.remove('Netherlands')
    ##elif country == 'es':
    ##    wCountries1.remove('Espania')
    ##    wCountries1.remove('Spain') 
    ##elif country == 'de':
    ##   wCountries1.remove('Deutschland')
    ##   wCountries1.remove('Germany')
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
    logFile = "4b_results_" + country.upper() + lang.lower() + "1.txt"
    if startPos > 0 and os.path.isfile(logFile):
        ##Append info to existing file
        f = open(logFile, 'a')
    else:
        ##Create new file
        f = open(logFile, 'w')

    ##1. Obtain data                   
    ##1a. Get urls, from script 4a diferent files
    fileName = localDir + "/4_external_" + country.upper() + lang.lower() + "1.csv"
    urls_found = pandas.read_csv(fileName, sep = ",", header=None)

    ##Create list of urls to be checked
    dom_found = list(urls_found.iloc[:,0])
    if dom_found[0] == '0':
        dom_found = dom_found[1:len(dom_found)]

    print(str(len(dom_found)) + " total number of links loaded")
    f.write(str(len(dom_found)) + " total number of links loaded\n")

    ##Adjust when startPos > 0
    if startPos > 0 and startPos < len(dom_found):
        dom_found = dom_found[startPos:len(dom_found)]
        print(str(len(dom_found)) + " total number of links will be processed, starting at " + str(startPos))
        f.write(str(len(dom_found)) + " total number of links will be processed, starting at " + str(startPos) + "\n")

    ##Process links
    ##Can be multicore ##HERE##
    ##Challenge is the countr identification for NL
    ##Multicore or not
    if cores >= 2:
        ##Muliticore test version
        print("Parallel search option is used (max 2 different)")
        f.write("Parallel search option is used (max 2 different)")

        half = round(len(dom_found)/2)
        urls_Dom1 = dom_found[0:half]
        urls_Dom2 = dom_found[(half+1):len(dom_found)]

        ##init output queue
        out_q = mp.Queue()

        ##Init 2 simultanious queries, store output in queue
        p1 = mp.Process(target = ProcessLinksmp, args = (urls_Dom1, 1, out_q))
        p1.start()
        p2 = mp.Process(target = ProcessLinksmp, args = (urls_Dom2, 2, out_q)) ##Make sure it runs (deal with driver issues)
        p2.start()
        
        time.sleep(5)
    
        ##Wait till finished
        p1.join()
        p2.join()
    
        ##Combine results
        DFmp = []
        for i in range(2):
            DFmp.append(out_q.get())

        ##Combine Dataframes
        resultDF = pandas.concat(DFmp)
        ##Drop duplicates (in first column)
        ##resultDF = resultDF.drop_duplicates()
    else:
        print("Serial search option is used")
        f.write("Serial search option is used")
        resultDF = ProcessLinks(dom_found)

    ##Convert result to dataframe
    ##resultDF = pandas.DataFrame(data = result, columns = ["org_url", "url_checked", "inReland", "score", "action"])
    ##Store findings
    if startPos > 0 and startPos < len(dom_found):
        fileNameR = "4_Result_" + country.upper() + lang.lower() + "_" + str(startPos) + ".csv"
    else:
        fileNameR = "4_Result_" + country.upper() + lang.lower() + "_1.csv"
    ##Save DataFrame as file
    resultDF.to_csv(fileNameR, sep = ";", index=False)
    
    print("All urls processed, file saved")
    f.write("All urls processed, file saved")
    print("Script 4b finished")
    
##END of script 4b

 

