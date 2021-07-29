#!/usr/bin/env python3
# -*- coding: utf-8 -*-
## July 29 2021, version 1.01

## Check urls found, input is a list of urls, first part of script4 only goes as afar as to check social media for additional urls
##with updated location search (inlcude text preprocessing and spaceses added to names)

#Load libraries 
import os
import sys
import re
import time
import pandas
##import nltk
import multiprocessing as mp
import configparser

##Get current directory
localDir = os.getcwd()

##get regex for url matching in documents
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

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

        ##Check for existence of essential files
        fileName1E = localDir + "/1_external_" + country.upper() + lang.lower() + "1.csv"
        fileName2E = localDir + "/2_external_" + country.upper() + lang.lower() + "1.csv"
        fileName3E = localDir + "/3_externalPDF_" + country.upper() + lang.lower() + "1.csv" ##Check name
        if not os.path.isfile(fileName1E):
            print(fileName1E + " file was not found, make sure its available. Script halted")
            Continue = False
        if not os.path.isfile(fileName2E):
            print(fileName2E + " file was not found, make sure its available. Script halted")
            Continue = False
        if not os.path.isfile(fileName3E):
            print(fileName3E + " file was not found, make sure its available. Script halted")
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
    urls_found1 = pandas.read_csv(fileName1E, sep = ",", header=None)
    
    ##get results of script 2
    urls_found2 = pandas.read_csv(fileName2E, sep = ",", header=None) 
    
    ##Get intermediat result of script 2 (may not be created when empty)
    fileName2Eb = localDir + "/2_external_" + country.upper() + lang.lower() + "_drone_low_1.csv"
    if os.path.isfile(fileName2Eb):
        urls_found2b = pandas.read_csv(fileName2Eb, sep = ",", header=None) ##
    else:
        print(fileName2Eb + " was not found, is this OK?")
        urls_found2b = []
    
    ##Get intermediat result of script 2 (may not be created when empty)    
    fileName2Ec = localDir + "/2_external_" + country.upper() + lang.lower() + "_drone_high_1.csv"
    if os.path.isfile(fileName2Ec):
        urls_found2c = pandas.read_csv(fileName2Ec, sep = ",", header=None) ##May also contain relevant URLs
    else:
        print(fileName2Ec + " was not found, is this OK?")
        urls_found2c = []
    
    ##Get result from PDF extraction
    urls_found3 = pandas.read_csv(fileName3E, sep = ",", header=None)

    ##Combine findings
    frames = [urls_found1, urls_found2, urls_found2b, urls_found2c, urls_found3]
    ##frames = [urls_found1, urls_found3]
    urls_foundDF = pandas.concat(frames)
    ##Drop duplicates (in first column)
    urls_foundDF = urls_foundDF.drop_duplicates()

    ##Create list of urls to be checked
    urls_found = list(urls_foundDF.iloc[:,0])
    if urls_found[0] == '0':
        urls_found = urls_found[1:len(urls_found)]

    print(str(len(urls_found)) + " total number of links loaded")
    f.write(str(len(urls_found)) + " total number of links loaded\n")

    
    ##1b. Do cleaning and checking of all urls (removes any clearly erronious links, which seriously reduces processing later on)
    urls_found2 = PreProcessList(urls_found, country)
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

    ##show results
    print(str(len(urls_foundSoc)) + " unique social media links found")
    f.write(str(len(urls_foundSoc)) + " unique social media links found\n")
    print(str(len(urls_found2b)) + " unique other websites links found")
    f.write(str(len(urls_found2b)) + " unique other websites links found\n")

    ##2. Proccess social media data, in parallel or serial
    if cores >=2:
        ##Muliticore test version
        print("Parallel social media search option is used (max 2 different)")
        f.write("Parallel social media search option is used (max 2 different)\n")

        half = round(len(urls_foundSoc)/2)
        urls_Soc1 = urls_foundSoc[0:half]
        urls_Soc2 = urls_foundSoc[(half+1):len(urls_foundSoc)]

        ##init output queue
        out_q = mp.Queue()

        ##Init 2 simultanious queries, store output in queue
        p1 = mp.Process(target = ProcessSocmp, args = (urls_Soc1, 1, out_q))
        p1.start()
        p2 = mp.Process(target = ProcessSocmp, args = (urls_Soc2, 2, out_q)) ##Make sure it runs (deal with driver issues)
        p2.start()
        
        time.sleep(5)
    
        ##Wait till finished
        p1.join()
        p2.join()
    
        ##Combine results
        urls_extra_found = []
        for i in range(2):
            urls_extra_found.append(out_q.get())

        ##Add to urls_found2b
        for links in urls_extra_found:
            for url in links:
                if not url in urls_found2b:
                    urls_found2b.append(url)
    else:
        print("Serial social media search option is used (1 process)")
        f.write("Serial social media search option is used (1 process)\n")

        ##Check for country and extra link (compared to urls_found2b)
        urls_extra_found = ProcessSoc(urls_foundSoc)
        ##urls_extra_found = list(set(urls_extra_found))
        ##Add anyrthong thats nes
        for url in urls_extra_found:
            if not url in urls_found2b:
                urls_found2b.append(url)


    print(str(len(urls_found2b)) + " total number of urls found")
    f.write(str(len(urls_found2b)) + " total number of urls found\n")

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
    
    ##Save list of urls
    fileName4 = "4_external_"+ country.upper() + lang.lower() + "1.csv"
    totalUrls = pandas.DataFrame(dom_found2b)
    totalUrls.to_csv(fileName4, index=False) 

    ##Indiacte how many doms are included in checking
    print(str(len(dom_found2b)) + " total number of domain based urls saved (after slash reduction)")
    f.write(str(len(dom_found2b)) + " total number of domain based urls saved (after slash reduction)\n")
    f.close()
    
    print("4 results file complete prepared for final step")
    print("Script 4a finished")

## ENd of script 4 a


