#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 08:37:51 2021

@author: piet

Version 2, April 16, 2021

"""
##Drone functions file

#Load libraries 
from googlesearch import search
import os
import io
import re
import ssl
import urllib.request 
import bs4
import time
import timeout_decorator
from random import randint
import pandas
##from collections import defaultdict
##from googletrans import Translator
import nltk
import subprocess
##import pandas
##from random import randint
import requests
from pdfminer.high_level import extract_text

##Surpress specific warning of requests when ssl is needed
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
##For chrom driver
from selenium import webdriver  
from selenium.webdriver.chrome.options import Options
##From search_engine_parser package
from search_engine_parser.core.engines.yahoo import Search as YahooSearch ##Error in Baidu Search
from search_engine_parser.core.engines.aol import Search as AOLSearch
from search_engine_parser.core.engines.google import Search as GoogleSearch
from search_engine_parser.core.engines.bing import Search as BingSearch
from search_engine_parser.core.engines.duckduckgo import Search as DuckSearch

##required to deal with search_engine_parser in SPYDER
import nest_asyncio
nest_asyncio.apply()

##Settings  "drone operators ireland list (filetype:pdf OR filetype:doc OR filetype:docx)"
##Max wait time for website to react
waitTime = 60
##Country searched for (either 'ie, nl, es or de)
country = 'ie'
##Set maximum number of scraped pages per domain
maxDomain = 300
##Set header
header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}

##Regex
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
##define list of other EU countries domain + us, ca, au, sg, china
euDom = ['ad', 'ae', 'am', 'at', 'au', 'ba', 'be', 'bg', 'br', 'by', 'ca', 'ch', 'cn', 'cy', 'cz', 'de', 'dk', 'ee', 'es', 'fi', 'fo', 'fr', 'gg', 'gi', 'gl', 'gp', 'gr', 'hr', 'hu', 'ie', 'il', 'im', 'is', 'it', 'je', 'jp', 'kr', 'li', 'lt', 'lu', 'lv', 'mc', 'md', 'me', 'mk', 'mt', 'nl', 'no', 'nz', 'pm', 'pl', 'pt', 're', 'ro', 'rs', 'ru', 'se', 'sg', 'si', 'sk', 'su', 'tf', 'tr', 'ua', 'uk', 'us', 'wf', 'yt', 'za']
##list is not complete yet (but it is for Europe), added nz kr ae

##Init chromium
##UBUNTU STYLE LOCATION
CHROME_PATH = '/usr/bin/google-chrome'
CHROMEDRIVER_PATH = '/user/bin/chromedriver'
chrome_options = Options()  
chrome_options.add_argument("--headless") 
chrome_options.add_argument("--disable-gpu")   
chrome_options.add_argument("--enable-javascript") ##Some websites require this option
driver = None

##init driver with chrome
try:
    ##clse driver (if it exists, else error occurs)
    if driver:
        driver.close()
        driver.quit()
        driver = None
    else:
        driver = None
except:
    print("Iniializing Chrome driver")
finally:
    ##UBUNTU STYLE LOCATION
    CHROME_PATH = '/usr/bin/google-chrome'
    CHROMEDRIVER_PATH = '/user/bin/chromedriver'
    chrome_options = Options()  
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--disable-gpu")   
    chrome_options.add_argument("--enable-javascript") ##Some websites require this option
    ##Init new driver object
    driver = webdriver.Chrome(options=chrome_options)  

##Functions
##Restart chromedriver
def restartChromedDriver():
    try: 
        if driver:
            driver.close()
            driver.quit()
            driver = None
        else:
            driver = None
    except:
        print("Restarting Chrome driver")
    finally:
        ##UBUNTU STYLE LOCATION
        chrome_options = Options()  
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--disable-gpu")   
        chrome_options.add_argument("--enable-javascript") ##Some websites require this option
        ##Init new driver object
        driver = webdriver.Chrome(options=chrome_options)  
       
##querywords per country
def getQuery(country, getListSites = True):
    query = ""
    lang = "en"
    if country == 'ie':
        query = "(drone OR rpas OR uav OR uas) (operator OR company OR builder) ireland"
        if getListSites:
            query += " list"
    if country == 'nl':
        query = "(drone OR rpas OR uav OR uas) (operator OR bedrijf OR bouwer) nederland"
        if getListSites:
            query += " lijst" ##?operator
        lang = "nl"
    if country == 'es':
        query = "(drones OR rpas OR uav OR uas) (operador OR empresa OR constructor) espania"
        if getListSites:
            query += " lista"
        lang = "es"
    if country == 'de':
        query = "(drohnen OR rpas OR uav OR uas) (betrieber OR unternehmen OR bauer) deutschland"
        if getListSites:
            query += " liste"
        lang = "de"
    if country == 'qq':
        query = "drone rpas uav uas bedrijven lijst nederland vergunningen"
        if getListSites:
            query += " list"
    return(query, lang)    
  
##regurus checking of url to assure only valid urls are scraped
def checkUrls(url):
    urls2 = []
    ##Remove leading and lagging spaces
    url = url.strip()

    ##1. Check for multiple http
    if url.count('http') > 1:
        ##Split links
        urls = url.split('http')
        urls.remove('')
        urls = ['http' + x for x in urls]
        for url1 in urls:
            url1 = url1.strip()
            ##Check for empty
            if not url1 == '':
                ##Check for %3A and %2F
                url1 = url1.replace('%3A', ':')
                url1 = url1.replace('%2F', '/')
                ##Check http composition
                url1 = _checkUrl(url1)
                if not url1 == '':
                    urls2.append(url1)
                
    elif url.count('http') == 1:
        url = _checkUrl(url)
        if not url == '':
            urls2.append(url)
    else:
        url = 'http://' + url ##redirect will later adjust it if https if needed
        urls2.append(url)

    return(urls2)
    
##Internal check function used by CheckUrls
def _checkUrl(url):
    url = url.strip()
    
    ##1. First check for erroneous leading http/https
    if not url == '' and not url.startswith('http://') and not url.startswith('https://'):
        if url.startswith('http:/') or url.startswith('https:/'):
            url = url.replace('http:/', 'http://')
            url = url.replace('https:/', 'https://')
        elif url.startswith('http:') or url.startswith('https:'):
            url = url.replace('http:', 'http://')
            url = url.replace('https:', 'https://')            
        elif url.startswith('http/') or url.startswith('https/'):
            url = url.replace('http//', 'http://')
            url = url.replace('https//', 'https://')
            url = url.replace('http/', 'http://')
            url = url.replace('https/', 'https://')
        else: ##missing leading http part
            url = 'http://' + url
            
    ##2. check for to many // in url (correct http or https afterwards)
    if not url == '':
        while url.find('//') > 0:
            url = url.replace('//', '/')
        url = url.replace('http:/', 'http://')
        url = url.replace('https:/', 'https://')
    
    ##3. Check if result make sense
    if not re.match(genUrl, url):
        url = ''
        
    return(url)

def filterUrl(url, country):
    ##Default is False
    result = False
    
    ##Check country and filter options
    if country == 'ie':
        if re.findall(r'\.ie/', url) or (re.findall(r'ireland', url) and not re.findall(r'northern', url)):
            result = True
    if country == 'nl':
        if re.findall(r'\.nl/', url) or re.findall(r'nederland', url):
            result = True
    if country == 'es':
        if re.findall(r'\.es/', url) or re.findall(r'espania', url):
            result = True
    if country == 'de':
        if re.findall(r'\.de/', url) or re.findall(r'deutschland', url):
            result = True
    if country == 'qq':
        if re.findall(r'\.nl/', url) or re.findall(r'nederland', url) or re.findall(r'holland', url) or re.findall(r'netherlands', url) :
            result = True
            
    return(result)

##google search function INCLUDING  FILTER
def searchGoogle(query, country):
    urls_found = []
     ##Country specific setting
    domain = country
    if country == 'qq':
        domain = 'nl'
    lang = country
    if country == 'ie' or country == 'qq':
        lang = 'en'
        
    for i in search(query,        # The query you want to run
    tld = domain,  # The top level domain
    lang = lang,  # The language
    num = 10,     # Number of results per page
    start = 0,    # First result to retrieve
    stop = 50,  # Last result to retrieve
    pause = 2.0,  # Lapse between HTTP requests
    ):
    
        ##Only keep relevant urls
        if filterUrl(i, country):
            urls_found.append(i)

    ##remove any duplicates        
    ##urls = list(set(urls_found))
    
    return(urls_found)

#google search function NO FILTER MAX 300
def queryGoogle3(query, country, waitTime = 20, limit = 0):
    urls_found = []
     ##Country specific setting
    domain = country
    if country == 'qq':
        domain = 'nl'
    lang = country
    if country == 'ie' or country == 'qq':
        lang = 'en'

    ##Stop after first page
    if limit == 0:
        stop = None
    else:
        stop = limit
            
    for i in search(query,        # The query you want to run
    tld = domain,  # The top level domain
    lang = lang,  # The language
    num = 10,     # Number of results per page
    start = 0,    # First result to retrieve
    stop = stop,  # Last result to retrieve
    pause = waitTime,  # Lapse between HTTP requests
    ):
    
        ##Only keep relevant urls
        ##if filterUrl(i, country):
        if not i in urls_found:
            ##Add to list
            urls_found.append(i)        
            ##show progress
            if len(urls_found) % 10 == 0:
                print(str(len(urls_found)) + " links found G3")

        
    ##remove any duplicates        
    ##urls = list(set(urls_found))
    if limit > 0:
        print(str(len(urls_found)) + " links found G3")
    
    return(urls_found)

def queryGoogle3mp(query, country, outputQueue, limit = 0, waitTime = 20):
    links = []
    try:
        ##get results
        links = queryGoogle3(query, country, waitTime, limit)
    except:
        ##Ann error occured
        pass
    finally:        
        outputQueue.put(links)
  
##Internal function
def _removeDotDirs(cleanLink):
    ##Check occurence of "/../"
    res = cleanLink.count(r"/../")
    ##check result
    if(res > 0):
        while(res > -1):
            ##remove "/../" part
            cleanLink = re.sub(r"/[a-zA-Z0-9\_-]+/../", r"/", cleanLink)
            res = res - 1
    
    ##Also check for /./ (only remove as it has no effect)
    cleanLink = re.sub(r"/./", r"/", cleanLink)    
    return(cleanLink)

##Get urls from soup
##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def extractLinks(soup, thisurl, mailInclude=True):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    ##remove html-page part
    if thisurl.endswith(".html") or thisurl.endswith(".htm"):
        thisurl = re.sub('\/[a-zA-Z\_\-0-9]+\.html?$', '', thisurl)
    ##Remove slash from end if included in url scraped
    if thisurl.endswith("/"):
        thisurl = thisurl[:-1]
    ##create empty lists
    listlinksinternal = []
    listlinksexternal = [] 
    listmail = []
    ##Look for URLS
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##remove any hard returns
            cleanLink = re.sub('\n', '', cleanLink)
            ##exclude mailto: and tel: containing href's
            if not "mailto:" in cleanLink and not "tel:" in cleanLink and not "javascript:" in cleanLink:
                if cleanLink.startswith('http') and not (cleanLink.startswith(thisurl) or cleanLink.startswith(thisDomain)):
                    listlinksexternal.append(cleanLink)        
                elif cleanLink.find("www.") > 0 and not cleanLink.startswith('http') and not (cleanLink.startswith(getDomain(thisurl, False))):
                    if cleanLink.startswith("//"):
                        cleanLink = "http:" + cleanLink
                    elif cleanLink.startswith("/"):
                        cleanLink = "http:/" + cleanLink
                    else:
                        cleanLink = "http://" + cleanLink
                    listlinksexternal.append(cleanLink) ##add http version 
                else:
                    ##internal links
                    if cleanLink.startswith("/"):
                        cleanLink = thisurl + cleanLink                               
                    elif not cleanLink.startswith("http"):
                        cleanLink = thisurl + "/" + cleanLink                    
                    ##remove any /../sections
                    cleanLink = _removeDotDirs(cleanLink)
                    ##exclude links with # in it
                    if not '#' in cleanLink:
                        ##Add link
                        if cleanLink.endswith('.html') or cleanLink.endswith(".htm"):                   
                            listlinksinternal.append(cleanLink)
                        elif cleanLink.startswith('http') and re.match('^https?\:\/\/[a-zA-Z0-9]([a-zA-Z0-9-]+\.){1,}[a-zA-Z0-9]+\Z', cleanLink):
                            listlinksinternal.append(cleanLink)
                        elif not cleanLink.endswith(r'.[a-z]+'): 
                            listlinksinternal.append(cleanLink)
            elif mailInclude: ##When mail has to be included
                if "mailto:" in cleanLink and cleanLink.find('@') > 0:
                    ##mailaddress found
                    start = cleanLink.find('@') + 1
                    mail = cleanLink[start:len(cleanLink)]
                    if not mail in listmail:
                        listmail.append(mail)
                        
        ##Could include a flattext url and/or mail search here (similar to pdf search)
        ##DEAL WITH LINKS WITH HTTP://WWW.URL!.COM#HTTP://WWW.URL2.com (Split?)

        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        ##When mail derived urls should be included
        if mailInclude:
            ##Check mail based links
            dom = getDomain(thisurl, False)
            for mail in listmail:
                if not mail in dom:
                    ##check extern and domain should not already been included
                    if not any(mail in link for link in listlinksexternal):
                        ##Can include exclusion mail domains or filter out later
                        mail = "http://" + mail
                        listlinksexternal.append(mail)
                    
        ##Combine lists
        ##linksComb = list(listlinksinternal) + list(listlinksexternal)
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

def extractLinksText(text, regEx):
    links = []
    if len(text) > 0:
        ##Multiple matches may occur, choose max fittted (this is the first in the fitted list for each item)
        links = [item[0] for item in re.findall(regEx, text)]
        ##Only return unique links found in document
        links = list(set(links))                
    
    return(links)
    
##Funtion to get pdf and extract text
def getPdfText(url, lower = False):
    textPdf = ""
    if url.find(".pdf") > 0:
        try:
            ##Get connection to url inlcude header and deal with SSL
            response = requests.get(url, headers=header, verify=False)
            ##Checl response code
            if response.status_code == 200:
                ##All seems ok, extract text
                textPdf = extract_text(io.BytesIO(response.content)) 
                
            else:                
                ##Get pdf as local file !!!IS THIS NECESARRY??
                ##create file name
                localFile = "tempFile.pdf"
                response = requests.get(url, headers=header, stream=True)            
                ##download and save file
                with open(localFile, 'wb') as f:
                    f.write(response)
                ##get text from local file
                textPdf = extract_text(localFile)
        except:
            ##An error occured, accessing pdf file failed
            textPdf = ""
        finally:
            ##Check tolower
            if lower:
                textPdf = textPdf.lower()
    return(textPdf)


##scrape function (purely beautifulsoup based; no dealing with JavaScript)
@timeout_decorator.timeout(waitTime) ## If execution takes longer than 180 sec, TimeOutError is raised
def createsoup(site):
    ''' create a soup based on the url '''
    visited_site = site
    try: 
        req = urllib.request.Request(site)
        req.add_header('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')
        req.add_header('Accept', 'text/html,application/xhtml+xml,*/*')
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(req, context=context)
        visited_site = page.url  ##get url actually visted
        soup = bs4.BeautifulSoup(page, "lxml")
        return(soup, visited_site)
    except:
        ## print("not possible to read:", thisurl)
        return (0, visited_site)

def chromesoup(site):
    ''' create a soup from url with Chromedriver '''
    visited_site = site
    try:
        ##use chrome driver to get html code and create soup
        driver.get(site)
        time.sleep(1)
        visited_site = driver.current_url
        html2 = driver.page_source
        soup = bs4.BeautifulSoup(html2, "lxml") 
        driver.close() ##IS this needed?
        return(soup, visited_site)
    except:
        return(0, visited_site)

##Function to cut out domain name of an url (wtith our without http(s):// part)
def getDomain(url, prefix = True):
    top = ""
    if len(url) > 0:
        if url.find("/") > 0:
            ##remove any ? containing part (may be added to url)
            url = url.split('?')[0]
            ##Get domain name
            top = url.split('/')[2]
            ##add prefix
            if prefix:
                top = url.split('/')[0] + '//' + top
        else:
            top = url
    return(top)
    
##Function to cut out domain name of an url (wtith our without http(s):// part)
##Also works for non http leading urls
def getDomain2(url, prefix = True):
    top = ""
    if len(url) > 0:
        if url.find("/") > 0:
            ##remove any ? containing part (may be added to url)
            url = url.split('?')[0]
                
            if url.startswith("http"):
                ##Get domain name
                top = url.split('/')[2]
                ##add prefix
                if prefix:
                    top = url.split('/')[0] + '//' + top
            else:
                ##Get domain name
                top = url.split('/')[0]
                ##prefix is not relevant
        else:
            top = url
    return(top)

##Extract text from soup
def visibletext(soup, lower=True):
    text = ""
    try:
        ''' kill all the scripts and style and return texts '''
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        ##replacce breaks with spaces (otherwise some text is sticked together)
        soup = bs4.BeautifulSoup(str(soup).replace("<br/>", " "), "lxml")        
        text = soup.get_text(" ")
        if lower:
            text = text.lower()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)     
        ##replace tabs
        text = text.replace('\t', ' ')
    except:
        text = ""
    return(text)

##Remove non-letter characters from text
def clearText(text, country, stemming=False):
    ##english, spanish, german, dutch
    language = "english"
    if country == "nl": #https://www.iaa.ie/general-aviation/drones
        language = "dutch"
    if country == "es":
        language = "spanish"
    if country == "de":
        language = "german"
    
    wordList = []
    for word in text.lower().split():
        ''' remove weird characters  '''##https://www.iaa.ie/general-aviation/drones
        word = re.compile('[^a-z]').sub(' ', word)
        if word not in nltk.corpus.stopwords.words(language):                
                for w in word.split():
                    if len(w) > 1:
                        if stemming:
                            wordList.append((nltk.stem.snowball.SnowballStemmer(language).stem(w)).strip())
                        else:
                            wordList.append(w)
    return(wordList)
        
##get Title part from soup
def getTitle(soup):
    title = ""
    try:
        ##Get title
        title = soup.title.get_text().lower().strip() ##Crashes on pdf
        ##remove strange characters
        title = re.compile('[^a-z]').sub(' ', title)
        title = title.strip()
        ##remove double spaces
        while title.find('  ') > 0:
            title = title.replace('  ', ' ')        
    except:
        title = ""
    return(title)

##get keywords from soup
def getKeywords(soup):
    keywords = ""
    try:
        ##Get keywords tag
        keywords = soup.find(attrs={"name":'keywords'})
        if not keywords:
            keywords = soup.find(attrs={"name":'Keywords'})
        if not keywords:
            keywords = soup.find(attrs={"name":'KeyWords'})
        if keywords:
            keywords = keywords['content'].lower()
        if len(keywords.split()) > 20:
            keywords = " ".join(keywords.split()[0:20])
        wordsK = []
        keys = re.split(" |,|;", keywords)
        for key in keys:
            w1 = re.compile('[^a-z]').sub(' ', key)        
            w1 = w1.strip()
            if len(w1) > 0:
                wordsK.append(w1)
        keywords = ' '.join(wordsK)
    except:
        keywords = ""
    
    return(keywords)
 
@timeout_decorator.timeout(20) ## If execution takes longer than 20 sec, TimeOutError is raised
def getRedirect(url):
    vurl = ""
    if len(url) > 0:
        try:
            resp = requests.head(url, verify=False) ##Throws a warning which is surpressed
            if resp.is_redirect:
                ##get url referred to
                vurl = resp.headers['Location']
            else:
                ##Get url
                vurl = resp.url
            ##Check result, if more redirecting is needed (recursively?) 
            while url != vurl:            
                ##wait(3)
                time.sleep(3)
                resp = requests.head(vurl)
                if resp.is_redirect:
                    vurl2 = resp.headers['Location']
                else:
                    vurl2 = resp.url                
                ##copy linkshttps://airspace.flyryte.com
                url = vurl ##copy old
                vurl = vurl2 ##copy new    
        except:  ##timed out by website (because it does not exists)
            vurl = ""
    return(vurl)    

##Function to search website main and links on main page with regex
def searchWebsite(url, regEx):
    res = []
    if len(url) > 1:
        ##Get correct url
        vurl = getRedirect(url)
        ##Get soup
        soup, vurl = createsoup(vurl)
        ##Get text
        text = visibletext(soup)

        if len(text) > 0:
            ##Check main page and internal linked pages
            if not len(re.findall(regEx, text)) > 0:
                ##Get links
                inter, exter = extractLinks(soup, vurl, False)
                ##check the other links
                for link in inter:
                    soup2, vurl2 = createsoup(link)
                    ##get text
                    text2 = visibletext(soup2)
                    ##check text
                    if len(re.findall(regEx, text2)) > 0:
                        ##print("Found")
                        ##print(vurl2)
                        res = re.findall(regEx, text2)
            else:
                res = re.findall(regEx, text)

    return(res)

##create google function
def queryGoogle1(query, country, waitTime = 20, limit = 0):
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create search url
    url = "https://www.google." + country + "/search?q="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 + "&filter=0"
    
    ##get first page result
    soup, vurl = createsoup(url)
    ##extractLinks from soup
    inter, exter = _extractGoogleLinks(soup, url)

    ##if results are found
    if exter != '':
        ##Add links found to list
        for link in exter:
            if not link in urls_found:
                urls_found.append(link)
        
        ##Get number of links found
        prevLinks = len(urls_found)
        print(str(prevLinks) + " links found G1")
    
    ##Check multiple pgae search    
    if limit == 0 or (limit > 0 and limit > len(urls_found)):
        ##get link to next search page (if any)
        urls = [x for x in inter if x.find('start=') > 0]
    
        ##Check if something is found
        if not urls == []:
            ##get start number of this link
            startNum = _getStartNumber(urls)

            ##loop through pages
            while not startNum == 0:
                ##Get url with startNum in it
                if startNum == 10:
                    urlS = urls[0]
            
                ##Check if filter is included in link (if not add filter=0)
                if urlS.find('filter=0') == -1:
                    urlS = urlS + "&filter=0"

                ##wait a while
                time.sleep(waitTime+randint(0,40)) ##randomize between 20 and 60 sec
            
                ##get result from page
                soup, vurl = createsoup(urlS)
                ##extractLinks
                inter, exter = _extractGoogleLinks(soup, url)
    
                ##Add links found to list
                if exter != '':
                    for link in exter:
                        if not link in urls_found:
                            urls_found.append(link)
                        
                ##Check internal links found and check if new external links have been found
                if inter != '' and len(urls_found) >= prevLinks:   ##End when no new links are found
                    ##Store current number of links found
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found G1")

                    if limit == 0 or (limit > 0 and limit > len(urls_found)):
                        ##Store previous startNumber
                        prevStart = startNum
                        ##get link to next search page (if any)
                        urls = [x for x in inter if x.find('start=') > 0]
                        ##get startNum
                        startNum1 = _getStartNumber(urls) 
                        ##Check if higher then previous number
                        if startNum1 > prevStart:
                            startNum = 0 ##if nothing is found, search will end
                            ##Get next url from urls
                            for link in urls:
                                ##Check if exact start number occurs
                                if link.find('start=' + str(startNum1)) > 0:
                                    urlS = link
                                    startNum = startNum1
                                    break 
                        else:
                            ##end reached end loop
                            startNum = 0                        
                    else:
                        ##end search, return exact number of links requested by limit
                        if limit > 0:
                            urls_found = urls_found[0:limit]
                        startNum = 0
                else:
                    ##print('Search ended')
                    startNum = 0
                
    return(urls_found)

##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def _extractGoogleLinks(soup, thisurl):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    
    ##create empty lists
    listlinksinternal = []
    listlinksexternal = [] 
    
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##print(cleanLink)
            ##remove any hard returns
            ##cleanLink = re.sub('\n', '', cleanLink)
            
            ##Check result
            if cleanLink.startswith("/url?q=") and not cleanLink.find("google") > 0:
                ##add to external
                cleanLink = cleanLink.replace('/url?q=', '')
                if cleanLink.find('&') > 0:
                    end = cleanLink.find('&')
                    cleanLink = cleanLink[0:end]
                if cleanLink.find('%3') > 0:
                    end = cleanLink.find('%3')
                    cleanLink = cleanLink[0:end]    
                listlinksexternal.append(cleanLink)                
            else:
                if cleanLink.startswith("/search?q="):
                    ##append to internal
                    cleanLink = thisDomain + cleanLink
                    listlinksinternal.append(cleanLink)
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

def _getStartNumber(urls):
    num1 = 0
    num = []
    for link in urls:
        if link.find('start') > 0:
            ##get number after start
            begin = link.find('start')
            text = link[begin:len(link)]
            number = re.findall('[0-9]+', text)[0]
            number = int(number)
            num.append(number)
    
    ##get max
    num1 = max(num)        
    return(num1)
    
def queryGoogle1mp(query, country, outputQueue, limit = 0, waitTime = 20):
    links = []
    try:
        ##get results
        links = queryGoogle1(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)
    
def queryGoogle2(query, country, waitTime = 20, limit = 0):
    ##Makes use of search engine library, for Google
    dsearch = GoogleSearch()

    urls_found = []
    prevLinks = 0
    
    pageNum = 1
    
    while not pageNum == 0:
        ##set search args per page
        search_args = (query, pageNum)
        
        try:
            ##get country specific results        
            dresults = dsearch.search(*search_args, url='google.' + str(country)) ##Adjust to country and more
            ##get all links found
            links = dresults['links']
            ##extract all links in links returned
            links1 = [re.findall("http[s]?://[a-zA-Z0-9./-]+", string = x) for x in links]
            ##Select second link (if available, firts link is google)
            links2 = []
            for link in links1:
                for lin in link:
                    if not str(lin).find('google') > 0: 
                        links2.append(lin)
    
            ###Apppend links2
            for link in links2:
                if link.endswith('//'):
                    link = link[0:-1]
                if not link in urls_found:
                    urls_found.append(link)

            ##wait a while
            time.sleep(waitTime+randint(0,40)) 
            ##time.sleep(20+randint(0,40)) ##randomize between 20 and 60 sec
            
        except:
            ##An error occured
            print("An error occured while accessing Google")
            pageNum = 0
            pass
        
        finally:
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                ##Next 1
                if prevLinks <= len(urls_found):  ##CHECK FOR LAST PAGE?
                    ##continue
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found G2")
                    if not pageNum == 0:
                        pageNum += 1
                    print(len(dresults['titles']))
                else:
                    pageNum = 0
            else:
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0

    return(urls_found)

def queryGoogle2mp(query, country, outputQueue, limit = 0, waitTime = 20):
    links = []
    try:
        ##get results
        links = queryGoogle2(query, country, waitTime, limit)
    except:
        ##Ann error occured
        pass
    finally:
        outputQueue.put(links)

def queryBing1(query, country, waitTime = 5, limit = 0):
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create search urlhttps://www.bing.com/search?q=drone+rpas+uav+uas+operators+ireland&first=28&FORM=PERE2
    url = "https://www.bing.com/search?q="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 ##+ "&filter=0"
    
    ##get first page result
    soup, vurl = createsoup(url)
    ##extractLinks from soup
    inter, exter = _extractBingLinks(soup, url)

    ##if results are found
    if exter != '':
        ##Add links found to list
        for link in exter:
            if not link in urls_found:
                urls_found.append(link)
        
        ##Get number of links found
        prevLinks = len(urls_found)
        print(str(prevLinks) + " links found B1")
        
    ##Check if multiple pages need to be searches
    if limit == 0 or (limit > 0 and limit > len(urls_found)):
        ##Create link to next page
        urls = url + "&first=11&FORM=PORE"
        pageNum = 11
    
        while pageNum != 0:
            ##Get page
            time.sleep(waitTime)
            ##get soup of next page
            soup, vurl = createsoup(urls)    
            ##extractLinks from soup
            inter, exter = _extractBingLinks(soup, url)
        
            if exter != '':
                ##Add links found to list
                for link in exter:
                    if not link in urls_found:
                        urls_found.append(link)        
          
            ##Check internal links found and check if new external links have been found
            if not inter == '': ##and len(urls_found) >= prevLinks:  ##WHEN TO END?
                if limit == 0 or (limit > 0 and limit > len(urls_found)):
                    ##Store current number of links found
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found B1")        

                    ##store previous pageNume
                    prevPage = pageNum
             
                    ##Get link to next page
                    urls1 = [x for x in inter if x.find("first=") > 0]
                    ##Sort to assure PORE links are sorted from low to high
                    urls1.sort
                    urls = ''
                    if not urls1 == []:
                        for lin in urls1:
                            if lin.find('FORM=PORE') > 0: ##FORM+PORE indicates next page link
                                ##Choose first
                                number = getFirstNumber(lin)
                                if number > prevPage:
                                    urls = lin
                                    pageNum = number
                                    break                        
                    ##print(urls)
            
                    ##Check pageNum found
                    if pageNum < prevPage:
                        urls = ''
                        pageNum = 0
            
                    ##Check if urls is found
                    if urls == '':
                        pageNum = 0
                else:
                    if limit > 0:
                        urls_found = urls_found[0:limit]
                    pageNum = 0
            else:
                ##print('Search ended')
                pageNum = 0
                
    return(urls_found)
    
##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def _extractBingLinks(soup, thisurl):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    
    ##create empty listsurls_found = []
    listlinksinternal = []
    listlinksexternal = [] 
    
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##remove any hard returns
            ##cleanLink = re.sub('\n', '', cleanLink)
            
            ##Check result
            if cleanLink.startswith("http") and not cleanLink.find("bing") > 0 and not cleanLink.find("microsoft") > 0:
                ##add to external
                ##cleanLink = cleanLink.replace('/url?q=', '')
                if cleanLink.find('&') > 0:
                    end = cleanLink.find('&')
                    cleanLink = cleanLink[0:end]
                listlinksexternal.append(cleanLink)                
            else:
                if cleanLink.startswith("/search?q=") or cleanLink.startswith(thisDomain):
                    ##append to internal
                    cleanLink = thisDomain + cleanLink
                    listlinksinternal.append(cleanLink)
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

def getFirstNumber(urls):
    num1 = 0
    num = []
    if urls.find('first') > 0:
        ##get number after start
        begin = urls.find('first=')
        text = urls[begin:len(urls)]
        number = re.findall('[0-9]+', text)[0]
        number1 = int(number)
        num.append(number1)
    
    ##get max
    num1 = max(num)        
    return(num1)

def queryBing1mp(query, country, outputQueue, limit = 0, waitTime = 5):
    links = []
    try:
        ##get results
        links = queryBing1(query, country, waitTime, limit)
    except:
        ##Ann error occured
        pass
    finally:
        outputQueue.put(links)

##Yandex does a very thorough bot check!!!
def queryYandex(query, country, waitTime = 20):
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create search urlhttps://www.bing.com/search?q=drone+rpas+uav+uas+operators+ireland&first=28&FORM=PERE2
    url = "https://yandex.com/search/?text="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 ##+ "&filter=0"
    
    ##get first page result
    soup, vurl = createsoup(url)  ##HAs a robot check
    ##extractLinks from soup
    inter, exter = _extractYandexLinks(soup, url)

    ##if results are found
    if exter != '':
        ##Add links found to list
        for link in exter:
            if not link in urls_found:
                urls_found.append(link)
        
        ##Get number of links found
        prevLinks = len(urls_found)
        print(str(prevLinks) + " links found Yandex")
        
    ##Create link to next page
    urls = url + "&p=1"
    pageNum = 1
    
    while pageNum != 0:
        ##Get page
        time.sleep(waitTime)
        ##get soup of next page
        soup, vurl = createsoup(urls)    
        ##extractLinks from soup
        inter, exter = _extractYandexLinks(soup, url)
        
        if exter != '':
            ##Add links found to list
            for link in exter:
                if not link in urls_found:
                    urls_found.append(link)        
          
        ##Check internal links found and check if new external links have been found
        if inter != '' and len(urls_found) > prevLinks:
            ##get next start link
            prevPage = pageNum
            pageNum += 1
    
            ##Create link to next page
            urls = urls.replace('p='+str(prevPage), 'p='+str(pageNum))
                
            ##Store current number of links found
            prevLinks = len(urls_found)
            print(str(prevLinks) + " links found Yandex")        
        else:
            ##print('Search ended')
            pageNum = 0
                
    return(urls_found)

##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def _extractYandexLinks(soup, thisurl):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    
    ##create empty listsurls_found = []
    listlinksinternal = []
    listlinksexternal = [] 
    
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##print(cleanLink)
            ##remove any hard returns
            ##cleanLink = re.sub('\n', '', cleanLink)
            
            ##Check result
            if cleanLink.startswith("http") and not cleanLink.find("yandex") > 0:
                ##add to external
                ##cleanLink = cleanLink.replace('/url?q=', '')
                if cleanLink.find('&') > 0:
                    end = cleanLink.find('&')
                    cleanLink = cleanLink[0:end]
                listlinksexternal.append(cleanLink)                
            else:
                if cleanLink.startswith("/search/?text=") or cleanLink.startswith(thisDomain):
                    ##append to internal
                    cleanLink = thisDomain + cleanLink
                    listlinksinternal.append(cleanLink)
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

def queryYahoo1(query, country, waitTime = 5, limit = 0):
    print("preferably use queryYahoo2")
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create search urlhttps://www.bing.com/search?q=drone+rpas+uav+uas+operators+ireland&first=28&FORM=PERE2
    url = "https://search.yahoo.com/search?p="
    ##url += ";_ylt=A0geKepjo1Rgz3oA7K5XNyoA;_ylu=Y29sbwNiZjEEcG9zAzEEdnRpZAMEc2VjA3BhZ2luYXRpb24-?p="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 ##+ "&filter=0"
    
    driver.get(url)
    vurl = driver.current_url
    
    try:
        ##There could be an Accept all button
        Accept = driver.find_element_by_xpath('//button[@value="agree"]')
        ##Check element
        while Accept:
            ##Click on next page button
            driver.execute_script('arguments[0].scrollIntoView();', Accept)
            Accept.click()
            time.sleep(waitTime)
    except:
        print("No Accept button")
    finally:
        ##just continue
        ##get html
        html2 = driver.page_source
        ##Make soup from html code
        soup = bs4.BeautifulSoup(html2, "lxml")
        ##Get links
        inter, exter = _extractYahooLinks(soup, url)
    
        ##if results are found
        if exter != '':
            ##Add links found to list
            for link in exter:
                if not link in urls_found:
                    urls_found.append(link)
        
            ##Get number of links found
            prevLinks = len(urls_found)##
            print(str(prevLinks) + " links found Y1")
     
        ##Check if multipages need to be searched
        if limit == 0 or (limit > 0 and limit > len(urls_found)):
            ##Create link to next page
            urls = url + "&norw=1"
            pageNum = 1
    
            while pageNum != 0:
                ##Get page
                time.sleep(waitTime)
                ##get soup of next page
                soup, vurl = createsoup(urls)    
                ##extractLinks from soup
                inter, exter = _extractYahooLinks(soup, url)
        
                if exter != '':
                    ##Add links found to list
                    for link in exter:
                        if not link in urls_found:
                            urls_found.append(link)        
          
                ##Check internal links found and check if new external links have been found
                if limit == 0 or (limit > 0 and limit > len(urls_found)):
                    if len(urls_found) >= prevLinks:
                        ##get next start link
                        prevPage = pageNum
                        pageNum += 1
    
                        ##Create link to next page
                        urls = urls.replace('norw='+str(prevPage), 'norw='+str(pageNum))
                
                        ##Store current number of links found
                        prevLinks = len(urls_found)
                        print(str(prevLinks) + " links found Y1")        
                    else:
                        ##print('Search ended')
                        pageNum = 0
                else:
                    if limit > 0:
                        urls_found = urls_found[0:limit]
                    pageNum = 0
        else:
            pageNum = 0
    return(urls_found)
   
##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def _extractYahooLinks(soup, thisurl):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    
    ##create empty listsurls_found = []
    listlinksinternal = []
    listlinksexternal = [] 
    
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##print(cleanLink) ##Links are hidden in html page, via chomeium browser
            ##print(link)
            ##remove any hard returns
            ##cleanLink = re.sub('\n', '', cleanLink)
            
            ##Check result
            if cleanLink.startswith("http") and not cleanLink.find("yahoo") > 0:
                ##add to external
                ##cleanLink = cleanLink.replace('/url?q=', '')
                if cleanLink.find('&') > 0:
                    end = cleanLink.find('&')
                    cleanLink = cleanLink[0:end]
                listlinksexternal.append(cleanLink)                
            else:
                if cleanLink.startswith("/html/?q") or cleanLink.startswith(thisDomain):
                    ##append to internal
                    cleanLink = thisDomain + cleanLink
                    listlinksinternal.append(cleanLink)
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

def queryYahoo1mp(query, country, outputQueue, limit = 0, waitTime = 5):
    links = []
    try:
        ##get results
        links = queryYahoo1(query, country, waitTime, limit)
    except:
        ##Ann error occured
        pass
    finally:
        outputQueue.put(links)

##DuckDUckGo search via chromium webdriver, works well
def queryDuck1(query, country, waitTime = 5, limit = 0):
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create start search url, use html version of website
    url = "https://html.duckduckgo.com/html/?q="
    ##url += ";_ylt=A0geKepjo1Rgz3oA7K5XNyoA;_ylu=Y29sbwNiZjEEcG9zAzEEdnRpZAMEc2VjA3BhZ2luYXRpb24-?p="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 + "&t=h_&ia=web"
    
    ##USe CHROMEDRIVER TO FIND LINKS
    driver.get(url)
    vurl = driver.current_url
    ##get html
    html2 = driver.page_source
    ##Make soup from html code
    soup = bs4.BeautifulSoup(html2, "lxml")
    ##Get links
    inter, exter = _extractDuckLinks(soup, url)

    ##Add links to urls
    if exter != '':
        ##Add links found to list
        for link in exter:
            if not link in urls_found:
                urls_found.append(link)
        
        ##Get number of links found
        prevLinks = len(urls_found)
        print(str(prevLinks) + " links found D1")
    
    ##Check if multiple pages need to be searches
    if limit == 0 or (limit > 0 and limit > len(urls_found)):
        ##Set page Number
        pageNr = 1 
    
        try:
            ##GIs there a next page?    
            nxt_page = driver.find_element_by_xpath('//input[@value="Next"]')
            ##Check element
            while nxt_page:
                ##Click on next page button
                driver.execute_script('arguments[0].scrollIntoView();', nxt_page)
                nxt_page.click()
                time.sleep(waitTime)
                ##print(pageNr+1)
            
                ##get html
                html2 = driver.page_source
                ##Make soup from html code
                soup = bs4.BeautifulSoup(html2, "lxml")
                ##Get links
                inter, exter = _extractDuckLinks(soup, url)

                ##Add links to urls
                if exter != '':
                    ##Add links found to list
                    for link in exter:
                        if not link in urls_found:
                            urls_found.append(link)
        
                    ##Get number of links found
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found D1")
                
                if limit == 0 or (limit > 0 and limit > len(urls_found)):
                    ##if there is a next page
                    nxt_page = driver.find_element_by_xpath('//input[@value="Next"]')
                    pageNr += 1
                else:
                    if limit > 0:
                        urls_found = urls_found[0:limit]
                    pageNr = 0
        except:
            ##An error occured (no next page button)
            pageNr = 0
         
    return(urls_found)    

def queryDuck1mp(query, country, outputQueue, limit = 0, waitTime = 5):
    links = []
    try:        
        ##get results
        links = queryDuck1(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)

##Duck duck go via direct scraping (BIG ISSUE GETTING THE NEXT PAGE: PREFER chromium based driver approach)
def queryDuck2(query, country, waitTime = 20, limit = 0):
    print("preferably use queryDuck1")
    ##init search result
    urls_found = []
    prevLinks = 0
    
    ##Create search url, use html version of website
    url = "https://html.duckduckgo.com/html/?q="
    ##url += ";_ylt=A0geKepjo1Rgz3oA7K5XNyoA;_ylu=Y29sbwNiZjEEcG9zAzEEdnRpZAMEc2VjA3BhZ2luYXRpb24-?p="
    ##adjust words to search query words
    query2 = query.replace(' ', '+')
    ##create query (do NOT filter results)
    url = url + query2 ##+ "&t=h_&ia=web"
    
    ##Create soup
    soup, vurl = createsoup(url)
    #extractLinks from soup
    inter, exter = _extractDuckLinks(soup, url)

    ##if results are found
    if exter != '':
        ##Add links found to list
        for link in exter:
            if not link in urls_found:
                urls_found.append(link)
        
        ##Get number of links found
        prevLinks = len(urls_found)
        print(str(prevLinks) + " links found D2")
        
    ##Check if multiple pages need to be scraped
    if limit == 0 or (limit > 0 and limit > len(urls_found)):
        ##Create link to next page
        ##urls = url + "&s=$(($n*30))&dc=$(($n*30+1))&v=l&o=json&api=/d.js"
        s = _getS(soup)
        ##if int(s) < len(urls_found):
        ##    s = str(len(urls_found)+1)
        dc = _getDC(soup)
        if int(dc) <= int(s):
            dc = str(int(s) + 30)    
        urls = url + "&s=" + str(s) + "&v=l&dc=" + str(dc)
    
        ##urls = url + "&norw=1"
        pageNum = 1
    
        while pageNum != 0:
            ##Get page
            time.sleep(waitTime) ##Check wait time needed
            ##Get first age results
            soup, vurl = createsoup(urls)
            #extractLinks from soup
            inter, exter = _extractDuckLinks(soup, urls)

            if exter != '':
                ##Add links found to list
                for link in exter:
                    if not link in urls_found:
                        urls_found.append(link)        
          
            ##Check internal links found and check if new external links have been found
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                if len(urls_found) >= prevLinks and not len(exter) == 0:
                    ##get next start link
                    prevPage = pageNum
                    pageNum += 1
    
                    ##Create link to next page, see https://stackoverflow.com/questions/35974954/duck-duck-go-html-version-get-next-page-of-results-url-query-param
                    ##urls = urls.replace('&s='+str(30+(50*(prevPage-1))), '&s='+str(30+(50*(pageNum-1))))
                    s = _getS(soup)
                    ##if int(s) < len(urls_found):
                    ##    s = str(len(urls_found)+1)
                    dc = _getDC(soup)
                    if int(dc) <= int(s):
                        dc = str(int(s) + 30)           
                    urls = url + "&s=" + str(s) + "&v=l&dc=" + str(dc) ##+ "&v=l" ##&v=l&o=json&api=/d.js"    
                    ##urls = urls.replace('&dc='+str(30-1+(50*(prevPage-1))), '&dc='+str(30-1+(50*(pageNum-1))))
                    print(urls)    
                    ##Store current number of links found
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found D2")        
                else:
                    ##print('Search ended')
                    pageNum = 0
            else:
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0
    return(urls_found)
   
##Extract urls from soup, CHECK FOR LINKS IDENTICAL TO URL??
def _extractDuckLinks(soup, thisurl):  
    ''' find all links referred to by the website internally and externally '''
    thisurl = thisurl.strip()
    thisDomain = getDomain(thisurl)
    
    ##create empty listsurls_found = []
    listlinksinternal = []
    listlinksexternal = [] 
    
    try:
        for link in soup.find_all('a', href=True):
            ##extract link and make sure to remove any leading and lagging spaces
            cleanLink = link['href'].strip() 
            ##print(cleanLink) ##Links are hidden in html page, via chomeium browser
            ##print(link)
            ##remove any hard returns
            ##cleanLink = re.sub('\n', '', cleanLink)
            
            ##Check result
            if cleanLink.startswith("http") and not (cleanLink.find("duckduckgo") > 0 or cleanLink.find("spreadprivacy") > 0 or cleanLink.find("donttrack.us") > 0):
                ##add to external
                ##cleanLink = cleanLink.replace('/url?q=', '')
                ##if cleanLink.find('&') > 0:
                ##    end = cleanLink.find('&')
                ##    cleanLink = cleanLink[0:end]
                listlinksexternal.append(cleanLink)                
            else:
                if cleanLink.startswith("/"):
                    ##append to internal
                    cleanLink = thisDomain + cleanLink
                    listlinksinternal.append(cleanLink)
                elif cleanLink.startswith('http') and cleanLink.find("duckduckgo") > 0:
                    ##append to internal
                    listlinksinternal.append(cleanLink)
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = list(set(listlinksinternal))                  
        if len(listlinksexternal) > 0:
            listlinksexternal = list(set(listlinksexternal))
        
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string##report number of links found
        print("Something went wrong in DuckDuckGo") 
               
    return ("", "")

def _getS(soup):
    val = '0'
    try:
        ##Found input node s
        Snode = soup.find_all("input", {"name": "s"})
        ##Get the highest value
        for nod in Snode:
            val1 = nod["value"]
            if int(val1) > int(val):
                val = val1
    except:
        val = '-1'
    ##return value
    return(val)
    
def _getDC(soup):
    val = '0'    
    try:
        ##Found input node s
        DCnode = soup.find_all("input", {"name": "dc"})
        ##Get the highest value
        for nod in DCnode:
            val1 = nod["value"]
            if int(val1) > int(val):
                val = val1
    except:
        val = '-1'
    ##return value
    return(val)
 
def queryDuck2mp(query, country, outputQueue, limit = 0, waitTime = 5):
    links = []
    try:
        ##get results
        links = queryDuck2(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)

##DuckDuckGo search engine via search engine library
def queryDuck3(query, country, waitTime = 20, limit = 0):
    ##Search Yahoo
    dsearch = DuckSearch()
    ##init list
    urls_found = []
    prevLinks = 0
    ##set start page Nume
    pageNum = 1
    while not pageNum == 0:        
        ##set search args per page
        search_args = (query, pageNum)
        
        try:
            ##get results
            dresults = dsearch.search(*search_args)
            ##get links crude
            links = dresults['links']
            ##extract links found (remove added %F3 etc)
            ##links1 = [re.findall("http[s]?://[a-zA-Z0-9./-]+", string = x) for x in links]
        
            ##Check links, add new ones
            for link in links:
                if not link in urls_found:
                    urls_found.append(link)

            ##Wait between scraper
            time.sleep(waitTime)
        
        except:
            print("An error occurred while accessing DuckDuckGo")
            pageNum = 0
            pass
        
        finally:
            ##Check for multiple page scrape
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                ##Check if new page needs to be scraped
                if prevLinks <= len(urls_found):
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found D3") 
                    if not pageNum == 0:
                        pageNum += 1
                else: 
                    pageNum = 0
            else:
                ##report number of links found
                print(str(len(urls_found)) + " links found D3") 
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0
                
    return(urls_found)

def queryDuck3mp(query, country, outputQueue, limit = 0, waitTime = 20):
    links = []
    try:
        ##get results
        links = queryDuck3(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)
   
##Yahoo search engine via search engine library
def queryYahoo2(query, country, waitTime = 5, limit = 0):
    ##Search Yahoo
    ysearch = YahooSearch()
    ##init list
    urls_found = []
    prevLinks = 0
    ##set start page Nume
    pageNum = 1
    while not pageNum == 0:        
        ##set search args per page
        search_args = (query, pageNum)
        
        try:
            ##get results
            yresults = ysearch.search(*search_args)
            ##get links crude
            links = yresults['links']
            ##extract links found (remove added %F3 etc)
            ##links1 = [re.findall("http[s]?://[a-zA-Z0-9./-]+", string = x) for x in links]
        
            ##Check links, add new ones
            for link in links:
                if not link in urls_found:
                    urls_found.append(link)

            ##Wait between scraper
            time.sleep(waitTime)
        
        except:
            print("An error occurred while accessing Yahoo")
            pageNum = 0
            pass
        
        finally:
            ##Check for multiple page scrape
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                ##Check if new page needs to be scraped
                if prevLinks <= len(urls_found):
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found Y2") 
                    if not pageNum == 0:
                        pageNum += 1
                    ##print(str(len(yresults['titles'])))
                else: 
                    pageNum = 0
            else:
                ##report number of links found
                print(str(len(urls_found)) + " links found Y2") 
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0
                
    return(urls_found)

def queryYahoo2mp(query, country, outputQueue, limit = 0, waitTime = 5):
    links = []
    try:
        ##get results
        links = queryYahoo2(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)

##AOL search engine via search engine library
def queryAOL(query, country, waitTime = 20, limit = 0):
    ##Search Yahoo
    asearch = AOLSearch()
    ##init list
    urls_found = []
    prevLinks = 0
    ##set start page Nume
    pageNum = 1
    while not pageNum == 0:        
        ##set search args per page
        search_args = (query, pageNum)
        
        try:
            ##get results
            aresults = asearch.search(*search_args)
            ##get links crude
            links = aresults['links']
            ##preprocess results (return aol link in which the url is included)
            links1 = [x.replace('%3a', ':') for x in links] 
            links1 = [x.replace('%2f', '/') for x in links1] 
            ##extract second link in links returned
            links2 = [re.findall("http[s]?://[a-z0-9./-]+", string = x)[1] for x in links1]
    
            ##Check links, add new ones
            for link in links2:
                ##Check and remove double // if included
                if link.endswith('//'):
                    link = link[0:-1]
                ##Check if link is new
                if not link in urls_found:
                    urls_found.append(link)

            ##Wait between scraper
            time.sleep(waitTime)
        
        except:
            print("An error occured while accessing AOL")
            pageNum = 0
            pass
        
        finally:
            ##Check if multple pages need to be scraped
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                ##Check if new page needs to be scraped
                if prevLinks < len(urls_found):
                    ##continue
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found AOL")
                    if not pageNum == 0:
                        pageNum += 1
                else:
                    pageNum = 0
            else:
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0
              
    return(urls_found)

def queryAOLmp(query, country, outputQueue, limit = 0, waitTime = 20):
    links = []
    try:
        ##get results
        links = queryAOL(query, country, waitTime, limit)
    except:
        ##An error occured
        pass
    finally:
        outputQueue.put(links)


##Yahoo search engine via search engine library
def queryBing2(query, country, waitTime = 20, limit = 0):
    ##Search Bing
    bsearch = BingSearch()
    ##init list
    urls_found = []
    prevLinks = 0
    ##set start page Nume
    pageNum = 1
    while not pageNum == 0:        
        ##set search args per page
        search_args = (query, pageNum)
        
        try:
            ##get results
            bresults = bsearch.search(*search_args) ##Produces and error
            ##get links crude
            links = bresults['links']
            ##extract links found (remove added %F3 etc)
            ##links1 = [re.findall("http[s]?://[a-zA-Z0-9./-]+", string = x) for x in links]
        
            ##Check links, add new ones
            for link in links:
                if not link in urls_found:
                    urls_found.append(link)

            ##Wait between scraper
            time.sleep(waitTime)
        
        except:
            print("An error occurred while accessing Bing")
            pageNum = 0
            pass
        
        finally:
            if limit == 0 or (limit > 0 and limit > len(urls_found)):
                ##Check if new page needs to be scraped
                if prevLinks <= len(urls_found):
                    prevLinks = len(urls_found)
                    print(str(prevLinks) + " links found B2") 
                    if not pageNum == 0:
                        pageNum += 1
                    ##print(str(len(bresults['titles'])))
                else: 
                    pageNum = 0
            else:
                ##report number of links found
                print(str(len(urls_found)) + " links found B2") 
                if limit > 0:
                    urls_found = urls_found[0:limit]
                pageNum = 0
            
    return(urls_found)

def queryBing2mp(query, country, outputQueue, limit = 0, waitTime = 20):
    ##get results
    links = []
    try:
        links = queryBing2(query, country, waitTime, limit)
    except:
        ##An error occured, make sure output is stored in queue
        pass
    finally:
        outputQueue.put(links)

##function
def checkCountry(url, euDom1):
    vurl = ''
    ##Check link length and if dot is included
    if len(url) > 0 and url.find('.') > 0:
        dom = getDomain(url, False)
        domExt = dom[dom.rindex('.')+1:len(dom)]
        ##Check if included
        if not domExt in euDom1: ##Can you also exclude any unknown domExt if size 2??
            vurl = url        
    return(vurl)
    
def checkCountries(urls, country):
    urls_checked = []
    
    ##Create domain list
    euDom1 = euDom.copy()
    euDom1.remove(country) ##Remove country extension from list of domain extensions
    
    ##Check urls
    for url in urls:
        vurl = checkCountry(url, euDom1)
        if not vurl == '':
            urls_checked.append(vurl)

    return(urls_checked)

##Function to return the combined results of all search engines implemented (per search engine only the unique urls are retruned, combined NOT)
##if singlePage is True only the results on the first page are returned
def queryAllSearchEngines(query, country, limit = 0):
    ##Use default waitTime
    urls_found = []
    
    print("Searching Google 1")
    urls_found = queryGoogle1(query, country, 20, limit)
    
    print("Searching Bing 1")
    urls = queryBing1(query, country, 5, limit)    
    urls_found += urls     
    
    print("Searching DuckDuckGo 1")
    urls = queryDuck1(query, country, 5, limit)    
    urls_found += urls     
    
    print("Searching Yahoo 2")
    urls = queryYahoo2(query, country, 5, limit)    
    urls_found += urls     
    
    print("Searching AOL")
    urls = queryAOL(query, country, 5, limit)    
    urls_found += urls     

    print("Searching Google 3")
    urls = queryGoogle3(query, country, 20, limit)    
    urls_found += urls     
    
    print("Searching Bing 2")
    urls = queryBing2(query, country, 20, limit)    
    urls_found += urls     
    
    print("Searching DuckDuckGo 3")
    urls = queryDuck3(query, country, 5, limit)    
    urls_found += urls     

    ##Remove obvious nn-country links    
    urls_found2 = checkCountries(urls_found, country)
    
    return(urls_found2)
    
##USe local browser to actively open page, save html and soup it
def browsersoup(site):
    soup = 0
    ##Access webbrowser to get html via scripting
    tempFile = "temp.html"
    if os.path.exists(tempFile):
        os.remove(tempFile)
    try:    
        if os.path.exists("save_page_as.sh"):
            message = subprocess.call(["./save_page_as.sh", site, "--browser", "firefox", "--destination", tempFile])
            time.sleep(10)
            ##Check if file is save
            if os.path.exists(tempFile):
                f = open(tempFile, encoding="utf-8")    
                time.sleep(1)
                soup = bs4.BeautifulSoup(f, "lxml")
                f.close()
        else:
            print("save_page_as script does NOT exists")
    except:
        soup = 0
    finally:
        return(soup, site)       

def createsoupAllIn1(site):
    soup = 0
    vurl2 = site
    ##1. get content
    soup, vurl2 = createsoup(site)
    time.sleep(1)
    text = visibletext(soup, True)
    ##Check text and scrape again with chromdriver if empty
    if text == '':
        soup, vurl2 = chromesoup(site)      
        text = visibletext(soup, True)
    ##Check text agian, if not use browser scrape
    if text == '':
        soup, vurl2 = browsersoup(site)
        ##text = df.visibletext(soup, True)

    return(soup, vurl2)
    
def cleanLinks(links, country, checkCount = True):
    links1a = []
    links1b = []
    
    if len(links) > 0:
        ##Remove obvious duplicates
        links = list(set(links))

        ##Clear list of domains of other countries
        if checkCount:
            links = checkCountries(links, country)
    
        ##Do advanced cleaning
        links2 = []
        ##remove anythng after & and check for near similarity (www. missing etc)
        for link in links:
            ##remove part starting at &
            if link.find('&') > 0:
                link = link[0:link.find('&')]
            ##ReFalsemove part after .pdf (may disturb access)
            if link.lower().find('.pdf') > 0:
                link = link[0:(link.lower().find('.pdf')+4)]
            ##replace %2520 with space
            if link.find('%2520') > 0:
                link = link.replace('%2520', ' ')
            ##replace %20 with space
            if link.find('%20') > 0:
                link = link.replace('%20', ' ')
            ##remove www. section (may cause duplicates)
            if link.find('www.') > 0:
                link = link.replace('www.', '')
            ##Check for domain slash duplciates
            if link == getDomain(link) + "/":
                link = getDomain(link)
            ##Add to new cleaned list
            links2.append(link)
    
        ##remove duplicates after preprocessing
        links2 = list(set(links2))
        ##sort links
        links2.sort()
    
        ##Seperate pdfs from the rest
        for link in links2:
            if link.lower().find('.pdf') > 0:
                links1b.append(link) ##all pdfś
            else:
                links1a.append(link) ##all of the rest

    ##Return result        
    return(links1a, links1b)
    
def loadCSVfile(fileName):
    fileContent = ''
    try:
        ##Check if file existst
        if os.path.exists(fileName):
            fileContent = pandas.read_csv(fileName, sep = ";", header=None)
    except:
        ##An error occured
        pass
    finally:
        return(fileContent)

def loadTXTfile(fileName):
    fileContent = ''
    try:
        ##Check if file existst
        if os.path.exists(fileName):
            f = open(fileName, 'r')
            fileContent = f.readlines()
            f.close()            
            ##Remove lagging \(end of lines)
            fileContent = fileContent('\n', '')
    except:
        ##An error occured
        pass
    finally:
        return(fileContent)
