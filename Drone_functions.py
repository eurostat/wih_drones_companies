#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 08:37:51 2021

@author: piet
"""
##Drone functions file

#Load libraries 
from googlesearch import search
import re
import ssl
import urllib.request 
import bs4
import time
import timeout_decorator
##from collections import defaultdict
##from googletrans import Translator
import nltk
##import pandas
##from random import randint
import requests

##Surpress specific warning of requests when ssl is needed
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


##Settings  "drone operators ireland list (filetype:pdf OR filetype:doc OR filetype:docx)"
##Max wait time for website to react
waitTime = 60
##Country searched for (either 'ie, nl, es or de)
country = 'ie'
##Set maximum number of scraped pages per domain
maxDomain = 300


##Functions

##querywords per country
def queryWords(country):
    query = ''
    if country == 'ie':
        query = "drone rpas uav uas operators ireland list"
    if country == 'nl':
        query = "drone rpas uav uas bedrijven operators nederland lijst" ##?operator
    if country == 'es':
        query = "drones rpas uav uas operador espania lista"
    if country == 'de':
        query = "drohnen rpas uav uas betrieber unternehmen deutschland liste"
    if country == 'qq':
        query = "drone rpas uav uas bedrijven lijst nederland vergunningen"
    
    return(query)    

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

##google search function INCLUDING FILTER
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

def removeDotDirs(cleanLink):
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
                elif cleanLink.find("www.") > 0 and not (cleanLink.startswith(getDomain(thisurl, False))):
                    if cleanLink.startswith("//"):
                        cleanLink = "http:" + cleanLink
                    elif cleanLink.startwith("/"):
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
                    cleanLink = removeDotDirs(cleanLink)
                    ##exclude links with # in it
                    if not '#' in cleanLink:
                        ##Add link
                        if cleanLink.endswith('.html') or cleanLink.endswith(".htm"):                   
                            listlinksinternal.append(cleanLink)
                        elif cleanLink.startswith('http') and re.match('^https?\:\/\/[a-zA-Z0-9]([a-zA-Z0-9-]+\.){1,}[a-zA-Z0-9]+\Z', cleanLink):
                            listlinksinternal.append(cleanLink)
                        elif not cleanLink.endswith(r'.[a-z]+'): 
                            listlinksinternal.append(cleanLink)
            elif "mailto:" in cleanLink and cleanLink.find('@') > 0:
                ##mailaddress found
                start = cleanLink.find('@') + 1
                mail = cleanLink[start:len(cleanLink)]
                if not mail in listmail:
                    listmail.append(mail)

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
                        ##Include exclusion mail domains here!!
                        mail = "http://" + mail
                        listlinksexternal.append(mail)
                    
        ##Combine lists
        ##linksComb = list(listlinksinternal) + list(listlinksexternal)
        return(listlinksinternal, listlinksexternal)            
    except:
        ##Something whent wrong return empty string
        return ("", "")

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
def visibletext(soup):
    text = ""
    try:
        ''' kill all the scripts and style and return texts '''
        for script in soup(["script", "style"]):
            script.extract()    # rip it out
        text = soup.get_text().lower()
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
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

