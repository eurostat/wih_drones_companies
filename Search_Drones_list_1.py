##Search websites with list of drone company urls (in ireland)
##For the results to reproduce a VPN connection is required (I'm using Luxembourg as a default: Eurostat!)
##Currently only Google search is implemented (more alternatives need to be included)
##Sole focus of this approach is to collect as many urls of drone companies as possible with limited search!!!!

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
import pandas
from random import randint


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
def extractLinks(soup, thisurl):  
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
                        
        ##make sure only unique urls are returned by using set
        if len(listlinksinternal) > 0:
            listlinksinternal = set(listlinksinternal)                  
        if len(listlinksexternal) > 0:
            listlinksexternal = set(listlinksexternal)
        ##Combine lists
        ##linksComb = list(listlinksinternal) + list(listlinksexternal)
        return(list(listlinksinternal), list(listlinksexternal))            
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
        ##remove any ? containing part (may be added to url)
        url = url.split('?')[0]
        ##Get domain name
        top = url.split('/')[2]
        ##add prefix
        if prefix:
            top = url.split('/')[0] + '//' + top
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
 
###############################################################################    
##1. Run search query
query = queryWords(country)
print(query)
drones_list = searchGoogle(query, country)

print("Finished Google search")
print(str(len(drones_list)) + " urls found")
print(drones_list)

##2. Brute force scraping of urls detected in websites found via search 
##Start domain urls, external urls (all urls outsite strat domains) and pdf links found are stored
internalIE = drones_list.copy()
externalIE = []
totalPDF = []

##Create dictoionary to enable to count how often a domain is visited
domain_list = []
for i in internalIE:
    ##get Domain
    dom = getDomain(i)
    #add domain
    domain_list.append(dom)

##create dictionary
domain_dict = dict()
for i in domain_list:
    ##Start with 0 (for counting how often a domain is included)
    domain_dict[i] = domain_dict.get(i, 0)

##Count total number searches performed
count = 0
##Get internal links of sites in startingdronelist (store external links as well, but do not visit yet)
for i in internalIE:
    
    ##Start scraping pages (but not pdfs; store them)   
    if i.find('.pdf') > 0:
        if i not in totalPDF:
            ##Add to pdf list
            totalPDF.append(i)
    else:
        ##wait a random time (to distribute burden on domains)
        time.sleep(randint(0,5))
        
        ##get webpage content of url and return actual url visited
        soup, vurl = createsoup(i)
    
        ##show actual url visited and scraped
        print(vurl)    
    
        ##After soup, make sure to include new domains (synonyms) of first sets of urls when new (of original drones_list)
        if i != vurl and i in drones_list:
            ##Check if domain of vurl exists in dictionary
            dom = getDomain(vurl)
            if not dom in domain_dict:
                ##Add to dictionary (with value 0, 1 OR copy value from i?)
                domain_dict[dom] = domain_dict.get(dom, 0) ##domain_dict[getDomain(i)]
    
        ##Get and process links
        inter = []
        exter = []
        ##Check vurl for pdf
        if vurl.find('.pdf') > 0:
            if vurl not in totalPDF:
                ##Add to pdf list
                totalPDF.append(vurl)
        else:
            ##attempt to scrape page
            inter, exter = extractLinks(soup, vurl) ##lists are empty when nothing is found, page does not exist, page is timed out or an error occurs
    
        ##Add internal links to internalIE (if not already included)
        if len(inter) > 0:
            ##check eahc new link
            for url in inter:
                ##Check if referred to pdf
                if url.find('.pdf') > 0:
                    if url not in totalPDF:
                        ##Add to pdf list
                        totalPDF.append(url)
                else: 
                    ##Check if already included
                    if url not in internalIE:
                        ##Get domain of url
                        dom = getDomain(url)
                        ##Check count (only add when belo domain count)
                        if domain_dict[dom] < maxDomain:
                            ##Add url to end
                            internalIE.append(url)
                            ##Add one to domain count
                            domain_dict[dom] = domain_dict.get(dom, 0) + 1
                        ##else do not add and hence not include
                    
        ##Add external links to exteralIE (if not already included)
        if len(exter) > 0:
            for url in exter:
                ##Check if referred to pdf
                if url.find('.pdf') > 0:
                    if url not in totalPDF:
                        ##Add to pdf list
                        totalPDF.append(url)
                else: 
                    ##Check domain (might be a url of a domain already scraped)
                    dom = getDomain(url) 
                    ##Check if not included in domain_dict
                    if dom not in domain_dict:
                        ##if external url is NOT included in domains list
                        if url not in externalIE:
                            ##Add to external list
                            externalIE.append(url)
                    else:
                        ##Check if already found (and visited)
                        if url not in internalIE:
                            ##Check count (only add when belo domain count)
                            if domain_dict[dom] < maxDomain:
                                ##Add url to end
                                internalIE.append(url)
                                ##Add one to domain count
                                domain_dict[dom] = domain_dict.get(dom, 0) + 1
                        
    ##Add one to total webiste count
    count += 1

    ##Show progress
    print(count)
    
##Store results (three lists as .csv files)
interDF = pandas.DataFrame(internalIE)
interDF.to_csv("internalIE.csv",  index=False)
exterDF = pandas.DataFrame(externalIE)        
exterDF.to_csv("externalIE.csv", index=False)    
totalPDFDF = pandas.DataFrame(totalPDF)
totalPDFDF.to_csv('totalPDFIE.csv', index=False)

##Subsequent steps:
## 1. Create funcion to detect if an url is a drone website
## 2. Create ufnction to extract links from PDF's
