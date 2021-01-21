##Emulate congnitive process for ireland case

##Google search
from googlesearch import search
import re
import ssl
import urllib.request 
import bs4
import timeout_decorator
from collections import defaultdict
from googletrans import Translator
import nltk

##query = "drone operators ireland list (filetype:pdf OR filetype:doc OR filetype:docx)"
waitTime = 60
country = 'ie'

##Functions

##querywords per country
def queryWords(country):
    query = ''
    if country == 'ie':
        query = "drone operators ireland list"
    if country == 'nl':
        query = "drone bedrijven nederland lijst ROC" ##?pdf?
    if country == 'es':
        query = "drones operador espania lista"
    if country == 'de':
        query = "drohnen unternehmen deutschland liste"
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
                if cleanLink.startswith('http') and not cleanLink.startswith(thisurl):
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
        linksComb = list(listlinksinternal) + list(listlinksexternal)
        return(linksComb)            
    except:
        ##Something whent wrong return empty string
        return ("")

##scrape function
@timeout_decorator.timeout(waitTime) ## If execution takes longer than 180 sec, TimeOutError is raised
def createsoup(site):
    ''' create a soup based on the url '''
    try: 
        req = urllib.request.Request(site)
        req.add_header('User-agent', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36')
        req.add_header('Accept', 'text/html,application/xhtml+xml,*/*')
        context = ssl._create_unverified_context()
        page = urllib.request.urlopen(req, context=context)
        soup = bs4.BeautifulSoup(page, "lxml")
        return(soup)
    except:
        ## print("not possible to read:", thisurl)
        return 0

##Function to cut out domain name of an url
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

def visibletext(soup):
    ''' kill all the scripts and style and return texts '''
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    text = soup.get_text().lower()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = ' '.join(chunk for chunk in chunks if chunk)
    return(text)

def clearText(text, language, stemming=False):
    ##english, spanish, german, dutch
    wordList = []
    for word in text.lower().split():
        ''' remove weird characters  '''
        word = re.compile('[^a-z]').sub(' ', word)
        if word not in nltk.corpus.stopwords.words(language):                
                for w in word.split():
                    if len(w) > 1:
                        if stemming:
                            wordList.append((nltk.stem.snowball.SnowballStemmer(language).stem(w)).strip())
                        else:
                            wordList.append(w)
    return(wordList)
        
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
    
##run query
country = 'ie'
query = queryWords(country)
print(query)
drones_list = searchGoogle(query, country)

print("Finished Google search")
print(str(len(drones_list)) + " urls found")

##Go through urls found and determine if they provide a list of Drone companies 
for i in drones_list:
    print(i)
    ##get words from url
    wordsU = []
    ##Get part after domain name
    res = i.lower().split("/")[3:]
    for w in res:
        w1 = re.split("-|_", w)
        for w2 in w1:
            w2 = re.compile('[^a-z]').sub(' ', w2)        
            w2 = w2.strip()
            if len(w2) > 0:
                wordsU.append(w2)
    wordsUrl = ' '.join(wordsU)    
    ##Scrape content
    soup = createsoup(i)
    ##Get text
    text = visibletext(soup)
    ##Get title
    title = getTitle(soup)
    ##print("title: " + title) 
    keywords = getKeywords(soup)
    ##print("Keywords: " + keywords) 
    
    ##Combne words
    words = wordsUrl + " " + title + " " + keywords
    words = words.strip()
    print(words)
    
    ##Fingerprint text
    print("services: "+ str(text.count("services")))
    print("inspection: "+ str(text.count("inspection")))
    print("contact: "+ str(text.count("contact")))
    print("address: "+ str(text.count("address")))
    print("email: "+ str(text.count("email")))
    
    
    ##textList = clearText(text, "english", True)
    ##get wordFreq
    ##wordFreq = [textList.count(p) for p in textList]
    ##wordDic = dict(list(zip(textList,wordFreq)))
    ##Sort wordDic
    ##And sort result on value (most occuring first)
    ##wordDic = sorted(wordDic.items(), key=lambda k_v: k_v[1], reverse=True) 
    ##print(str(wordDic[0]) + ', ' + str(wordDic[1]) + ', ' + str(wordDic[2]))
       