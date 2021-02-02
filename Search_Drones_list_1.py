##Search websites with list of drone company urls (in ireland)
##For the results to reproduce a VPN connection is required (I'm using Luxembourg as a default: Eurostat!)
##Currently only Google search is implemented (more alternatives need to be included)
##Sole focus of this approach is to collect as many urls of drone companies as possible with limited search!!!!

#Load libraries 
##from googlesearch import search
import io
import os
import re
##import ssl
##import urllib.request 
##import bs4
import time
##import timeout_decorator
##from collections import defaultdict
##from googletrans import Translator
##import nltk
import pandas
from random import randint
import requests
##from random import randint
from pdfminer.high_level import extract_text


##Settings  "drone operators ireland list (filetype:pdf OR filetype:doc OR filetype:docx)"
##Max wait time for website to react
waitTime = 60
##Set maximum number of scraped pages per domain
maxDomain = 300
##Set header
header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'}
##get regex for url matching in documents
##regUrl = "http[s]?://[0-9a-zA-Z](-.\w]*[0-9a-zA-Z])*(:(0-9)*)*(\/?([a-zA-Z0-9\-\.\?\,\/"
##regUrl = "http[s]?://[0-9a-zA-z][-.\w]*[/0-9a-zA-Z_.:\-]+[/0-9a-zA-Z_:\-]+"
##regUrl = "[http://|https://]?[0-9a-zA-z][-.\w]+[/0-9a-zA-Z_.:\-]+\.[/0-9a-zA-Z_:\-]{2,5}"
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"

##Functions
##import functions
import Drone_functions as df 


## START ######################################################################

##1. Run search query
print("Searching the web for sites with lists of drone companies")
country = 'ie'
query = df.queryWords(country)
print(query)
drones_list = df.searchGoogle(query, country)

print("Finished Google search")
print(str(len(drones_list)) + " urls found")
print(drones_list)

##1b. Include other search engines here


##2. Brute force scraping of urls detected in websites found via search 
##Start domain urls, external urls (all urls outsite strat domains) and pdf links found are stored
internalIE = drones_list.copy()
externalIE = []
totalPDF = []

##2a. Create dictoionary to enable to count how often a domain is visited
domain_list = []
for i in internalIE:
    ##get Domain
    dom = df.getDomain(i)
    #add domain
    domain_list.append(dom)

##create dictionary
domain_dict = dict()
for i in domain_list:
    ##Start with 0 (for counting how often a domain is included)
    domain_dict[i] = domain_dict.get(i, 0)

##2b. Get internal links of sites in startingdronelist (store external links as well, but do not visit yet)
##Count total number searches performed
count = 0
print("Scraping the links found for new links")
for i in internalIE:    
    ##Start scraping pages (but not pdfs; store them)   
    if i.find('.pdf') > 0:
        if not i in totalPDF:
            ##Add to pdf list
            totalPDF.append(i)
    else:
        ##wait a random time (to distribute burden on domains)
        time.sleep(randint(0,5))
        
        ##get webpage content of url and return actual url visited
        soup, vurl = df.createsoup(i)
    
        ##show actual url visited and scraped
        print(vurl)    
    
        ##After soup, make sure to include new domains (synonyms) of first sets of urls when new (of original drones_list)
        if i != vurl and i in drones_list:
            ##Check if domain of vurl exists in dictionary
            dom = df.getDomain(vurl)
            if not dom in domain_dict:
                ##Add to dictionary (with value 0, 1 OR copy value from i?)
                domain_dict[dom] = domain_dict.get(dom, 0) ##domain_dict[getDomain(i)]
    
        ##Get and process links
        inter = []
        exter = []
        ##Check vurl for pdf
        if vurl.find('.pdf') > 0:
            if not vurl in totalPDF:
                ##Add to pdf list
                totalPDF.append(vurl)
        else:
            ##attempt to scrape page, option to include or exclude mail derived domains (extend potential webpages to visit)
            inter, exter = df.extractLinks(soup, vurl, True) ##lists are empty when nothing is found, page does not exist, page is timed out or an error occurs
    
        ##Internal: Add internal links to internalIE (if not already included)
        if len(inter) > 0:
            ##check eahc new link
            for url in inter:
                ##Check if referred to pdf
                if url.find('.pdf') > 0:
                    if not url in totalPDF:
                        ##Add to pdf list
                        totalPDF.append(url)
                else: 
                    ##Check if already included
                    if not url in internalIE:
                        ##Get domain of url
                        dom = df.getDomain(url)
                        ##Check count (only add when belo domain count)
                        if domain_dict[dom] < maxDomain:
                            ##Add url to end
                            internalIE.append(url)
                            ##Add one to domain count
                            domain_dict[dom] = domain_dict.get(dom, 0) + 1
                        ##else do not add and hence not include
                    
        ##External: Add external links to exteralIE (if not already included)
        if len(exter) > 0:
            for url in exter:
                ##Check if referred to pdf
                if url.find('.pdf') > 0:
                    if not url in totalPDF:
                        ##Add to pdf list
                        totalPDF.append(url)
                else: 
                    ##Check domain (might be a url of a domain already scraped)
                    dom = df.getDomain(url) 
                    ##Check if not included in domain_dict
                    if not dom in domain_dict:
                        ##if external url is NOT included in domains list
                        if not url in externalIE:
                            ##Add to external list
                            externalIE.append(url)
                    else:
                        ##Check if already found (and visited)
                        if not url in internalIE:
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

##2b. Collect pdfs and add links found to external list (if not in internal domains)
print("Collecting url links from pdfs found")
for i in totalPDF:    
    print(i)
    
    ##Check for .pdf in link
    if i.find(".pdf") > 0:
        
        ##i = "http://www.pietdaas.nl/beta/pubs/pubs/CARMA_2020.pdf"
        ##make sure the url is correct
        url = df.getRedirect(i)
        
        ##Check if url exists
        if url != "":
            ###cut out whole link including .pdf(removes garbage) 
            url = url[0:url.rindex(".pdf")+4]        
            print(url)
        
            ##1. get content of online file directly
            textPdf = ""            
            try:
                ##Get connection to url inlcude header and deal with SSL
                response = requests.get(url, headers=header, verify=False)
                ##Checl response code
                if response.status_code == 200:
                    ##All seems ok, extract text
                    textPdf = extract_text(io.BytesIO(response.content)) ##may cause errors (?due to html code?)
                
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
                
            ##Deal with text and \n in pdfs
            textPdf2 = ""
            links = ""
            if len(textPdf) > 0:
                ##Correct \n in text (may split urls in 2 parts)
                textPdf2 = textPdf.replace("\n", "")                    
                ##Find urls
                ##Multiple matches may occur, choose max fittted (this is the first in the fitted list for each item)
                links = [item[0] for item in re.findall(genUrl, textPdf2)]
                ##Only keep unique links in document
                links = list(set(links))
                
                ##remove url domain specific links
                links2 = []
                ##Get domain of url scraped
                dom = df.getDomain2(url, False)
                ##Check if url is NOT in domain of url, is NOT empty and (?is NOT in included in internal domains scraped)
                for link in links:
                    if link.find('.pdf') > 0:  ##Add new pds links to totalPDF list
                        if not link in totalPDF:
                            totalPDF.append(link)
                    elif df.getDomain2(link, False) != dom and link != "":
                        links2.append(link)
            
                ##Contiue with NON-PDF links found
                if len(links2) > 0:
                    ##do ahttp check and add to list of external links
                    for link in links2:
                        if link.startswith("http"):
                            dom = df.getDomain(link)                    
                            if not dom in domain_dict:
                                if not link in externalIE:
                                    externalIE.append(link)
                        else:
                            linkA = df.getRedirect("http://" + link)
                            if linkA != "":
                                dom = df.getDomain(linkA)
                                if not dom in domain_dict:
                                    if not linkA in externalIE:
                                        externalIE.append(linkA)    

    
##3. Store results for search query (three lists as .csv files)
interDF = pandas.DataFrame(internalIE)
interDF.to_csv("internalIE.csv",  index=False)  ##this list is stored to check findings internally
exterDF = pandas.DataFrame(externalIE)        
exterDF.to_csv("externalIE.csv", index=False)    
totalPDFDF = pandas.DataFrame(totalPDF)
totalPDFDF.to_csv('totalPDFIE.csv', index=False) ##Findings of PDF have been included in external list

##Subsequent step:
## 0. Include code to search with other search engines
## 1. Create process to reduce the number of websites that really need to be checked
## 2. Create funcion to detect if an url is a drone website
