#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
"""
##Search list of websites of drone company urls (in ireland)

#Load libraries 
import os
import time
import pandas
from random import randint
##from collections import Counter
import multiprocessing as mp

##Set directory
os.chdir("/home/piet/R/Drone")
##Max wait time for website to react
waitTime = 60
##Set maximum number of scraped pages per domain
maxDomain = 300 ##Perhaps 200 is better?
##Include mail as links
mailInclude = True

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##import functions
import Drone_functions as df 
##get cores of machine used
cores = mp.cpu_count()

## START ######################################################################

##1. Init search query
print("Searching the web for sites with lists of drone companies")
##Set country
country = 'ie'
##Set singlepage s True
limit = 50

##Get query and language
query, lang = df.getQuery(country, getListSites = True)

query = '(drone OR rpas OR uav OR uas) (members OR register OR list) ireland'

links = []
##1a. Check for multicore run
if cores >= 4:
    ##Use different search engines simultaniously
    print("Parallel search engine option is used (max 4 different)")
    ##init output queue
    out_q = mp.Queue()

    ##Init 4 simultanious queries, store output in queue
    p1 = mp.Process(target = df.queryGoogle1mp, args = (query, country, out_q, limit))
    p1.start()    
    p2 = mp.Process(target = df.queryBing1mp, args = (query, country, out_q, limit))
    p2.start()
    p3 = mp.Process(target = df.queryDuck1mp, args = (query, country, out_q, limit)) ##Make sure it runs (deal with driver issues)
    p3.start()
    p4 = mp.Process(target = df.queryYahoo2mp, args = (query, country, out_q, limit))
    p4.start()

    time.sleep(4)
    
    ##Wait till finished
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    
    ##Add to result
    result = []
    for i in range(4):
        result.append(out_q.get())

    ##When finished perform 4 more queries, store output in queue
    p1 = mp.Process(target = df.queryAOLmp, args = (query, country, out_q, limit))
    p1.start()
    p2 = mp.Process(target = df.queryGoogle3mp, args = (query, country, out_q, limit))
    p2.start()    
    p3 = mp.Process(target = df.queryBing2mp, args = (query, country, out_q, limit)) ##Error in Bing2?
    p3.start()
    p4 = mp.Process(target = df.queryDuck3mp, args = (query, country, out_q, limit))
    p4.start()
    
    time.sleep(4)

    links = []
    for res in result:
        links += res
        
else:
    ##use sequential search
    print("Searching Google 1")
    links = df.queryGoogle1(query, country, limit, 20)
    
    print("Searching Bing 1")
    urls = df.queryBing1(query, country, limit, 5)    
    links += urls     
    
    print("Searching DuckDuckGo 1")
    urls = df.queryDuck1(query, country, limit, 5)    
    links += urls     
    
    print("Searching Yahoo 2")
    urls = df.queryYahoo2(query, country, limit, 5)    
    links += urls     
    
    print("Searching AOL")
    urls = df.queryAOL(query, country, limit, 5)    
    links += urls     

    print("Searching Google 3")
    urls = df.queryGoogle3(query, country, limit, 20)    
    links += urls     
    
    print("Searching Bing 2")
    urls = df.queryBing2(query, country, limit, 20)    
    links += urls     
    
    print("Searching DuckDuckGo 3")
    urls = df.queryDuck3(query, country, limit, 5)    
    links += urls     

##1b. Cleanlinks, but do not strickly check country codes (yet)
links1a, links1b = df.cleanLinks(links, country, checkCount = False)

##1b. Check if the website is about ireland and drones (does this word occur in text on page?)
drone_words = ["drone", "rpas", "uav", "uas", "unmanned", "aerial"]
mem_words = ["member", "register", "registration", "list", "overview"]
links2a = []
for url in links1a:
    print(url)
    ##Check url fro inclusion of relevant terms
    if any(x in url.lower() for x in ['.ie', 'ireland', 'irish']):
        ##Check if words occur in url
        if any(x in url.lower() for x in drone_words) and any(x in url.lower() for x in mem_words):
            ##Only add when not already included (either as is or as lowercase)
            if not url in links2a and not url.lower() in links2a:
                links2a.append(url)

    ##Check content for relevant urls (should be .ie or com etc, not form other countries)
    if not url in links2a:
        ##Check country extension first, use list
        urls = df.checkCountries([url], country)
        if not urls == []:
            ##scrape page to get soup
            soup, vurl = df.createsoup(urls[0])
            ##get text as lowercase
            text = df.visibletext(soup, True)
            if not text == '':
                ##Check for ireland
                if any(x in text for x in ['ireland', ' eire ', 'irish']):
                    ##Check for other words in text
                    if any(x in text for x in drone_words) and any(x in text for x in mem_words):
                        links2a.append(url)

##Do the same for pdfs (are they aboit ireland, drones and members)
links2b = []
for url in links1b:
    ##scrape pdf, get text and make sure its lowercase
    text = df.getPdfText(url, True)
    ##Check for ireland
    if any(x in text for x in ['ireland', ' eire ', 'irish']):
        ##Check for other words in text
        if any(x in text for x in drone_words) and any(x in text for x in mem_words):
            links2b.append(url)

##Show end result of cleaning
print(str(len(links2a) + len(links2b)) + " unique links found")

##2. Brute force scraping of urls detected in websites found via search (But not the pdfs yet) after filtering
##Start domain urls, external urls (all urls outsite strat domains) and pdf links found are stored
internalL = links2a.copy()
internalL.sort()
externalL = []
##Keep pdfs found
if len(links2b) > 0:
    totalPDF = links2b.copy()
else:
    totalPDF = []

##2a. Create dictoionary to enable to count how often each domain is visited
domain_list = []
for url in internalL:
    ##get Domain
    dom = df.getDomain(url)
    ##Check if already included
    if not dom in domain_list:
        #add domain
        domain_list.append(dom)

##2b distribut urls over cores
    
##define function to find links on webpages list
def scrapeLinks(interLocal, domain_list):
    ##initiate store lists
    totalPDF = []
    externalL = []
    maxDom = 300
    
    ##create dictionary
    domain_dict = dict()
    for dom in domain_list:
        ##Start with 0 (for counting how often a domain is included)
        domain_dict[dom] = domain_dict.get(dom, 0)

    ##2b. Get internal links of sites in startingdronelist (store external links as well, but do not visit yet)
    ##Count total number searches performed
    count = 0
    print("Scraping the links found for drone website links") ##Need to exclude pictures from the links
    for url in interLocal:    
        ##Start scraping pages (but not pdfs; store them), deal with link in links?
        if url.lower().find('.pdf') > 0:
            if not url in totalPDF:
                ##Add to pdf list
                totalPDF.append(url)
        elif url.lower().find('.jpg') > 0 or url.lower().find('.jpeg') > 0 or url.lower().find('.png') > 0 or url.lower().find('.gif') > 0: ##Ignore links to pictures
            ##Ignore link
            pass
        else:
            ##wait a random time (to distribute burden on domains)
            time.sleep(randint(0,5))
        
            ##get webpage content of url and return actual url visited
            soup, vurl = df.createsoup(url)
    
            ##show actual url visited and scraped
            print(vurl)    
    
            ##After soup, make sure to include new domains (synonyms) of first sets of urls when new (of original drones_list)
            if not url == vurl and url in interLocal:
                ##Check if domain of vurl exists in dictionary
                dom = df.getDomain(vurl)
                if not dom in domain_dict:
                    ##Add to dictionary (with value 0, 1 OR copy value from i?)
                    domain_dict[dom] = domain_dict.get(dom, 0) ##domain_dict[getDomain(i)]
            ##else:
            ##    print(vurl + " new link which is not a synonym of original url set")
            
            ##Get and process links
            inter = []
            exter = []
            ##Check vurl for pdf
            if vurl.lower().find('.pdf') > 0:
                if not vurl in totalPDF:
                    ##Add to pdf list
                    totalPDF.append(vurl)
            elif vurl.lower().find('.jpg') > 0 or vurl.lower().find('.jpeg') > 0 or vurl.lower().find('.png') > 0 or vurl.lower().find('.gif') > 0: ##Ignore links to pictures
                ##Ignore link
                pass
            else:
                ##attempt to scrape page, option to include or exclude mail derived domains (extend potential webpages to visit)
                inter, exter = df.extractLinks(soup, vurl, mailInclude) ##lists are empty when nothing is found, page does not exist, page is timed out or an error occurs
    
            ##Internal: Add internal links to internalIE (if not already included)
            if len(inter) > 0:
                ##check eahc new link
                for url in inter:
                    ##Check if referred to pdf
                    if url.lower().find('.pdf') > 0:
                        if not url in totalPDF:
                            ##Add to pdf list
                            totalPDF.append(url)
                    elif url.lower().find('.jpg') > 0 or url.lower().find('.jpeg') > 0 or url.lower().find('.png') > 0 or url.lower().find('.gif') > 0: ##Ignore links to pictures
                        ##Ignore link
                        pass
                    else: 
                        ##Check if already included
                        if not url in interLocal:
                            ##Get domain of url
                            dom = df.getDomain(url)
                            ##Check if domain is known
                            if dom in domain_dict:
                                ##Check count (only add when belo domain count)
                                if domain_dict[dom] < maxDom: ##Error dom NOT in domain_dict
                                    ##Add url to end
                                    interLocal.append(url)
                                    ##Add one to domain count
                                    domain_dict[dom] = domain_dict.get(dom, 0) + 1
                            else:
                                if not url in externalL:
                                    externalL.append(url)
                                    ##print("Should not occur, so how to deal with these?")
                                    ##do not add and hence not include
                    
            ##External: Add external links to exteralIE (if not already included)
            if len(exter) > 0:
                for url in exter:
                    ##Check if referred to pdf
                    if url.lower().find('.pdf') > 0:
                        if not url in totalPDF:
                            ##Add to pdf list
                            totalPDF.append(url)
                    elif url.lower().find('.jpg') > 0 or url.lower().find('.jpeg') > 0 or url.lower().find('.png') > 0 or url.lower().find('.gif') > 0: ##Ignore links to pictures
                        ##Ignore link
                        pass
                    else: 
                        ##Check domain (might be a url of a domain already scraped)
                        dom = df.getDomain(url) 
                        ##Check if not included in domain_dict
                        if not dom in domain_dict:
                            ##if external url is NOT included in domains list
                            if not url in externalL:
                                ##Add to external list
                                externalL.append(url)
                        else:
                            ##Check if already found (and visited)
                            if not url in interLocal:
                                ##Check count (only add when belo domain count)
                                if domain_dict[dom] < maxDom:
                                    ##Add url to end
                                    interLocal.append(url)
                                    ##Add one to domain count
                                    domain_dict[dom] = domain_dict.get(dom, 0) + 1                                

        ##Add one to total webiste count
        count += 1

        ##Show progress
        print(count) ##Sow length internalL?
    
    ##Reached the end
    return(externalL, totalPDF)
    ##print("Finshed")
    
##Multicore version of scrapeLinks
def scrapeLinksmp(interLocal, domain_list, outputQueue):
    ##start script
    external, pdfs = scrapeLinks(interLocal, domain_list)
    total = external + pdfs
    outputQueue.put(total)

##Test with limited set of urls!!!!
##Scrape listst
if cores >= 4: 
    ##Use different search engines simultaniously
    print("Parallel search scrape option is used (max 4 different)")
    
    ##Create 4 seperate internalL listst
    internalL.sort()
    inter1 = []; inter2 = []; inter3 = []; inter4 = []
    interL = [inter1, inter2, inter3, inter4]
    prevDom = ''
    current = 0
    for url in internalL:
        ##Get domain no https
        dom = df.getDomain(url, False)
        ##Check result
        if prevDom == '':
            ##Add to first list
            interL[current].append(url)
        elif dom == prevDom:
            ##Add to current list
            interL[current].append(url)
        else:
            ##Swithc list
            current += 1
            if current > 3:
                current = 0
            ##Add to new list
            interL[current].append(url)
        ##Remember dom of url
        prevDom = dom

    ##init output queue
    out_q2 = mp.Queue()

    ##Init 4 simultanious queries, store output in queue
    p1 = mp.Process(target = scrapeLinksmp, args = (interL[0], domain_list, out_q2))
    p1.start()    
    p2 = mp.Process(target = scrapeLinksmp, args = (interL[1], domain_list, out_q2))
    p2.start()
    p3 = mp.Process(target = scrapeLinksmp, args = (interL[2], domain_list, out_q2))
    p3.start()
    p4 = mp.Process(target = scrapeLinksmp, args = (interL[3], domain_list, out_q2))
    p4.start()

    time.sleep(4)
    
    ##Wait till finished
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    
    ##Add to result (contains external links and pdfs)
    result = []
    for i in range(4):
        result.append(out_q2.get())

    ##result contains 4 lists
    result2 = []
    for res in result:
        for r in res:
            if not r in result2:
                result2.append(r)
    
    ##3. unique links, split pdf from the rest
    result2 = list(set(result2))
    totalPDF = []
    externalL = []
    for url in result2:    
        if url.lower().find('.pdf') > 0:
            totalPDF.append(url)
        else:
            externalL.append(url)
else:
    ##Single core approach
    print("Sequential search scrape option is used")
    extrenalL, totalPDF = scrapeLinks(internalL, domain_list)
    
##HIER##
##3. Store internal links found in a file
##interDF = pandas.DataFrame(internalL)
##fileNameI = "internal_" + country.upper() + ".csv"
##interDF.to_csv(fileNameI,  index=False)  ##this list is stored to check findings internally

##4. Collect pdfs and add links found to external list (if not in internal domains)
##4a. Combine pdfs found in step 1 with pdfs found in step 2 and clean links
Filename1 = "Searchsites_" + country.upper() + "_1_pdf.txt"
if os.path.exists(Filename1):
    ##Get content
    f = open(Filename1, 'r')
    pdf1 = f.readlines()
    f.close()
    ##Check and add content
    if len(pdf1) > 0:
        ##Add to totalPDF
        for i in pdf1:
            i = i.replace('\n', '').strip()
            if not i in totalPDF:
                totalPDF.append(i)

##4b. Clean pdf links (do not check country links), also removes andy non .pdf files included 
linksNot, linksPDF = df.cleanLinks(totalPDF, country, False)

##4c. Extract urls from pdf files found
print("Collecting url links from pdfs found") ##CHeck content for mentioning of Ireland?

##create dictionary from domain_list
domain_dict = dict()
for dom in domain_list:
   ##Start with 0 (for counting how often a domain is included)
   domain_dict[dom] = domain_dict.get(dom, 0)

for i in linksPDF:    
    ##print(i)    
    ##Check for .pdf in link
    if i.lower().find(".pdf") > 0:
        
        ##i = "http://www.pietdaas.nl/beta/pubs/pubs/CARMA_2020.pdf"
        ##make sure the url is correct
        url = df.getRedirect(i)
        
        ##Check if url exists and still refers to pdf
        if not url == "" and url.lower().find('.pdf') > 0:
            ###cut out whole link including .pdf (removes any garbage) 
            url = url[0:url.lower().rindex(".pdf")+4]        
            print(url)
        
            ##1. get content of online pdf file, get text as lower case
            textPdf = df.getPdfText(url, True)            
                
            ##Deal with text and \n in pdfs CHECKS TEXT IF ITS ABOUT DRONES and Ireland!!!!
            textPdf2 = ""
            links = ""
            if len(textPdf) > 0:
                ##Correct \n in text (may split urls in 2 parts)
                textPdf2 = textPdf.replace("\n", "")                    
                
                ##Check if the file is about Drones and Ireland
                if any(x in textPdf2 for x in ['ireland', ' eire ', 'irish']) and any(x in textPdf2 for x in drone_words): ## and any(x in text for x in mem_words):

                    ##Find urls
                    links = df.extractLinksText(textPdf2, genUrl)
                
                    ##Check if mail domains need to be included
                    if mailInclude:
                        ##get mail domains
                        links1 = df.extractLinksText(textPdf2, genMail)
                        ##Combine results
                        links = links + links1                   
                
                    ##remove any duplicates
                    links = list(set(links))                
                    #remove url domain specific links
                    links2 = []
                    ##Get domain of url scraped
                    dom = df.getDomain2(url, False)
                    ##Check if url is NOT in domain of url, is NOT empty and (?is NOT in included in internal domains scraped)
                    for link in links:
                        if link.lower().find('.pdf') > 0:  ##Add new pds links to totalPDF list
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
                                    if not link in externalL:
                                        externalL.append(link)
                            else:
                                ##no staring part, add it and check
                                linkA = df.getRedirect("http://" + link)
                                if linkA != "":
                                    dom = df.getDomain(linkA)
                                    if not dom in domain_dict:
                                        if not linkA in externalL:
                                            externalL.append(linkA)    


##5. Clear and save results of external Links
##5a. Remove double and obvious links to other countries 
externalL2, externalLNot = df.cleanLinks(externalL, country, True)

print("Finished extensive urls search")
print(str(len(externalL2)) + " unique external links found")

##Add already collected links in step 1
##4a. Combine pdfs found in step 1 with pdfs found in step 2 and clean links
Filename1 = "Searchsites_" + country.upper() + "_1.txt"
if os.path.exists(Filename1):
    ##Get content
    f = open(Filename1, 'r')
    links1 = f.readlines()
    f.close()
    ##Check and add content
    if len(links1) > 0:
        ##Add to totalPDF
        for i in links1:
            i = i.replace('\n', '').strip()
            if not i in externalL2:
                externalL2.append(i)


##5b. Save combined findings as csv's
fileNameE = "external_" + country.upper() + "_2.csv"
exterDF = pandas.DataFrame(externalL2)        
exterDF.to_csv(fileNameE, index=False)    
fileNameP = "totalPDF_"+ country.upper() + "_2.csv"
totalPDFDF = pandas.DataFrame(totalPDF)
totalPDFDF.to_csv(fileNameP, index=False) 

