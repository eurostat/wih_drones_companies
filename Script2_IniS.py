#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
with multiple search engines including Ask
@author: piet
Updated on Aug 3 2021, version 2.05
"""
##Search list of websites of drone company urls (in ireland)

#Load libraries 
import os
import sys
import time
import pandas
import random
import multiprocessing as mp
import numpy as np
import configparser

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Test_IE")

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##Factor to multiply the number of cores with for parallel scraping (not for search engine use)
multiTimes = 1 ##number to multiply number of parallel scraping sessions

##Define function
def SearchLists(query, country, limit):
    links = []
    ##Check for multicore run
    if cores >= 4:
        ##Use different search engines simultaniously
        ##print("Parallel search engine option is used (max 6 different)")
        ##init output queue
        out_q = mp.Queue()

        ##Init 4 simultanious queries, store output in queue
        if payedGoogle:
            p1 = mp.Process(target = pg.queryGoogleVmp, args = (query, country, out_q, limit))
        else:
            p1 = mp.Process(target = df.queryGoogle1mp, args = (query, country, out_q, limit))
        p1.start()    
        p2 = mp.Process(target = df.queryBing1mp, args = (query, country, out_q, limit))
        p2.start()
        p3 = mp.Process(target = df.queryDuck1mp, args = (query, country, out_q, limit)) ##Make sure it runs (deal with driver issues)
        p3.start()
        p4 = mp.Process(target = df.queryAOL2mp, args = (query, country, out_q, limit))
        p4.start()
        
        time.sleep(6)
    
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
        p5 = mp.Process(target = df.queryYahoo3mp, args = (query, country, out_q, limit))
        p5.start()
        p6 = mp.Process(target = df.queryAsk1mp, args = (query, country, out_q, limit))
        p6.start()

        if not payedGoogle:
            p7 = mp.Process(target = df.queryGoogle3mp, args = (query, country, out_q, limit))
            p7.start()    
    
        time.sleep(4)

        ##Wait till finished
        p5.join()
        p6.join()
        if not payedGoogle:
            p7.join()
    
        ##Add to result
        if payedGoogle:
            for i in range(2):
                result.append(out_q.get())
        else:
            for i in range(3):
                result.append(out_q.get())
            
        ##Combine all listst
        if payedGoogle:
            SEnames = ["Google", "Bing", "DuckDuckGo", "AOL", "Yahoo", "Ask"]
        else:
            SEnames = ["Google1", "Bing", "DuckDuckGo", "AOL", "Yahoo", "Ask", "Google3"]
   
        links = []
        count = 0
        for res in result:            
            f.write("Search engine " + str(SEnames[count]) +  " found " + str(len(res)) +  " urls\n")            
            links += res
            count += 1
        
    else:
        ##use sequential search
        print("Searching Google")
        if payedGoogle:
            links = pg.queryGoogleV(query, country, 0, limit)
            print("Google payed found " + str(len(links)) + " urls")
            f.write("Google payed found " + str(len(links)) + " urls\n")
        else:
            links = df.queryGoogle1(query, country, 20, limit)
            print("Google 1 found " + str(len(links)) + " urls")
            f.write("Google 1 found " + str(len(links)) + " urls\n")
    
        print("Searching Bing 1")
        urls = df.queryBing1(query, country, 5, limit)    
        print("Bing 1 found " + str(len(urls)) + " urls")
        f.write("Bing 1 found " + str(len(urls)) + " urls\n")
        links += urls     
    
        print("Searching DuckDuckGo 1")
        urls = df.queryDuck1(query, country, 20, limit)   
        print("DuckDuckGo 1 found " + str(len(urls)) + " urls")
        f.write("DuckDuckGo 1 found " + str(len(urls)) + " urls\n")
        links += urls     
    
        print("Searching Yahoo 3")
        urls = df.queryYahoo3(query, country, 20, limit)
        print("Yahoo 3 found " + str(len(urls)) + " urls")
        f.write("Yahoo 3 found " + str(len(urls)) + " urls\n")
        links += urls     
    
        print("Searching AOL 2")
        urls = df.queryAOL2(query, country, 20, limit)    
        print("AOL 2 found " + str(len(urls)) + " urls")
        f.write("AOL 2 found " + str(len(urls)) + " urls\n")
        links += urls     
         
        print("Searching Ask 1")
        urls = df.queryAsk1(query, country, 20, limit)    
        print("Ask 1 found " + str(len(urls)) + " urls")
        f.write("Ask 1 found " + str(len(urls)) + " urls\n")
        links += urls     

        if not payedGoogle:
            print("Searching Google 3")
            urls = df.queryGoogle3(query, country, 20, limit) 
            print("Google 3 found " + str(len(urls)) + " urls")
            f.write("Google 3 found " + str(len(urls)) + " urls\n")
            links += urls     

    return(links)

##function to check content of sites, see if they are drone aggregate sites (count drone acronyms)    
def CheckSites(links):
    ##Store vurl (so the actual link is used in future)
    links2a = [] ##Website links of aggregate drone sites
    links2b = [] ##Website that could potentially be a drone website
    for url in links:
        print(url)
        ##Check if .ie or ireland/irish is included in url
        if any(x in url.lower() for x in countryW1):
            ##Check country extension first, use list
            urls = df.checkCountries([url], country)
            if not urls == []:
                ##scrape page to get soup
                soup, vurl = df.createsoup(urls[0]) ##error prone?
                ##get text as lowercase
                text = df.visibletext(soup, True)
                if not text == '':
                    ##Check for country drones and members
                    if any(x in text for x in countryW2) and any(x in text for x in drone_words) and any(x in text for x in mem_words):
                        res = sum([text.count(x) for x in drone_words])      
                        print(res)
                        if res > 5: ##thereshold to include links (should contain at least 6 mentions of drone synonyms)
                            if not vurl in links2a:  ##use vurl here so actual link wil be processed
                                links2a.append(vurl) 
                        elif res > 0:
                            if not vurl in links2b:
                                links2b.append(vurl)
                        ##keep links with res between 1 and 5?
    ##return llist
    return(links2a, links2b)

##define function to find links on webpages list
def ExtractSites(interLocal):
    ##initiate store lists
    totalPDF = []
    externalL = []
    maxDom = maxDomain
    
    ##Create dictionary (is used later)
    alias_dict = g_alias_dict.copy()
    domain_dict = dict()
    for dom in g_domain_list:
        ##Get alias
        alia = alias_dict[dom]
        ##Start with 0 (for counting how often a domain is included)
        domain_dict[alia] = domain_dict.get(alia, 0)

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
            ##ignore social media??
            
            ##wait a random time (to distribute burden on domains)
            time.sleep(random.randint(0,5))
        
            ##get webpage content of url and return actual url visited
            soup, vurl = df.createsoup(url)
    
            ##show actual url visited and scraped
            print(vurl)    
    
            ##After soup, make sure to include new domains (synonyms) of first sets of urls when new (of original drones_list)
            if not url == vurl:
                ##Check if domain of vurl is new (does exists in alias dictionary)
                dom = df.getDomain3(vurl)
                if not dom in alias_dict:
                    ##Add vurl dom to alias_of url in alias_dict
                    dom1 = df.getDomain3(url)
                    alia = alias_dict[dom1]
                    alias_dict[dom] = alias_dict.get(dom, alia) ##This assures 'new' vurl domains are cointed as their url parent
            
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
                            dom = df.getDomain3(url)
                            ##Check if domain is known
                            if not dom in alias_dict:
                                ##Add dom of original vurl to alias_dict
                                dom1 = df.getDomain3(vurl)
                                alia = alias_dict[dom1]
                                alias_dict[dom] = alias_dict.get(dom, alia) ##This assures 'new' vurl domains are cointed as their url parent

                            ##dom will always occur in alias_dict, Add to count
                            alia = alias_dict[dom]
                            ##Check count (only add when belo domain count)
                            if domain_dict[alia] < maxDom: ##Error dom NOT in domain_dict
                                ##Add url to end
                                interLocal.append(url) ##Expands links to be scraped
                                ##Add one to domain count
                                domain_dict[alia] = domain_dict.get(alia, 0) + 1
                            ##else do nothing, do not add!!
                    
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
                        dom = df.getDomain3(url) 
                        ##Check if not included in domain_dict
                        if not dom in alias_dict:
                            ##if external url is NOT included in domains list
                            if not url in externalL:
                                ##Add to external list
                                externalL.append(url)
                        ##esle do nothing, do not add
                            
        ##Add one to total webiste count
        count += 1

        ##Show progress
        print(count) ##Sow length internalL?
    
    ##Reached the end
    print("Finshed")

    ##totalLinks = externalL + totalPDF
    return(externalL, totalPDF)


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
    print("Use 'python3 Script2_Ini.py <filename.ini>' to run program") 


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
        queries = config.get('SETTINGS2', 'queries').split(',')
        googleKey = config.get('MAIN', 'googleKey')
        countryW1 = config.get('SETTINGS2', 'countryW1').split(',')
        countryW2 = config.get('SETTINGS2', 'countryW2').split(',')
        drone_words = config.get('SETTINGS2', 'drone_words').split(',')
        mem_words = config.get('SETTINGS2', 'mem_words').split(',')
        payedGoogle = config.getboolean('SETTINGS2', 'payedGoogle')
        runParallel = config.getboolean('SETTINGS2', 'runParallel')
        limit = config.getint('SETTINGS2', 'limit')
        maxDomain = config.getint('SETTINGS2', 'maxDomain')
        mailInclude = config.getboolean('SETTINGS2', 'mailInclude')
        
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(queries) > 0 and len(str(countryW1)) > 0 and len(str(countryW2)) > 0 and len(str(drone_words)) > 0 and len(str(mem_words)) > 0 and len(str(payedGoogle)) > 0 and len(str(runParallel)) > 0 and len(str(limit)) > 0 and len(str(maxDomain)) > 0 and len(str(mailInclude)) > 0:
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


##C. Check contune and start scraping
if Continue:
    ##1. import functions
    import Drone_functions as df 
    import payedGoogle as pg
    ##set payed google key
    pg._api_key_ = googleKey  

    ##get cores of machine used
    cores = mp.cpu_count()

    ##Check for parallel run
    if not runParallel:
        cores = 1

    ##Create logfile
    logFile = "2_results_" + country.upper() + lang.lower() + "1.txt"
    f = open(logFile, 'w')

    ##show what will be done
    print("Searching the web for sites with lists of drone companies")
    f.write("Searching the web for sites with lists of drone companies\n")

    ##Save prallel status
    if runParallel:
        print("Parallel search engine option is used (max 6 different)")        
        f.write("Parallel run with " + str(cores) + " cores\n")
    else:
        print("Sequential search engine option is used")
        f.write("Serial run (single core)\n")


    ##2. Get links for all queries
    links = []
    for query in queries:
        print("Running query: " + str(query))
        f.write("Query: " + query + "\n")
        urls = SearchLists(query, country, limit)
        links += urls

    ##show end result
    print("A total number of " + str(len(links)) + " found")
    f.write("A total number of " + str(len(links)) + " found\n")

    ##3. Process results, remove as much irrelevant links as possible
    ##3a remove duplicates, split off pdfs and the rest (be strict)
    links1a, links1b = df.cleanLinks(links, country, True)

    ##show findings
    print(str(len(links1a)) + " unique web links found")
    f.write(str(len(links1a)) + " unique web links found\n")
    print(str(len(links1b)) + " unique pdf links found")
    f.write(str(len(links1b)) + " unique pdf links found\n")


    ##3b. Check if the website link is truely about country, drones and members/registration or something like that (does this word occur in text on page?)
    if cores >= 4:
        ##Multicore scraping
        ##First, randomize links list
        random.shuffle(links1a)
        ##Create chunks list equal to number of cores available
        chunks = np.array_split(links1a, cores*multiTimes, axis = 0)
    
        ##Scrape listst    ##Use all cores to process chunks
        pool = mp.Pool(cores*multiTimes) ##Pool can be made higher by increasing multiTimes number
        linksA = pool.map(CheckSites, [list(c) for c in chunks]) ##domain_list is used as global list
        time.sleep(4)    
        pool.close()
        pool.join()
    
        ##Add all links to links4 of lists in list in list
        links2a = []
        links2b = []
        ##Create combined list linksA
        for links in linksA:
            for link in links:
                for l in link:
                    if l.lower().find('.pdf') > 0:
                        if not l in links2b:
                            links2b.append(l)
                    else:
                        if not l in links2a: 
                            links2a.append(l)
        
    else:
        ##Process list sequential
        links2a, links2b = CheckSites(links1a)
                       
    ##PDF will be stored later (new files may be found)
    ##Show end result of cleaning
    print(str(len(links2a)) + " unique drone aggregate website links found")
    f.write(str(len(links2a)) + " unique drone aggregate website links found\n")
    print(str(len(links2b)) + " unique potential drone websites links found")
    f.write(str(len(links2b)) + " unique potential drone websites links found\n")
    ##print(str(len(links1b)) + " unique PDF-links found")

    ##Save intermediate files (so one can also continue later)
    if len(links2a) > 0 or len(links1b) > 0 or len(links2b) > 0:

        if len(links2a) > 0:
            ##Store result
            fileNameE = "2_external_" + country.upper() + lang.lower() + "_drone_high_1.csv"
            externalLDF = pandas.DataFrame(links2a)
            externalLDF.to_csv(fileNameE, index=False) 
            print(str(len(links2a)) + " links found saved in " + fileNameE)

        if len(links2b) > 0:
            ##Store result
            fileNameE = "2_external_" + country.upper() + lang.lower() + "_drone_low_1.csv"
            externalLDF = pandas.DataFrame(links2b)
            externalLDF.to_csv(fileNameE, index=False) 
            print(str(len(links2b)) + " links found saved in " + fileNameE)
 
        if len(links1b) > 0:
            ##store pdf links
            fileNameP = "2_totalPDF_" + country.upper() + lang.lower() + "1.csv"
            totalPDFDF = pandas.DataFrame(links1b)
            totalPDFDF.to_csv(fileNameP, index=False) 
            print(str(len(links1b)) + " PDF-file links found saved in " + fileNameP)
        
    else:
        print("No links found")
        f.write("No links found\n")

    ##4. Brute force scraping of urls detected in websites found via search (But not the pdfs yet) after filtering
    ##Start domain urls, external urls (all urls outsite strat domains) and pdf links found are stored

    ##Use Links2a file links for that, Redircet links2a, build dictionary based on that
    internalL = links2a.copy()
    internalL.sort()
    externalL = []

    ##4a. Create dictoionary to enable to count how often each domain is visited
    g_domain_list = []
    for url in internalL:
        ##get Domain, strict
        dom = df.getDomain3(url)
        ##Check if already included
        if not dom in g_domain_list:
            #add domain
            g_domain_list.append(dom)

    ##Creat alias list (enable multiple key assigned to same value)
    g_alias_dict = dict()
    for i in range(len(g_domain_list)):
        ##Create alias
        alia = "id" + str(i)
        dom = g_domain_list[i]
        g_alias_dict[dom] = g_alias_dict.get(dom, alia)     

    ##Create dictionary (is used later)
    g_domain_dict = dict()
    for dom in g_domain_list:
        ##Get alias
        alia = g_alias_dict[dom]
        ##Start with 0 (for counting how often a domain is included)
        g_domain_dict[alia] = g_domain_dict.get(alia, 0)


    ##4b distribut urls over cores and start extracting
    ##The use of alias seriously reduces the number of links found (18.248 to 9258)
    ##Check for multicore
    if cores >= 4:
        ##Multicore scraping
    
        ##distribute urls over chunks, with same domain in same chunk
        ##First, randomize domain_list
        random.shuffle(g_domain_list)
        ##Create chunks list equal to number of cores available
        chunks = [[] for i in range(cores*multiTimes)]  ##Number of cores can be made higher by increasing multiTimes
        count = 0
        for dom in g_domain_list:
            ##Get all urls in internalL per domain
            urls = [x for x in internalL if df.getDomain3(x) == dom]        
            ##Add to chunk
            for url in urls:
                chunks[count].append(url)
            ##Add 1 to count
            count += 1
            ##Check for maximum
            if count >= len(chunks):
                count = 0
    
        ##Scrape listst    ##Use all cores to process chunks
        pool = mp.Pool(cores*multiTimes) ##Pool can be made higher by increasing multiTimes number
        linksL = pool.map(ExtractSites, [list(c) for c in chunks]) ##domain_list is used as global list
        time.sleep(4)    
        pool.close()
        pool.join()
    
        ##Add all links to links4 of lists in list in list
        links4a = []
        links4b = []
        ##Create combined external list
        for links in linksL:
            for link in links:
                for l in link:
                    if l.lower().find('.pdf') > 0:
                        links4b.append(l)
                    else: 
                        links4a.append(l)
    else:
        ##Serial scraping
        links4a, links4b = ExtractSites(internalL)           

    ##show intermediate results
    print("Brute force scarping resulted in " + str(len(links4a)) + " website urls")
    f.write("Brute force scarping resulted in " + str(len(links4a)) + " website urls\n")
    print("Brute force scarping resulted in " + str(len(links4b)) + " pdf file urls")
    f.write("Brute force scarping resulted in " + str(len(links4b)) + " pdf file urls\n")

            
    ##5. Clear and save results of external Links
    ##5a. Clean list
    externalL2, externalLNot = df.cleanLinks(links4a, country, False) 
    ##Save list in file
    if len(externalL2) > 0:
        fileNameE = "2_external_" + country.upper() + lang.lower() + "1.csv"
        exterDF = pandas.DataFrame(externalL2)        
        exterDF.to_csv(fileNameE, index=False)    

    ##5b Add any pdfs found to already existing list of pdfs and save file
    linksPDF = links1b + links4b
    ##Clean link, but do not check striclty (will be done later)
    externalNot, linksPDF2 = df.cleanLinks(linksPDF, country, False)

    ##store PDFs in file
    if len(linksPDF2) > 0:
        ##store pdf links
        fileNameP = "2_totalPDF_" + country.upper() + lang.lower() + "1.csv"
        totalPDFDF = pandas.DataFrame(linksPDF2)
        totalPDFDF.to_csv(fileNameP, index=False) 
 
    ##show end result
    print("Finished extensive urls search")
    f.write("Finished extensive urls search\n")
    print(str(len(externalL2)) + " unique external links found")
    f.write(str(len(externalL2)) + " unique external links found\n")
    print(str(len(linksPDF2)) + " unique pdf-links in total found")
    f.write(str(len(linksPDF2)) + " unique pdf-links in total found\n")

    f.close()
    print("Program ended")

##### END ###################################################################
