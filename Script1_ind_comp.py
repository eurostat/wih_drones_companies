#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
"""
##Search websites of drone company urls (in ireland)

#Load libraries 
import os
import time
import pandas
import multiprocessing as mp

##Set directory
os.chdir("/home/piet/R/Drone")

##Set variables
waitTime = 60

##import functions
import Drone_functions as df 
##get cores of machine used
cores = mp.cpu_count()

## INIT SETTING ###############################################################

query = '(drone OR rpas OR uav OR uas) (operator OR company OR builder) ireland'
country = 'ie'
lang = 'en'
##ireland1 = ['ie', 'ireland', 'irish']
##ireland2 = ['ireland', ' eire ', 'irish']
##drone_words = ["drone", "rpas", "uav", "uas", "unmanned", "aerial"]
##mem_words = ["member", "register", "registration", "list", "overview"]

### START #######################################################################

##1. Init search query
print("Searching the web for sites of drone companies")

##Set no limit
limit = 0 ##(so no limitations on number of links collected)


##2. Scrape sequentially or multicore
links = []
##Check multicore option
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

    ##Wait til finished
    p1.join()
    p2.join()
    p3.join()
    p4.join()
    
    ##Add to result
    for i in range(4):
        result.append(out_q.get())
    
    ##combine all links
    links = []
    for res in result:
        links += res
    
else:
    ##Use search enginses sequentally, include searches that work (B2 is a problem) 
    print("Sequential search engine option is used")

    ##Google
    print("Google 1 search")
    G1links = df.queryGoogle1(query, country)
    print("Google 1 found " + str(len(G1links)) + " urls")
                              
    ##Bing
    print("Bing 1 search")
    B1links = df.queryBing1(query, country)
    print("Bing 1 found " + str(len(B1links)) + " urls")

    ##DuckDuckGo
    print("Duck 1 search")
    D1links = df.queryDuck1(query, country) ##stop rather early
    print("Duck 1 found " + str(len(D1links)) + " urls")

    ##Yahoo search
    print("Yahoo 2 search")
    Y2links = df.queryYahoo2(query, country)
    print("Yahoo 2 found " + str(len(Y2links)) + " urls")

    print("AOL 1 search")
    A1links = df.queryAOL(query, country)
    print("AOL 1 found " + str(len(A1links)) + " urls")
    
    ##Google
    print("Google 3 search")
    G3links = df.queryGoogle3(query, country)
    print("Google 3 found " + str(len(G3links)) + " urls")

    ##Bing
    print("Bing 2 search")
    B2links = df.queryBing2(query, country)
    print("Bing 2 found " + str(len(B2links)) + " urls")
    
    ##DuckDuckGo
    print("Duck 3 search")
    D3links = df.queryDuck3(query, country) ##stop rather early
    print("Duck 3 found " + str(len(D3links)) + " urls")

    ##Combine all
    links = G1links + B1links + D1links + Y2links + A1links + G3links + B2links + D3links
    
##3. Process links and store result (do a country check)
links1a, links1b = df.cleanLinks(links, country, True)

if len(links1a) > 0 or len(links1b) > 0:
    print(str(len(links1a)+len(links1b)) + " unique links found")

    if len(links1a) > 0:
        ##Store result
        fileNameE = "1_external_" + country.upper() + ".csv"
        externalLDF = pandas.DataFrame(links1a)
        externalLDF.to_csv(fileNameE, index=False) 
        print(str(len(links1a)) + " links found saved in " + fileNameE)

    
    if len(links1b) > 0:
        ##store pdf links
        fileNameP = "1_totalPDF_" + country.upper() + ".csv"
        totalPDFDF = pandas.DataFrame(links1b)
        totalPDFDF.to_csv(fileNameP, index=False) 
        print(str(len(links1b)) + " PDF-file links found saved in " + fileNameP)

        
else:
    print("No links found")
    
print("Program 1 -find unique links to drone web sites- finished")


## END ############################################################################
