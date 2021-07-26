#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
Scrape per search engine
@author: piet
Updated on July 23 2021, version 2.0 (All loop needs to implemented nicely)
"""
##Search websites of drone company urls with six Search Engines (in paralle sequential or seperate)

#Load libraries 
import os
import sys
import time
import pandas
import multiprocessing as mp
import configparser

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Scripts")

##Define local function
def scrapeSearchEngines(searchEng, query, country, limit = 0):
        
    ##Use search enginses sequentally, include searches that work (B2 is a problem) 
    if searchEng == "" or searchEng == "D1":
        ##DuckDuckGo
        print("Duck 1 search")
        D1links = df.queryDuck1VPN(query, country, 5, limit) ##Chromedriver may crash, restart not optimal yet
        print("Duck 1 found " + str(len(D1links)) + " urls")
        f.write("Duck 1 found " + str(len(D1links)) + " urls\n")
    else:
        D1links = []
                
    ##Google
    if searchEng == "" or searchEng == "GV" or searchEng == "G1":
        if payedGoogle or searchEng == "GV":
            print("Google payed search")
            G1links = pg.queryGoogleV(query, country, 0, limit)
            print("Google payed found " + str(len(G1links)) + " urls")
            f.write("Google payed found " + str(len(G1links)) + " urls\n")
        else:
            if not payedGoogle or searchEng == "G1":
                print("Google 1 search")
                G1links = df.queryGoogle1(query, country, 20, limit)
                print("Google 1 found " + str(len(G1links)) + " urls")
                f.write("Google 1 found " + str(len(G1links)) + " urls\n")
    else:
        G1links = []
                
    ##Bing
    if searchEng == "" or searchEng == "B1":
        print("Bing 1 search")
        B1links = df.queryBing1(query, country, 5, limit)
        print("Bing 1 found " + str(len(B1links)) + " urls")
        f.write("Bing 1 found " + str(len(B1links)) + " urls\n")
    else:
        B1links = []
                
    ##Yahoo search
    if searchEng == "" or searchEng == "Y3":
        print("Yahoo 3 search")
        Y3links = df.queryYahoo3VPN(query, country, 20, limit)
        print("Yahoo 3 found " + str(len(Y3links)) + " urls")
        f.write("Yahoo 3 found " + str(len(Y3links)) + " urls\n")
    else:
        Y3links = []
                
    ##AOL search ##Include Ask?
    if searchEng == "" or searchEng == "A2":
        print("AOL 2 search")
        A2links = df.queryAOL2VPN(query, country, 20, limit)
        print("AOL 2 found " + str(len(A2links)) + " urls")
        f.write("AOL 2 found " + str(len(A2links)) + " urls\n")
    else:
        A2links = []
                
    ##Search Ask S1
    if searchEng == "" or searchEng == "S1":
        print("Ask 1 search")
        S1links = df.queryAsk1VPN(query, country, 20, limit)
        print("Ask 1 found " + str(len(S1links)) + " urls")
        f.write("Ask 1 found " + str(len(S1links)) + " urls\n")
    else:
        S1links = []
             
    ##Combine all (will be emtpy when wrong searchEngine code is used)
    links = G1links + B1links + D1links + Y3links + A2links + S1links

    return(links)

### START #####################################################################
Continue = False
fileName = ''
searchEng = ''

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
        searchEng = str(sys.argv[2])
        ##set continue first to False
        Continue = False
        ##Check input
        if searchEng == "A2" or searchEng == "B1" or searchEng == "D1" or searchEng == "G1" or searchEng == "GV" or searchEng == "S1" or searchEng == "Y3" or searchEng == "All":
            print("Specific search engine scraping selected (" + str(searchEng) + ")")
            Continue = True
else:
    print("Use 'python3 Script1_Ini.py <filename.ini> <OPT: searchEngine (A2,B1,D1,G1,GV,S1,Y3,All)>' to run program") 


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
        googleKey = config.get('MAIN', 'googleKey')
        queries = config.get('SETTINGS1', 'queries').split(',')
        payedGoogle = config.getboolean('SETTINGS1', 'payedGoogle')
        runParallel = config.getboolean('SETTINGS1', 'runParallel')
        limit = config.getint('SETTINGS1', 'limit')
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(queries) > 0 and len(str(payedGoogle)) > 0 and len(str(runParallel)) > 0 and len(str(limit)) > 0:
            print("All variables from ini-file checked")
        
    except:
        ##An erro has occured
        print("An error has occured while reading ini-file")
        print("Check ini-files content and retry")
        Continue = False
        ##Check vars needed
else:
    if not fileName == '':
        print("Please provide a valid filename")
    else:
        if len(searchEng) > 0:
            print("Please use one of the allowed search engine abbreviations (A2, B1, D1, G1, GV, S1, Y3, All)")
    Continue = False

##C. Check if programm can be run
if Continue:
    ##import functions from py-files
    import Drone_functions as df 
    import payedGoogle as pg
    ##set payed google key
    pg._api_key_ = googleKey  
    
    ##get cores of machine used
    cores = mp.cpu_count()
    
    ##1. Init search query
    print("Searching the web for sites of drone companies")

    ##Check for parallel run
    if not runParallel:
        cores = 1
    
    ##check for specific searchEngine scrape
    if len(searchEng) > 0:
        runParallel = False
        cores = 1
        ##Specific check
        if searchEng == "GV" or searchEng == "All":
            payedGoogle = True
                  
    ##2. Run queries on search engine (either sequentially or multicore

    ##Create logfile
    if len(searchEng) > 0:
        logFile = "1_results_" + country.upper() + lang.lower() + "1_" + str(searchEng) + ".txt"
    else:
        logFile = "1_results_" + country.upper() + lang.lower() + "1.txt"
    f = open(logFile, 'w')
    
    ##create intermediate file
    intFile = logFile.replace(".txt", "_int.txt") ##Store query and links imediatly after collection

    ##Save prallel status
    if runParallel:
        print("Parallel search engine option is used (max 5 different)")        
        f.write("Parallel run with " + str(cores) + " cores\n")
    else:
        print("Sequential search engine option is used")
        f.write("Serial run (single core)\n")
        if len(searchEng) > 0 and not searchEng == "All":
            f.write("Specific search engine scraping selected (" + str(searchEng) + ")\n")
        else:
            f.write("All search engines are selected (" + str(searchEng) + ") and run sequential\n")
    
    ##Check option chosen
    links2 = []
    if not searchEng ==  "All":
        ##create int file
        fint = open(intFile, "a")

        ##Save first string ininit file
        if not searchEng == "":
            fint.write("Intermediate file for " + str(fileName) + " for search engine " + str(searchEng) + "\n")
        else:
            fint.write("Intermediate file for " + str(fileName) + "\n")
        fint.close() ##So the data is saved immediatly
        
        ##Run queries
        for query in queries:
            f.write("Query: " + query + "\n")
            print("Query: " + query)
        
            ##Parallel scrape or not (per query)
            if cores >= 2:
                ##init output queue
                out_q = mp.Queue()

                ##Init 4 simultanious queries, store output in queue
                p1 = mp.Process(target = df.queryBing1mp, args = (query, country, out_q, limit))
                p1.start()
                p2 = mp.Process(target = df.queryDuck1mp, args = (query, country, out_q, limit)) ##Make sure it runs (deal with driver issues)
                p2.start()
                p3 = mp.Process(target = df.queryYahoo3mp, args = (query, country, out_q, limit))
                p3.start()
                if payedGoogle:
                    p4 = mp.Process(target = pg.queryGoogleVmp, args = (query, country, out_q, limit))
                else:
                    p4 = mp.Process(target = df.queryGoogle1mp, args = (query, country, out_q, limit))            
                p4.start()    
                p5 = mp.Process(target = df.queryAOL2mp, args = (query, country, out_q, limit))
                p5.start()
                p6 = mp.Process(target = df.queryAsk1mp, args = (query, country, out_q, limit))
                p6.start()
        
                time.sleep(6)
    
                ##Wait till finished
                p1.join()
                p2.join()
                p3.join()
                p4.join()
                p5.join()
                p6.join()
    
                SEnames = ["Bing", "DuckDuckGo", "Yahoo", "Google", "AOL", "Ask"]
            
                ##Add to result
                result = []
                for i in range(len(SEnames)):
                    result.append(out_q.get())
 
                ##combine all links
                links = []
                count = 0
                for res in result:
                    f.write("Search engine " + str(SEnames[count]) +  " found " + str(len(res)) +  " urls\n")
                    links += res
                    count += 1
                
                time.sleep(5)
                ##Close query?
            else: 
                ##Scrape sequentially   (query based)
                links = scrapeSearchEngines(searchEng, query, country, limit)    
            
            ##Add links results to total links
            links2 += links
        
            ##Add intermediairy results, ALL query and new links found, to intermediairy file (to assure results are saved)
            fint = open(intFile, "a")
            if not searchEng == "":
                fint.write("## '" + str(query) + "' for search engine " + str(searchEng) + "\n")
            else:    
                fint.write("## " + str(query) + "\n")
            ##Store results of links are found
            if len(links) > 0:
                for link in links:
                    fint.write(link + "\n")
            fint.close()               
         
        ##Process data collected
        ##show end result
        print("A total number of " + str(len(links2)) + " found")
        f.write("A total number of " + str(len(links2)) + " found\n")

        ##Store results
        ##3. Check links and store result (do a country check)
        links1a, links1b = df.cleanLinks(links2, country, True)

        if len(links1a) > 0 or len(links1b) > 0:
            print(str(len(links1a)+len(links1b)) + " unique links found")
            f.write(str(len(links1a)+len(links1b)) + " unique links found\n")

            if len(links1a) > 0:
                ##Store result
                if searchEng == "":
                    fileNameE = "1_external_" + country.upper() + lang.lower() + "1.csv"
                else:
                    fileNameE = "1_external_" + country.upper() + lang.lower() + "1_" + str(searchEng) + ".csv"
                externalLDF = pandas.DataFrame(links1a)
                externalLDF.to_csv(fileNameE, index=False) 
                print(str(len(links1a)) + " web site links found saved in " + fileNameE)
                f.write(str(len(links1a)) + " web site links found saved in " + fileNameE + "\n")
    
            if len(links1b) > 0:
                ##store pdf links
                if searchEng == "":
                    fileNameP = "1_totalPDF_" + country.upper() + lang.lower() + "1.csv"
                else:
                    fileNameP = "1_totalPDF_" + country.upper() + lang.lower() + "1_" + str(searchEng) + ".csv"
                totalPDFDF = pandas.DataFrame(links1b)
                totalPDFDF.to_csv(fileNameP, index=False) 
                print(str(len(links1b)) + " PDF-file links found saved in " + fileNameP)
                f.write(str(len(links1b)) + " PDF-file links found saved in " + fileNameP + "\n")
        
        else:
            print("No links found")
            f.write("No links found\n")
        
    else:
        ##query each search engine sequential and store results in individual files
        if payedGoogle:
            SE = ["D1", "B1", "GV", "A2", "S1", "Y3"]
        else:
            SE = ["D1", "B1", "G1", "A2", "S1", "Y3"]

        ##Select search enging
        for searchEng1 in SE:
            ##creat specific init file
            intFile1 = intFile.replace("_int.txt", "_" + str(searchEng1) + "_int.txt") 
            ##create file
            fint = open(intFile1, "a")

            ##Save first string ininit file
            fint.write("Intermediate file for " + str(fileName) + " for search engine " + str(searchEng1) + "\n")
            fint.close() ##So the data is saved immediatly

            links2 = []
            for query in queries:
                ##show progress
                print(str(searchEng1) + " for " + str(query))
                ##Collect links
                links = scrapeSearchEngines(searchEng1, query, country, limit)
                ##Add links results to total links
                links2 += links
            
                ##Add intermediairy results, ALL query and new links found, to intermediairy file (to assure results are saved, as recent as possible)
                fint = open(intFile1, "a")
                fint.write("## '" + str(query) + "' for search engine " + str(searchEng1) + "\n")
                ##Store results of links are found
                if len(links) > 0:
                    for link in links:
                        fint.write(link + "\n")
                fint.close()            

            ##show end result
            print("A total number of " + str(len(links2)) + " found")
            f.write("A total number of " + str(len(links2)) + " found\n")

       
            ##3. Check links and store result (do a country check) for EACH search engine in a seperate file
            links1a, links1b = df.cleanLinks(links2, country, True)

            if len(links1a) > 0 or len(links1b) > 0:
                print(str(len(links1a)+len(links1b)) + " unique links found")
                f.write(str(len(links1a)+len(links1b)) + " unique links found\n")

                if len(links1a) > 0:
                    ##Store result
                    fileNameE = "1_external_" + country.upper() + lang.lower() + "1_" + str(searchEng1) + ".csv"
                    externalLDF = pandas.DataFrame(links1a)
                    externalLDF.to_csv(fileNameE, index=False) 
                    print(str(len(links1a)) + " web site links found saved in " + fileNameE)
                    f.write(str(len(links1a)) + " web site links found saved in " + fileNameE + "\n")
 
                if len(links1b) > 0:
                    ##store pdf links
                    fileNameP = "1_totalPDF_" + country.upper() + lang.lower() + "1_" + str(searchEng1) + ".csv"
                    totalPDFDF = pandas.DataFrame(links1b)
                    totalPDFDF.to_csv(fileNameP, index=False) 
                    print(str(len(links1b)) + " PDF-file links found saved in " + fileNameP)
                    f.write(str(len(links1b)) + " PDF-file links found saved in " + fileNameP + "\n")
        
            else:
                print("No links found")
                f.write("No links found\n")

    ##Close log file
    f.close()

    if searchEng == "":
        print("Program 1 -find unique links to drone web sites- finished")
    else:
        print("Program 1 -find unique links with " + str(searchEng) + " search engine to drone web sites- finished")

## END ############################################################################

