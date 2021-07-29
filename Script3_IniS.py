#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
Updated on July 28 2021, version 1.12
"""
##Extract links (of drone companies) from pdf-files

#Load libraries 
import os
import sys
import time
import pandas
import random
import re
import glob
import multiprocessing as mp
import numpy as np
import configparser

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts")

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]{2,}[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##PDF processing function
def processPDF(linksPDF):
    ##init list for links found
    links2 = []
    
    ##Check for multicore, create tempFiles of PDF (when needed) unique for each core
    if cores >= 2:
        ##Get process
        num = mp.current_process()
        ##num = '<ForkProcess(ForkPoolWorker-1, started daemon)>'
        ##get first number in process description
        number = re.findall(r'\d+', str(num))[0]
    else:
        number = '0'
        
    ##Get content of PDFs and extract links
    for url in linksPDF:
        ##Check for .pdf in link
        if url.lower().find(".pdf") > 0:                     
            ##Check if file exists (and get correct url, just in case)
            vurl = df.getRedirect(url) ##this prevents that acces may 'hang' on it
            
            ##IF FILE DOES NOT EXISTS SEARCH FOR LINK TO FILE BY SEARCHING FOR DOMAIN NAME and FILENAME (on Yahoo3/GooglePayed?)
            if vurl == "":
                 ##Try to find fresh link to file in domain (Yahoo is used for search)
                if payedGoogle:
                    vurl = pg.searchPDFlink(url, country)
                else:
                    vurl = df.searchPDFlink(url, country) ##Set to Yahoo at the moment (may be a problem when run in Parallel)
                
            ##Check if url exists and still refers to pdf
            if not vurl == "" and vurl.lower().find('.pdf') > 0:
                ###cut out whole link including .pdf (removes any garbage) 
                vurl = vurl[0:vurl.lower().rindex(".pdf")+4]        
                ##Show url to process
                print(vurl)
                
                ##Scrape pdf, get text and make sure its lowercase (function has a time out, in case)
                textPdf = df.getPdfText(vurl, True, number) ##adjust function for multicore download?
                
                ##If no text is found , try to find a fresh link to file
                if len(textPdf) == 0:
                    ##Try to find fresh link to file in domain (Yahoo is used for search)
                    if payedGoogle:
                        vurl = pg.searchPDFlink(vurl, country)
                    else:
                        vurl = df.searchPDFlink(vurl, country) ##Set to Yahoo at the moment (may be a problem in Parallel)
 
                    ##Check result
                    if not vurl == "" and vurl.lower().find('.pdf') > 0:
                        ##Scrape pdf, get text and make sure its lowercase (function has a time out, in case)
                        textPdf = df.getPdfText(vurl, True, number) ##adjust function for multicore download?
                    else:
                        textPdf = ''
                        
                ##Check content
                if len(textPdf) > 0:
                    ##Deal with \n but prevent . (at end of sentence) merges with start of next sentence
                    text = textPdf.replace(".\n", ". ")  
                    ##Correct \n in text (may split urls in 2 parts)
                    text = text.replace("\n", "") 
                    ##Check for ireland and drone words
                    if any(x in text for x in countryW2) and any(x in text for x in drone_words):
                        ##Count number of drone_words occuring in text?
                        ##Get all urls
                        links = df.extractLinksText(text, genUrl)
                
                        ##Check if mail domains need to be included
                        if mailInclude:
                            ##get mail domains
                            links1 = df.extractLinksText(text, genMail)
                            ##Combine results
                            links = links + links1                   
                
                        ##remove any duplicates
                        links = list(set(links))                
                    
                        ##Check if url is NOT in domain of url, is NOT empty and (?is NOT in included in internal domains scraped)
                        for link in links:
                            link = link.strip()
                            ##Check end, is it a dot?
                            if link.lower().endswith("."):
                                link = link[0:len(link)-1]
                            ##Check more
                            if link.lower().find('.pdf') > 0:  ##Add new pds links to totalPDF list
                                ##Check url
                                if not link.lower().startswith("http"):
                                    link = df.getRedirect("http://" + link)   
                                ##Check if it needs to be added
                                if not link in linksPDF:
                                    linksPDF.append(link)
                            else:            
                                ##Contiue with NON-PDF links found
                                ##do ahttp check and add to list of external links
                                if link.startswith("http"):
                                    if not link in links2:
                                        links2.append(link)
                                else:
                                    ##Not staring with http, add it and check (may solve some erroneous links)
                                    linkA = df.getRedirect("http://" + link)
                                    if not linkA == "":
                                        if not linkA in links2:
                                            links2.append(linkA)   
    ##report result
    if len(links2) > 0:
        print(str(len(links2)) + " links extracted")
    return(links2)


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
    print("Use 'python3 Script3_Ini.py <filename.ini>' to run program") 


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
        payedGoogle = config.getboolean('SETTINGS3', 'payedGoogle')
        countryW2 = config.get('SETTINGS3', 'countryW2').split(',')
        drone_words = config.get('SETTINGS3', 'drone_words').split(',')
        runParallel = config.getboolean('SETTINGS3', 'runParallel')
        mailInclude = config.getboolean('SETTINGS3', 'mailInclude')
        
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(countryW2) > 0 and len(drone_words) > 0 and len(str(runParallel)) > 0 and len(str(mailInclude)) > 0:
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


##C. Check if programm can be run
if Continue:
    ##import functions
    import Drone_functions as df 
    import payedGoogle as pg
    ##set payed google key
    pg._api_key_ = googleKey  
    
    ##get number of cores of machine used
    cores = mp.cpu_count()

    ##Check runParallel setting
    if not runParallel:
        cores = 1

    ##Create logfile
    logFile = "3_results_" + country.upper() + lang.lower() + "1.txt"
    f = open(logFile, 'w')

    print("Loading PDF-files found in scripts 1 and 2")
    f.write("Loading PDF-files found in scripts 1 and 2\n")

    ##construct filesname
    fileName1 = "1_totalPDF_" + country.upper() + lang.lower() + "1.csv"
    fileName2 = "2_totalPDF_" + country.upper() + lang.lower() + "1.csv"

    ##Get content
    pdf1Content = df.loadCSVfile(fileName1)
    pdf2Content = df.loadCSVfile(fileName2)

    ##Exctract filrst coloumn
    pdf1 = list(pdf1Content.iloc[:,0])
    pdf2 = list(pdf2Content.iloc[:,0])

    print(str(len(pdf1)) + " links found in pdf-list of Script 1")
    f.write(str(len(pdf1)) + " links found in pdf-list of Script 1\n")
    print(str(len(pdf2)) + " links found in pdf-list of Script 2")
    f.write(str(len(pdf2)) + " links found in pdf-list of Script 2\n")

    ##Combine lists
    pdfAll = pdf1 + pdf2
    
    ##Clean files (deduplicate, preprocess  etc, DO NOT CHECK COUNTRY YET)
    linksNot, totalPDF = df.cleanLinks(pdfAll, country, False)

    ##store results
    print("A total of " + str(len(totalPDF)) + " links found in pdf-list of Script 1")
    f.write("A total of " + str(len(totalPDF)) + " links found in pdf-list of Script 1\n")

    ##2. Process and check PDFs
    links2 = []
    ##2a. Check for multicore run
    if cores >= 2:
        ##Use different search engines simultaniously
        print("Parallel PDF-extraction started on " + str(cores) + " cores")
        f.write("Parallel PDF-extraction started on " + str(cores) + " cores\n")
    
        ##randomize dataframe
        random.shuffle(totalPDF)
    
        ##split pdf in chunks
        chunks = np.array_split(totalPDF, cores, axis = 0)
    
        ##Use all cores to process file
        pool = mp.Pool(cores)
        linksP = pool.map(processPDF, [list(c) for c in chunks])
        time.sleep(4)    
        pool.close()
        pool.join()
    
        ##Add all links to links2 of lists in list
        for link in linksP:
            for l in link:
                if not l in links2:
                    links2.append(l)
       
    else:
        ##Use sequential processing method
        print("Sequential PDF extraction started")
        f.write("Sequential PDF extraction started\n")    
        links2 = processPDF(totalPDF)

    ##Report result
    print(str(len(links2)) + " total number of pdf derived links found")
    f.write(str(len(links2)) + " total number of pdf derived links found\n")


    ##3. Clear and save results of external Links
    ##3a Remove double and obvious links to other countries 
    externalL, externalLNot = df.cleanLinks(links2, country, True)
    ##links will be checked for correct http etc in next script

    ##Show result
    print("Finished extensive urls search")
    f.write("Finished extensive urls search\n")
    print(str(len(externalL)) + " unique external links found")
    f.write(str(len(externalL)) + " unique external links found\n")
    f.close()

    ##3b. Save combined findings as csv's
    fileNameP = "3_externalPDF_"+ country.upper() + lang.lower() + "1.csv"
    totalPDFDF = pandas.DataFrame(externalL)
    totalPDFDF.to_csv(fileNameP, index=False) 
    
    ##3c. Check for temp files and remove them
    tempF = glob.glob("tempFile*.pdf")
    ##Check and remove if tempFiles are found
    if len(tempF) > 0:
        ##remove files
        for f in tempF:
            os.remove(f)
            

##Finished
