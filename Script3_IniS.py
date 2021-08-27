#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
Updated on Aug 11 2021, version 1.16, included limited pdf name search by creating list of pdf names, chunks contain identical named files
Try to maximally reduce extra (payed google) searches, imporved parallel processing
Option to in- or exclude fileSearch (new in Ini-file), additional country domain extension check after Redirect
"""
##Extract links (of drone companies) from pdf-files

#Load libraries 
import os
import sys
import time
import pandas
##import random
import re
import glob
import multiprocessing as mp
##import numpy as np
import configparser

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts")

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]{2,}[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##Factor to multiply the number of cores with for parallel scraping (not for search engine use)
multiTimes = 1 ##number to multiply number of parallel scraping sessions

##Define fundtions, to dealwith global variable
def getName(url):
    pdf = ""
    ##Split url and locate pdf containing part
    if len(url) > 0:
        ##Get pdf part
        res = url.split("/")
        for r in reversed(res):
            if r.lower().find('pdf') > 0:
                pdf = r
                break
    return(pdf)    

##PDF processing function
def processPDF(linksPDF):
    ##init list for links found
    links2 = []
    pdfsread = []
    
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
        ##define and clear name
        name = ""
        nameV = ""
        vurl = ""
        ##Check for .pdf in link
        if url.lower().find(".pdf") > 0:                     
            ##Check if filename in url is already downloaded succesfully
            name = getName(url)
            if not name in pdfsread and not name == "":
                ##Check if file exists (and get correct url, just in case)
                vurl = df.getRedirect(url) ##this prevents that acces may 'hang' on it, may result in other country domain ext link
            
                ##if file is not found and searc option is true SEARCH FOR DOMAIN NAME and FILENAME (on Yahoo3/GooglePayed?)
                if vurl == "" and fileSearch:
                    ##Try to find fresh link to file in domain (Yahoo is used for search)
                    if payedGoogle:
                        vurl = pg.searchPDFlink(url, country)
                    else:
                        vurl = df.searchPDFlink(url, country) ##Set to Yahoo at the moment (may be a problem when run in Parallel)
                        
                ##Check if link is located in domain extensions allowed
                if not vurl == "":
                    vurl = df.checkCountries([vurl], country) 
            else:
                ##already proccessed, clear vurl
                vurl = ""
                    
            ##Check if url exists and still refers to pdf
            if not vurl == "" and vurl.lower().find('.pdf') > 0:
                ###cut out whole link including .pdf (removes any garbage) 
                vurl = vurl[0:vurl.lower().rindex(".pdf")+4]        
                
                ##Check vurl name of file
                nameV = getName(vurl)
                if not nameV in pdfsread and not nameV == "":
                    ##Show url to process
                    print(vurl)
                
                    ##Scrape pdf, get text and make sure its lowercase (function has a time out, in case)
                    textPdf = df.getPdfText(vurl, True, number) ##adjust function for multicore download?
                
                    ##If no text is found , try to find a fresh link to file
                    if len(textPdf) == 0 and not url == vurl and fileSearch:  ##No need to search agian when urls are identical
                        ##Try to find fresh link to file in domain (Since name of file has not be extracted yet)
                        if payedGoogle:
                            vurl = pg.searchPDFlink(vurl, country)
                        else:
                            vurl = df.searchPDFlink(vurl, country) ##Set to Yahoo at the moment (may be a problem in Parallel)
 
                        ##Check result
                        nameV = getName(vurl)
                        if not vurl == "" and vurl.lower().find('.pdf') > 0 and not nameV in pdfsread:
                            ##Scrape pdf, get text and make sure its lowercase (function has a time out, in case)
                            textPdf = df.getPdfText(vurl, True, number) ##adjust function for multicore download?
                        else:
                            textPdf = ''
                else:
                    ##clear vars
                    vurl = ''
                    textPdf = ''
                        
                ##Check content
                if len(textPdf) > 0:
                    ##When file has been scraped succesfuly add to pdfsread
                    if not nameV in pdfsread: 
                        pdfsread.append(nameV)
                    
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
        fileSearch = config.getboolean('SETTINGS3', 'fileSearch')
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
    pdf2Content = df.loadCSVfile(fileName2) ##Error in file???

    ##Exctract filrst coloumn
    pdf1 = list(pdf1Content.iloc[:,0])
    if pdf1[0] == '0':
        pdf1 = pdf1[1:len(pdf1)]
    pdf2 = list(pdf2Content.iloc[:,0])
    if pdf2[0] == '0':
        pdf2 = pdf2[1:len(pdf2)]

    print(str(len(pdf1)) + " links found in pdf-list of Script 1")
    f.write(str(len(pdf1)) + " links found in pdf-list of Script 1\n")
    print(str(len(pdf2)) + " links found in pdf-list of Script 2")
    f.write(str(len(pdf2)) + " links found in pdf-list of Script 2\n")

    ##Combine lists
    pdfAll = pdf1 + pdf2
    
    ##Clean files (deduplicate, preprocess  check country)
    linksNot, totalPDF = df.cleanLinks(pdfAll, country, True)

    ##store results
    print("A total of " + str(len(totalPDF)) + " links found in pdf-list of Scripts 1 and 2")
    f.write("A total of " + str(len(totalPDF)) + " links found in pdf-list of Scripts 1 and 2\n")

    ##2. Process and check PDFs
    links2 = []
    ##2a. Check for multicore run
    if cores >= 2:
        ##Use different search engines simultaniously
        print("Parallel PDF-extraction started on " + str(cores) + " cores")
        f.write("Parallel PDF-extraction started on " + str(cores) + " cores\n")
    
        ##Create chunks with similar pdf names in same chunk
        ##a. Get unique names
        namesPDF = []
        for pdf in totalPDF:
            name = getName(pdf)
            if not name in namesPDF and not name == '':
                namesPDF.append(name)
        namesPDF.sort()
        
        ##b. Create empty chunks
        chunks = []
        for i in range(cores*multiTimes):
            chunks.append([])
            
        ##c. distribute pdfs over chunks (name dependent so identical names are included in same chunk)
        start = 0
        for name in namesPDF:
            ##get pdflinks containing exact name (PARTIAL MATCHING MUST BE PREVENTED)
            pdfs = [x for x in totalPDF if name == getName(x)]
            ##Add to chunk
            if len(pdfs) > 0:
                for p in pdfs:
                    if not p in chunks[start]:
                        chunks[start].append(p)
                start += 1
                if start >= len(chunks):
                    start = 0
            
        ##Use all cores to process file
        pool = mp.Pool(cores*multiTimes)
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
