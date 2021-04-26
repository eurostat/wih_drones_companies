#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
"""
##Extract links (of drone companies) from pdf-files (in ireland)

#Load libraries 
import os
import time
import pandas
import random
##from collections import Counter
import multiprocessing as mp
import numpy as np

##Set directory
os.chdir("/home/piet/R/Drone")
##Max wait time for website to react
waitTime = 60
##Include mail as links
mailInclude = True

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]+[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"

##import functions
import Drone_functions as df 
##get number of cores of machine used
cores = mp.cpu_count()

## INIT SETTING ###############################################################

country = 'ie'
lang = 'en'
##ireland1 = ['ie', 'ireland', 'irish']
ireland2 = ['ireland', ' eire ', 'irish']
drone_words = ["drone", "rpas", "uav", "uas", "unmanned", "aerial"]
##mem_words = ["member", "register", "registration", "list", "overview"]

## START ######################################################################

##1. Find and load pdf from previous search query
print("Loading PDF-files found in scripts 1 and 2")

##construct filesname
fileName1 = "1_totalPDF_" + country.upper() + ".csv"
fileName2 = "2_totalPDF_" + country.upper() + ".csv"

##Get content
pdf1Content = df.loadCSVfile(fileName1)
pdf2Content = df.loadCSVfile(fileName2)

##Exctract filrst coloumn
pdf1 = list(pdf1Content.iloc[:,0])
pdf2 = list(pdf2Content.iloc[:,0])

print(len(pdf1) + " links found in pdf-list of Script 1")
print(len(pdf2) + " links found in pdf-list of Script 2")

##Combine lists
pdfAll = pdf1 + pdf2
##Clean files (deduplicate, preprocess  etc, DO NOT CHECK COUNTRY YET)
linksNot, totalPDF = df.cleanLinks(pdfAll, country, False)

##Define pdf process fundtion
def processPDF(linksPDF):
    ##init list for links found
    links2 = []
    
    ##Get content of PDFs and extract links
    for url in linksPDF:
        ##Check for .pdf in link
        if url.lower().find(".pdf") > 0:        
            ##Get the url referred to
            vurl = df.getRedirect(url)
        
            ##Check if url exists and still refers to pdf
            if not vurl == "" and vurl.lower().find('.pdf') > 0:
                ###cut out whole link including .pdf (removes any garbage) 
                vurl = vurl[0:vurl.lower().rindex(".pdf")+4]        
                print(vurl)

                ##Scrape pdf, get text and make sure its lowercase
                textPdf = df.getPdfText(vurl, True)
            
                ##Check content
                if len(textPdf) > 0:
                    ##Correct \n in text (may split urls in 2 parts)
                    text = textPdf.replace("\n", "")                    
                
                    ##Check for ireland and drone words
                    if any(x in text for x in ireland2) and any(x in text for x in drone_words):
            
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
                    
                        #remove url domain specific links
                        links2 = []
                        
                        ##Check if url is NOT in domain of url, is NOT empty and (?is NOT in included in internal domains scraped)
                        for link in links:
                            if link.lower().find('.pdf') > 0:  ##Add new pds links to totalPDF list
                                if not link in linksPDF:
                                    linksPDF.append(link)
                            else:            
                                ##Contiue with NON-PDF links found
                                ##do ahttp check and add to list of external links
                                if link.startswith("http"):
                                    if not link in links2:
                                        links2.append(link)
                                else:
                                    ##no staring part, add it and check
                                    linkA = df.getRedirect("http://" + link)
                                    if not linkA == "":
                                        if not linkA in links2:
                                            links2.append(linkA)    

    return(links2)

##2. Process and check PDFs
links2 = []
##1a. Check for multicore run
if cores >= 2:
    ##Use different search engines simultaniously
    print("Parallel PDF extraction started on " + str(cores) + " cores")
    
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
    ##use sequential search
    print("Sequential PDF extraction started")
    links2 = processPDF(totalPDF)

##3. Clear and save results of external Links
## Remove double and obvious links to other countries 
externalL, externalLNot = df.cleanLinks(links2, country, True)
##links will be checked for correct http etc in next script

##Show result
print("Finished extensive urls search")
print(str(len(externalL)) + " unique external links found")

##5b. Save combined findings as csv's
fileNameP = "3_externalPDF_"+ country.upper() + ".csv"
totalPDFDF = pandas.DataFrame(externalL)
totalPDFDF.to_csv(fileNameP, index=False) 

