#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
Updated on Oct 25 2021, version 1.43, included limited pdf name search by creating list of pdf names, chunks contain identical named files
Try to maximally reduce extra (payed google) searches, imporved parallel processing
Option to in- or exclude fileSearch (new in Ini-file), additional country domain extension check after Redirect, vurl empty list check added
Include option to extract names (entities) from pdf files (option includeNames) and save as a seperate file, with minimal memory use, incl very strict entity reduction (spacy based)
Added additional cleanig steps based on ES, IE, DE, NL, IT experiences, included how to deal with zero names extracted
"""
##Extract links (of drone companies) from pdf-files

#Load libraries 
import os
import sys
##import time
import pandas
##import random
import re
import glob
import multiprocessing as mp
##import numpy as np
import configparser
import unicodedata
import string
import gc
import spacy

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts")

##Define regexes used
genUrl = r"((?:https?://)?(?:[a-z0-9\-]+[.])?([a-zA-Z0-9_\-]{2,}[.][a-z]{2,4})(?:[a-zA-Z0-9_\-./]+)?)"
genMail = r"[a-zA-Z0-9_\-.]+@([a-zA-Z0-9_\-]+[.][a-z]{2,4})"
##Patern to remove spacces between single characters
genSpace = re.compile(r'\b([a-zA-Z]) (?=[a-zA-Z]\b)', re.I)

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
    del url
    return pdf

##function to detect strings filled with predominantly city names (> 80%)
def checkMultipleCityNames(text):
    ##if many names occure ignore string
    boolCheck = False
    score = 0
    words = []
    ##Check for splitters
    spaceNr = text.count(" ")
    hyphNr = text.count("-")
    
    ##Check which splitter to use
    if hyphNr > spaceNr:
        ##split words
        words = text.split("-")
        ##Keep words with at least one capital only
        words = [x for x in words if any(s.isupper() for s in x)]
        
        if len(words) > 0:
            ##get score
            for w in words:
                w = w.strip().lower().capitalize()
                if w in municL:
                    score +=1
            ##Check fraction of city names
            if score/len(words) > 0.8:
                boolCheck = True
        else:
            ##No relevant words, so remove
            boolCheck = True
    elif spaceNr > 0 and spaceNr >= hyphNr:
        ##split words
        words = text.split(" ")
        ##Keep words with capatals only
        words = [x for x in words if any(s.isupper() for s in x)]
        
        if len(words) > 0:
            ##get score
            for w in words:
                if not w == "":
                    w = w.strip().lower().capitalize()
                    if w in municL:
                        score +=1
            ##Check fraction of city names        
            if score/len(words) > 0.8: 
                boolCheck = True
        else:
            ##No relevant words so remove
            boolCheck = True
    else:
        ##Single word?
        word = text.strip().lower().capitalize()
        if word in municL:
            boolCheck = True
    
    ##clear vars
    del text 
    del words
    return boolCheck

##Function used to check for duplicates
def cleanString(text):
    text = ''.join([word for word in text if word not in string.punctuation])
    text = text.lower()
    text = ''.join([word for word in text.split()])
    return text
    
##Function to reduce names in names list
def reduceNames(namesL):
    if len(namesL) > 0:
        ##remove any duplicates
        namesL = list(set(namesL))    
        
        ##Remove obvious non-enity names
        namesL = [x.replace('"', '').strip() for x in namesL]
        namesL = [x.replace("'", "").strip() for x in namesL]
        
        ##remove any names with aa #
        namesL = [x for x in namesL if not x.count("#") > 0]
        namesL = [x for x in namesL if not x.count("[") > 0]
        namesL = [x for x in namesL if not x.count("]") > 0]        
        namesL = [x for x in namesL if not x.count("%") > 0]
        namesL = [x for x in namesL if not x.count("$") > 0]
        namesL = [x for x in namesL if not x.count("*") > 0]
        namesL = [x for x in namesL if not x.count("=") > 0]
        namesL = [x for x in namesL if not x.count("<") > 0]
        namesL = [x for x in namesL if not x.count(">") > 0]  
        namesL = [x for x in namesL if not x.count("_") > 2]
        namesL = [x for x in namesL if not x.count("+") > 1]
        
        ##More specific cleaning
        ##clear leading & (but keep other &) and leading @
        for i in range(len(namesL)):
            name = namesL[i]
            if name.startswith("&"):
                ##remove leading & sign (nothing more) and any leading (and lagging) spaces 
                namesL[i] = name[1:len(name)].strip()
            elif name.startswith("@ "):
                namesL[i] = name.replace("@", "").strip()        
        
        ##remove lagging numbers
        namesL = [x.rstrip(string.digits).strip() for x in namesL]
        
        ##Clear more specific names  
        namesL = [x for x in namesL if not x.startswith("_")]
        namesL = [x for x in namesL if not x.startswith("-")]
        namesL = [x for x in namesL if not x.endswith(" FAX")]
        namesL = [x for x in namesL if not x.startswith("+ ")]
        namesL = [x for x in namesL if not x.startswith("}")]
        namesL = [x for x in namesL if not x.startswith("^")]
        namesL = [x for x in namesL if not x.startswith("~")]
            
        ##Number followed by space or '
        namesL = [x for x in namesL if not re.match("^\d{1}[\s+\']", x)]
        ##Remove names starting with 5 or more digits
        namesL = [x for x in namesL if not re.match("^\d{5,}", x)]
        ##Remove names starting with a single letter followed by space
        namesL = [x for x in namesL if not re.match("^[A-Za-z]{1}[\s-]+", x)]
        ##Remove any pdf containing names (Amount of small words?)
        namesL = [x for x in namesL if not x.find(' pdf') > -1]
        
        ##make sure no duplicats remain
        namesL = list(set(namesL))        
        ##Sort case independent
        namesL.sort(key = lambda v: v.upper())        
      
        ##Process names by checking if names occurs without leading number
        for i in range(len(namesL)):
            ##get name
            name = namesL[i].strip()
            ##Check if names start with number
            if name[0].isdigit():
                ##remove leading spaces
                name = name.lstrip(string.digits).strip()
                ##Check is name is included in namesL without numbers
                if not name in namesL:
                    namesL[i] = name
                else:
                    ##remove name
                    namesL[i] = ""
        
        ##Clean list
        namesL = [x for x in namesL if not x == ""]    
        ##Sort case independent
        namesL.sort(key = lambda v: v.upper())
        
        ##Process names found
        ##Remove mulriple variants of same word?  bijv. HSE, HSE 20, HSE 20E, HSE 20E RTK, keep HSE 20E RTK
        for i in range(len(namesL)-1):
            if not namesL[i] == "": 
                ##Check if name is part of next name
                if namesL[i].lower() in namesL[i+1].lower():
                    ##Clear first name
                    namesL[i] = ""
                else:
                    ##Check for cityName(s) in text
                    if checkMultipleCityNames(namesL[i]):
                        namesL[i] = ""
                    else:
                        ##Compare cleaned strings
                        name1 = cleanString(namesL[i])
                        name2 = cleanString(namesL[i+1])
                        ##Select which to choose
                        if name1 == name2:
                            if(len(namesL[i]) > len(namesL[i+1])):
                                namesL[i+1] = ""
                            else:
                                namesL[i] = ""
                            
        ##When finished check last record for cityName, if not empty
        if not namesL[-1] == "":
            if checkMultipleCityNames(namesL[-1]):
                namesL[-1] = ""
       
        ##Clean list
        namesL = [x for x in namesL if not x == ""]    
    
    ##return result
    return namesL

##function to extract names of drone companies 
def extractNames(text):
    names = []
    ##Check input
    if len(text) > 0:
        ##0, convert text, first convert to unicode
        text = unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode("utf-8")
        ##Add returns (so the last item in the list is always empty)
        text += "\n\n"
        ##replace text characters to be split
        text = text.replace(":", "\n\n")
        text = text.replace(";", "\n\n")
        text = text.replace("(", "\n\n")
        text = text.replace(")", "\n\n")
        text = text.replace("|", "\n\n")
        text = text.replace("/", "\n\n")
        text = text.replace("\\", "\n\n") ##replace single backslash
        
        ##Standardize -
        text = text.replace(" -", "-")
        text = text.replace("- ", "-")                
        ##Make list, split text on "\n"
        names = text.split("\n")
        del text
        gc.collect()
        
        ##1. Go through text and clean
        ##Combine things with no empty row between them (they belong together)
        ##Prevent copying list                
        i = 0
        word = ""
        while i < (len(names)):     
            ##get content of list position
            w = names[i].strip()
            
            ##Check if any uppercase character occurs
            if not w == "" and not any(s.isupper() for s in w):
                ##Clear w (no relevant text included)
                w = ""
            ##If fully Capital names, check for occurence of any lowercase
            if not w == "" and fullyCapitalNames:
                if any(s.islower() for s in w):
                    w = ""

            ##Four situations can occure
            if w == "" and len(word) == 0:
                ##Clear list position (as it may not be empty)
                names[i] = ""
            elif w == "" and len(word) > 0:
                ##End of name found, add word to list
                word = word.strip()
                ##remove space before a dot
                word = word.replace(".", " ")
                word = word.replace(",", " ")
                ##remove any double spaces, maintain single space
                word = ' '.join(word.split())
                ##remove spaces between single letters
                word = re.sub(genSpace, r'\1', word)            
                ##Add processed words to list
                names[i] = word.strip()
                ##clear word
                word = ""
            elif not w == "" and len(word) == 0:
                ##get words
                word = w
                ##clear position in list
                names[i] = ""
            else: ##words found after non-empty list position
                ##Add w to word
                word += " " + w
                ##clear position in list
                names[i] = ""
            ##net i
            i += 1
            
        ##Check occurence of non-empty word after processing list
        if len(word) > 0:
            ##End of name found, add word to list
            word = word.strip()
            ##remove space before a dot
            word = word.replace(".", " ")
            word = word.replace(",", " ")
            ##remove any double spaces, maintain single space
            word = ' '.join(word.split())
            ##remove spaces between single letters
            word = re.sub(genSpace, r'\1', word)            
            ##Add word to last position in list
            names[-1] = word.strip()
        
        ##Remove all empty and 2 or less letters and shorter than 85 letters list positions
        names = [x for x in names if len(x) > 2 and len(x) < 85]
        
        ##CHeck if list contains abbreviations typical for comapnies in country studied
        ##Use most common used abbreviation to do this (MUST be first in entType list)
        abbr = " " + entType[0]
        abbr = abbr.lower()
        if sum([x.lower().endswith(abbr) for x in names]) >= 5:  ##SPAIN SL, SA SLU
            ##remove non-relevant names (duplicates are removed here as well)
            names = reduceNames(names)    
        else:
            ##Clear list (document does not have any company names)
            names = []
        ##LS check for Spain? ignore if no SL firm is found in document?
        ##names = [x for x in names if x.upper().find("SL") > -1]
        ##Clear memory 
        gc.collect()
        
    ##return result
    return names

def entityCheck(name):
    isEntity = False
    
    ##standardize name?
    nameL = [x.lower().capitalize() for x in name.split()]
    name = " ".join(x for x in nameL)
    del nameL
    
    ##NLP name
    nameNLP = nlp(name)
    count = 0    
    ##Check for entity properties
    for token in nameNLP:
        ##print(token.text + " " + token.ent_type_) ##Types ORG PER MISC LOC and emtpy
        if token.ent_type_ == "ORG" or token.ent_type_ == "PER":            
            count += 1
    
    ##Check if the majortiy of the words are entities (org or persons)
    if count >= (len(nameNLP)/2):
        isEntity = True
        
    del nameNLP
    del name
    return(isEntity)

##Very strict reduction of names
def reduceNames2(names):
    ##1. process whole list with reducenames
    names = reduceNames(names)
    
    ##2. Get obvious company names (end with entity Type abbreviations)
    ##For spain SL SA SLU SLNE
    names2 = []
    ##Extract any obvious company names (ending with SL, SLU etc)
    for j in range(len(entType)):
        ##get abbreviation
        abbr = " " + entType[j].lower()
        ##process names
        for i in range(len(names)):
            ##get name
            name = names[i].lower().strip()
            ##Check for empty
            if not name == "":
                if name.endswith(abbr):
                    ##Check if name also starts with a entType
                    if [name for x in entType if name.startswith(x.lower() + " ")]:
                        ##remove leading EntType (= first word)
                        nameL = name.split()
                        name = " ".join(nameL[1:len(nameL)])
                    ##add name to names2
                    if not name == "" and not name in names2:
                        names2.append(names[i])
                    ##clear name
                    names[i] = ""            
    
    ##Reduce names
    names =[x for x in names if not x == ""]
    
    ##check names with drone synonym in it
    for i in range(len(names)):
        ##get name
        name = names[i].lower().strip()
        ##Check for empty
        if not name == "":
            ##Check if name includes drone acronym
            if any([name for x in drone_words if name.find(x) > -1]):
                ##add name to names2
                if not name in names2:
                    names2.append(names[i])
                ##clear name
                names[i] = ""            
     
    ##Reduce names
    names =[x for x in names if not x == ""]
     
    ##Check remaining with entityCheck
    for i in range(len(names)):
        name = names[i]
        ##Check entity
        if entityCheck(name):
            if not name in names2:
                names2.append(names[i])
            names[i] = ""
    
    ##clear list
    del names
    gc.collect()    
    return names2

##PDF processing function
def processPDF(linksPDF):
    ##init list for links found
    links2 = []
    pdfsread = []
    namesN = []
    
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
        text = ""
        textPdf0 = ""
        textPdf = ""
        
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
                    ##Check returend
                    if len(vurl) > 0:
                        vurl = vurl[0]
                    else:
                        vurl = ""
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
                    textPdf0 = df.getPdfText(vurl, False, number) ##adjust function for multicore download?
                    textPdf = textPdf0.lower()
                    
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
                            textPdf0 = df.getPdfText(vurl, False, number) ##adjust function for multicore download?
                            textPdf = textPdf0.lower()
                        else:
                            textPdf = ''
                else:
                    ##clear vars
                    vurl = ''                    
                        
                ##Check content
                if len(textPdf) > 0:
                    ##When file has been scraped succesfuly add to pdfsread
                    if not nameV in pdfsread: 
                        pdfsread.append(nameV)
                    
                    ##Deal with \n but prevent . (at end of sentence) merges with start of next sentence
                    text = textPdf.replace(".\n", ". ")  
                    ##Correct \n in text (may split urls in 2 parts)
                    text = text.replace("\n", "") 
                    ##Check for country and drone words
                    if any(x in text for x in countryW2) and any(x in text for x in drone_words):
                        ##Count number of drone_words occuring in text?
                        ##Get all urls
                        links = df.extractLinksText(text, genUrl)
                
                        ##Check if mail domains need to be included
                        if mailInclude:
                            ##get mail domains
                            links1 = re.findall(genMail, text)
                            ##keep unique
                            links1 = list(set(links1))
                            ##Combine results
                            links = links + links1                   
                            del links1
                        
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
                        del links
                        
                        ##Check if names should also be extracted
                        if includeNames:
                            ##use raw pdf text NOT LOWERED (also gets url; just in case)
                            names = extractNames(textPdf0)        
                            ## If document does not contain any country specific abbreviatins (suc as SL in spain, etc)
                            if len(names) > 0:                                ##get name s
                                for n in names:
                                    if not n in namesN:
                                        namesN.append(n)
                            ##remove names
                            del names                            
                            
                        ##remove orginal text
                        del text
                        del textPdf
                        del textPdf0
                        gc.collect()
                    else:
                        ##make sure to clean texts
                        del text
                        del textPdf
                        del textPdf0
                        gc.collect()
                else:
                    textPdf = ""
                    textPdf0 = ""
                    gc.collect()
                    
    ##report result
    if len(links2) > 0:
        print(str(len(links2)) + " links extracted")
    if includeNames and len(namesN) > 0:
        print(str(len(namesN)) + " names extracted")
    return links2, namesN


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
        includeNames = config.getboolean('SETTINGS3', 'includeNames')
        fullyCapitalNames = config.getboolean('SETTINGS3', 'fullyCapitalNames')
        cityNameFile = config.get('SETTINGS3', 'cityNameFile')
        countryNames = config.get('SETTINGS3', 'countryNames').split(',')
        entType = config.get('SETTINGS3', 'entityType').split(',')
        
        ##city names file
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(countryW2) > 0 and len(drone_words) > 0 and len(str(runParallel)) > 0 and len(str(mailInclude)) > 0 and len(str(includeNames)) > 0 and len(cityNameFile) > 0 and len(countryNames) > 0:
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
        
    ##Get city names file, if names should be searched for
    if includeNames:
        #get municipalities of NL
        municl = pandas.read_csv(cityNameFile)
        municl2 = list(municl.iloc[:,0])
        ##Remove any leading and lagging spaces
        municL = [x.strip() for x in municl2]
        if municL[0] == '0':
            municL = municL[1:len(municL)]
        ##Add country name
        for name in countryNames:
            if not name in countryNames:
                municL.append(name) 
        
        try:
            ##Make sure spacy works 
            if lang == "en":
                nlp = spacy.load("en_core_web_sm")
            elif lang == "es":
                nlp = spacy.load("es_core_news_sm")
            elif lang == "nl":
                nlp = spacy.load("nl_core_news_sm")
            elif lang == "de":
                nlp = spacy.load("de_core_news_sm")
            elif lang == "it":
                nlp = spacy.load("it_core_news_sm")
        except:
            print("An error occured while loading spaCy language file")
            Continue = False
        
    if Continue:
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
        ##Clear all
        del pdf1
        del pdf2
        del pdf1Content
        del pdf2Content
    
        ##Clean files (deduplicate, preprocess  check country)
        linksNot, totalPDF = df.cleanLinks(pdfAll, country, True)

        ##store results
        print("A total of " + str(len(totalPDF)) + " links found in pdf-list of Scripts 1 and 2")
        f.write("A total of " + str(len(totalPDF)) + " links found in pdf-list of Scripts 1 and 2\n")

        ##2. Process and check PDFs
        links2 = []
        names = []
        ##2a. Check for multicore run
        if cores >= 2:
            ##Process pdfs on multiple cores
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
            ##namesPDF = namesPDF[0:100]
         
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
        
            del totalPDF
            del namesPDF
        
            ##Use all cores to process file
            pool = mp.Pool(cores*multiTimes)
            resultsP = pool.map(processPDF, [list(c) for c in chunks])        
            pool.close()
            pool.join()
    
            ##Add all links to links2 and all names to names in list
            for res in resultsP:
                ##contains two lists from each core, first links, second names
                for link in res[0]:
                    if not link in links2 and not link == "":
                        links2.append(link)
                for name in res[1]:
                    if not name in names and not name == "":
                        names.append(name)
            ##clear result            
            del resultsP
        else:
            ##Use sequential processing method
            print("Sequential PDF extraction started")
            f.write("Sequential PDF extraction started\n")    
            links2, names = processPDF(totalPDF)

        ##Report result
        print(str(len(links2)) + " total number of pdf derived links found")
        f.write(str(len(links2)) + " total number of pdf derived links found\n")

        ##3. Clear and save results of external Links
        ##3a Remove double and obvious links to other countries 
        externalL, externalLNot = df.cleanLinks(links2, country, True)
        ##links will be checked for correct http etc in next script
        del links2
        
        ##3b clean and reduce names files VERY STRICT, with name entity recogniition and more
        if includeNames:
            ##reduce complete list of names
            names = reduceNames2(names)
            ##Sort case independent prior to saving
            names.sort(key = lambda v: v.upper())
    
        ##Show result
        print("Finished extensive urls search")
        f.write("Finished extensive urls search\n")
        print(str(len(externalL)) + " unique external links found")
        f.write(str(len(externalL)) + " unique external links found\n")
        ##Show names findings (if needed)
        if includeNames:
            print(str(len(names)) + " total number of names extracted from pdfs")
            f.write(str(len(names)) + " total number of names extracted from pdfs\n")
        f.close()

        ##3b. Save combined findings as csv's
        fileNameP = "3_externalPDF_"+ country.upper() + lang.lower() + "1.csv"
        totalPDFDF = pandas.DataFrame(externalL)
        totalPDFDF.to_csv(fileNameP, index=False) 
    
        if includeNames:
            fileNameN = "3_externalNames_"+ country.upper() + lang.lower() + "1.csv"
            if len(names) == 0:
                names = ['0']
            totalNames = pandas.DataFrame(names)
            totalNames.to_csv(fileNameN, index=False) 
        
        ##3c. Check for temp files and remove them
        tempF = glob.glob("tempFile*.pdf")
        ##Check and remove if tempFiles are found
        if len(tempF) > 0:
            ##remove files
            for f in tempF:
                os.remove(f)
    
    else:        
        print("Program halted, make sure spaCy works!")

##Finished
