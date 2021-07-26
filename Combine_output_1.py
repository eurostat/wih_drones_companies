#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 09:19:05 2021
Script to search for individual web pages of drone companies
@author: piet
Updated on July 23 2021, version 0.1
"""
##Combine output of Script 1 from individual search engines (links and PDFs)

#Load libraries 
import os
import sys
import glob
import pandas
import configparser

##Set directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Test_ES")

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
else:
    print("Use 'python3 Combine_output_1.py <filename.ini>'") 


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
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0:
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
    Continue = False

##C. Check if programm can be run
if Continue:   
    ##1. Init search query
    print("Checking and combining output of Script 1 for " + str(country.upper()) + " and " + str(lang))

    ##2. Find relevant files
    ##Get external link files (includes main file if present)
    extFiles = glob.glob(os.getcwd() + "/1_external_" +  str(country.upper()) + str(lang.lower()) + "*.csv")
    ##Get pdf files (includes main file if present)
    pdfFiles = glob.glob(os.getcwd() + "/1_totalPDF_" +  str(country.upper()) + str(lang.lower()) + "*.csv")
                      
    ##Check external links found
    extLinks = []
    if len(extFiles) > 0:
        ##Show result
        print(str(len(extFiles)) + " external link files found")
        ##get content
        for file in extFiles:
            ##read file
            f = open(file, "r")
            cont = f.readlines()
            f.close()
            ##Add links
            for i in cont:
                #remove lagging \n
                i = i.replace("\n", "")
                if not i in extLinks and not i == '0':
                    extLinks.append(i)
        ##Sort links
        extLinks.sort()  

        if len(extLinks) > 0:
            ##Show result
            print(str(len(extLinks)) + " unique external links found")        
            ##save file
            fileNameE = "1_external_" + str(country.upper()) + str(lang.lower() + "1.csv")
            extLinksDF = pandas.DataFrame(extLinks)
            extLinksDF.to_csv(fileNameE, index=False) 
            print("File '" +  fileNameE + "' saved")
    else:
        print("No external link files found of script 1")
        
    ##Check pdf links found
    pdfLinks = []
    if len(pdfFiles) > 0:
        ##Show result
        print(str(len(pdfFiles)) + " external PDF-files found")
        ##get content
        for file in pdfFiles:
            ##read file
            f = open(file, "r")
            cont = f.readlines()
            f.close()
            ##Add links
            for i in cont:
                #remove lagging \n
                i = i.replace("\n", "")
                if not i in extLinks and not i == '0':
                    pdfLinks.append(i)
        ##Sort links
        pdfLinks.sort()  

        if len(pdfLinks) > 0:
            ##Show result
            print(str(len(pdfLinks)) + " unique links to PDF-files found")        
            ##save file
            fileNameP = "1_totalPDF_" + str(country.upper()) + str(lang.lower() + "1.csv")
            pdfLinksDF = pandas.DataFrame(pdfLinks)
            pdfLinksDF.to_csv(fileNameP, index=False) 
            print("File '" +  fileNameP + "' saved")
    else:
        print("No external link PDF-files found of script 1")


print("Finished")
## END ############################################################################
