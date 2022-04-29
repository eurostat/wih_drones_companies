#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 12:56:50 2021
Feb 18 2021 version 0.3 scripts that combines output of Script 4b for both languages 
@author: piet
"""

import os
import sys
import pandas
import configparser

##Function to cut out domain name of an url (wtith our without http(s):// part)
def getDomain(url, prefix = False):
    top = ""
    if len(url) > 0:
        if url.find("/") > 0 and url.startswith("http"):
            try:##remove any ? containing part (may be added to url)
                url = url.split('?')[0]
                ##Split url
                res = url.split("/")
                ##Check length of result (should at least be three 1. http: 2. //  3. domain.name
                if len(res) > 2:
                    top = res[2]
                    ##add prefix
                    if prefix:
                        top = res[0] + '//' + top
                else:
                    top = url
                    
                ##remove www
                top = top.replace("www.", "")
                
            except:
                ##An error occured, url is likely not composed as it should be
                top = ""
        else:
            top = url
    return(top)
    
##funtion to process and stadardize pandas dataframe
def processDF(df):
    
    ##Clear inCountry and text
    df['inCountry'] = df['inCountry'].fillna("")
    df['text'] = df['text'].fillna("")

    ##Clear dataframes, remove NOT in Country and empty text
    df.drop(df.loc[df['inCountry'] == False].index, inplace=True)
    df.drop(df.loc[df['text']==""].index, inplace=True)
    df = df.reset_index(drop=True)

    ##keep limited number of vars
    df = df[['org_url', 'url_visited', 'inCountry', 'language', 'text', 'action']]

    return(df)

###############################################################################
##Get directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Test_ES3")
localDir = os.getcwd()

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
    print("Use 'python3 Script5_Merge.py <filename.ini> to run program") 

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
            
            ##Check language used as input
            if lang == "en":
                if country == "es":
                    lang = "es"
                elif country == "nl":
                    lang = "nl"
                elif country == "de":
                    lang = "de"
                elif country == "it":
                    lang = "it"
            
            ##for ireland always use english
            if country == "ie":
                lang = "en"
                
        else:
            ##stop
            print("Invalled input file provided, check country and lang settings")
            Continue = False
                
    except:
        ##An erro has occured
        print("An error has occured while reading ini-file")
        print("Check ini-files content and retry")
        Continue = False
        ##Check vars needed
else:
    print("Please provide a valid filename")
    Continue = False

##Check if continued
if Continue:
    
    ##construct filenames
    fileName1 = localDir + "/4_Result_" + country.upper() + lang.lower() + "_1.csv"
    if not country == "ie":
        fileName2 = localDir + "/4_Result_" + country.upper() + "en" + "_1.csv"
    else:
        fileName2 = ""
     
    Continue = False
    ##Check if files are present
    if os.path.isfile(fileName1):
        if not fileName2 == "":
            if os.path.isfile(fileName2):
                ##all OK
                Continue = True
        else:
            if country == "ie":
                Continue = True
            else:
                print("Second filename " + str(fileName2) + " does not exists, program halted")
    else:
        print("File " + str(fileName1) + " does not exist, program halted")
    
    ##Read files
    if Continue:
        ##get first files
        data1 = pandas.read_csv(fileName1, sep = ";")
        data1 = processDF(data1)

        if not fileName2 == "":
            ##Get secobd file
            data2 = pandas.read_csv(fileName2, sep = ";")
            data2 = processDF(data2)

            ##get org_url of dataframe 1 as list
            urlList = list(data1['org_url']) ##33405
            ##Remove identical urls from data2
            for i in range(data2.shape[0]):
                ##get url
                url = data2.loc[i, 'org_url']
                if url in urlList:
                    ##Clear text field
                    data2.loc[i, 'text'] = ""
                    
            ##remove records with empty text fields
            data2.drop(data2.loc[data2['text']==""].index, inplace=True)
            data2 = data2.reset_index(drop=True)
           
            ##Combine Data1 and Dat2b
            frames = [data1, data2]
            ##frames = [urls_found1, urls_found3]
            dataComb = pandas.concat(frames, sort=False, ignore_index=True)
        else:
            ##Copy first dataset
            dataComb = data1.copy()
        
        ##Clean Data
        dataComb = dataComb.reset_index(drop=True)

        ##Domain related checkking
        dataComb['domain'] = ""
        dataComb['len_url'] = -1        
        ##Remove identical urls from data2
        for i in range(dataComb.shape[0]):
            dom = ""
            ##get url
            url = dataComb.loc[i, 'org_url']
            ##get domain
            dom = getDomain(url)
    
            if not dom == url:
                ##Clear text field
                dataComb.loc[i, 'domain'] = dom
    
            ##get length of url
            dataComb.loc[i, 'len_url'] = len(url)

        ##Select shortest url in same domain
        doms = list(set(dataComb['domain']))
        doms.sort()
        dataComb['select'] = 0
        ##Go thorugh doms
        for i in range(len(doms)):
            ##print(i)
            ##get dom
            dom = doms[i]    
            ##get df
            df = dataComb[dataComb['domain'] == dom]
            ##sort records occadring to length of url
            df = df.sort_values(['len_url'], ascending=True)
    
            ##Deal with Unknon locations?
    
            ##get url of first records and select
            url = list(df['org_url'])[0]
    
            ##select record in dataComb with this url, and select it
            dataComb.loc[dataComb['org_url'] == url, 'select'] = 1

        ##Make a copy and select selected records
        dataComb1 = dataComb.copy()
        dataComb1.drop(dataComb1.loc[dataComb1['select']==0].index, inplace=True)
        dataComb1 = dataComb1.reset_index(drop=True)

        ##sort records occadring to length of url
        dataComb1 = dataComb1.sort_values(['org_url'], ascending=True)
    
        ##Save file
        outputName = localDir + "/4_Result_" + country.upper() + "_2F.csv" ##ADJUST NAME 
        ##outputName = fileDir + "/" + outputName
        dataComb1.to_csv(outputName, sep = ';', index=False)
        
        print("File saved, program ended")

else:
    print("Program ended")      