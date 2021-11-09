#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 12:56:50 2021
Nov 9 2021 version 0.1 translate words in text output of Script4b_iniS2.py from native language to English
Requires apertium installed on Linux system (see https://apertium.org/)  
@author: piet
"""

import os
import sys
import pandas
##import apertium
import nltk
import multiprocessing as mp
import numpy as np
import configparser
import subprocess
import re


##Get directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Translate")
localDir = os.getcwd()

##Functions

##Functions that removes majority of Korean, Chines amnd japanse characters from text (translation halts on large numbers of that)
def cjk_remove(text):
    # korean
    if re.search("[\uac00-\ud7a3]", text):
        ##remove them
        text = re.compile("[\uac00-\ud7a3]", re.UNICODE).sub('', text)
    # japanese
    if re.search("[\u3040-\u30ff]", text):
        ##remove them
        text = re.compile("[\u3040-\u30ff]", re.UNICODE).sub('', text)
    # chinese
    if re.search("[\u4e00-\u9FFF]", text):
        ##remove them
        text = re.compile("[\u4e00-\u9FFF]", re.UNICODE).sub('', text)
    ##remove double spaces
    text = " ".join(text.split())
    return text

##Capatilize function (makes first letter into a Capital)
def capat(text):
    textL = text.split()
    textL = [w.capitalize() for w in textL if not textL == ""]
    text = " ".join(textL)
    del textL
    return(text)

##get stopwords english max length allowed
def translateMax(text, lang1, lang2):
    ##remove lead and lag spaces (just in case)
    text = text.strip()
    
    ##Check input (must contain something)
    if len(text) > 0:
        ##set max length of text allowed to be translated by a single apertium request
        n = 125000 ##more text will be ignored (prevents translation getting stuck on it)
        
        ##Check for max length allowed
        if len(text) > n:
            ##get position of last space below max length (n)
            end = text[0:n].rfind(' ') 
            ##cut out text
            text = text[0:end]
            
        ##use apertium via command line
        cmd = 'echo "' + text + '" | apertium -u ' + lang1 + '-' + lang2
        proc = subprocess.Popen(cmd, universal_newlines=True, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        o, e = proc.communicate()
        
        ##Remove enter at end of string ("/n")
        if len(e) < 2:
            ##remove retrun at end
            text = o[0:-1]
            text = text.strip()
        else:
            text =  "ERROR IN APERTIUM CMD!"
        
        ##clear memory
        del cmd
        del proc
        del o
        del e
    
    ##After translation always retrun lowercase         
    return text.lower()

##En this is the multicore function, that has dataframe as inut
def translateM(df):
    ##process dataframe
    
    ##define stopwordsList (once)
    stopwordsList = nltk.corpus.stopwords.words('english')
    
    for i in range(df.shape[0]):
        ##if i % 200:
        ##    print(i)
        
        ##get text from frame (MUST be column 4)
        text = df.iat[i,4]
        text = text.strip()
        
        ##Check text for processing
        if not text == "":        
            ##1. remove cjk signs
            text = cjk_remove(text) 
            
            ##1b Capatiliza all words
            ##text = capat(text)
            
            ##2. translate text
            text = translateMax(text, startLanguage, endLanguage)        
            
            ##3. remove english stopwords        
            textL = text.split()
            ##Remove english stopwords
            textL = [w for w in textL if not w.lower() in stopwordsList]
            ##rejoin text (single spaced)
            text = " ".join(w for w in textL if not w == "")                 
            
            del textL
            
            ##4. Include in dataframe
            df.iloc[i,6] = text.lower()
            
            del text
    
    del stopwordsList
    
    return(df)

### START #####################################################################
Continue = False
fileName = ''
endLanguage = 'eng'

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
    print("Use 'python3 Script5_Translate_Ini.py <filename.ini> to run program") 

##B. load ini-file if Continue is True
if Continue and not fileName == '':
    ## LOAD SETTING 
    ##enable ini file and load
    config = configparser.ConfigParser()
    config.read(fileName)

    ##get vars and values
    try:
        country = config.get('MAIN', 'country')
        lang = config.get('MAIN', 'lang')
        startLanguage = config.get('SETTINGS5', 'startLanguage')
        runParallel = config.getboolean('SETTINGS5', 'runParallel')
        
        print("Ini-file settings loaded")
        
        ##Check if vars are all available
        if len(country) > 0 and len(lang) > 0 and len(startLanguage) > 0 and len(str(runParallel)):
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

##Continue if all is well
if Continue:
    ##Check if apertium is active
    try:
        ##Test apertium translator
        text = "Este texto se utiliza simplemente para comprobar que la apertura está activa en el sistema actual"
        res = translateMax(text, startLanguage, endLanguage)

        if len(res) > 0 and not res == 'error in apertium cmd!':
            ##Everything seems to work, check if file name exists
            ##and get data, construct filename
            
            fileName1 = "4_Result_" + country.upper() + lang.lower() + "_1.csv"
            if os.path.isfile(localDir + "/" + fileName1):
                ##get data
                data = pandas.read_csv(fileName1, sep = ";")
                ##remove all records without texts
                data.dropna(subset = ["text"], inplace=True) ##36257 rows remaining
                ##remove records with empty text
                ##ADd english column to loaded dataframe
                data['text_en'] = ""

            else:
                print("File " + fileName1 +  " not found, program halted")
                Continue = False
        else:
            Continue = False
            print("An error occured while applying apertium translation, please check if the software is correctly installed")
            print("Program ended")

    except:
        ##An error has occured
        Continue = False
        print("An error occured while apllying apertium translation, please check if the software is correctly installed")
        print("Program ended")

##If all went well        
if Continue:             
    ##get number of cores
    cores = mp.cpu_count() ##get number of cores (threads) on machine

    if not runParallel:
        cores = 1
  
    ##process in parallel (RECOMMENDED)
    if cores > 1:
        ##split dataframe in chunks
        chunks = np.array_split(data, cores, axis = 0)
    
        pool = mp.Pool(cores)
        results = pool.map(translateM, [c for c in chunks])
    
        ##Add results of each core in single frame 
        frames = []
        for res in results:
            frames.append(res)
    
        pool.close()
        pool.join()
        
        ##Combine results to 1 single pandas dataframe
        data2 = pandas.concat(frames)
        
    else:
        ##singel core processing 
        data2 = translateM(data)
        
    ##Process data2 frame created
    #Sort dataframe accoring to orihinal urls
    data2 = data2.sort_values(by=['org_url'])
    
    ##Convert en translated daraframe
    data2 = data2.drop('text', 1)
    data2 = data2.rename(columns={'text_en': 'text'})
       
    ##save result
    fileName2 = "4_Result_" + country.upper() + lang.lower() + "_1T.csv"  
    data2.to_csv(fileName2, sep = ";", index = False)
     
    
    ##replace words
    ## produci  production
    ## ingenier\s engineer\s
    ## \sgesti\s \sgestured\s
    
    ## \sformaci\s \straining\s
    
    ## \s pol tica\s  \spolitics\s
    ## \stics\s  \sethics\s
    ##?tica privacy NOT yet
    
    ## ALso additional English stopword removal (of and the are problems)
    
    ##eglish stopword removal
    
    ##get stopwords english
    
    print("Text in file translated, program finished")
    
