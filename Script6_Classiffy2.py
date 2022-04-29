#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 27 12:56:50 2021
Feb 23 2021 version 0.2 script that classifies texts in csv file with model trained on detecting Drone companies (in English) 
@author: piet
##Script to classify scraped websites dor Spain, Irleand and Italy, and in all other cases English websites only
"""

import os
import sys
import pandas
import multiprocessing as mp
import numpy as np
##import configparser
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer, CountVectorizer
import sklearn.model_selection 
import sklearn.metrics
from sklearn.linear_model import LogisticRegression
import time

##Get directory
##os.chdir("/home/piet/R/Drone/Ini_scripts/Classify")
localDir = os.getcwd()
##set modelDir 
modelDir = localDir + "/Model_LG2/"
##Define empty translation file location (only implemented for Italian at the mment)
translateFile = ""
transFileLoc = ""

##Functions
##Define functions
def getText(df):
    text = []
    for file in df['text'].tolist():
        if isinstance(file, str):
            text.append(file.split())
        else:
            text.append(" ")
    return(text)

def addlanguagefeature(X, dataset):
    taal = dataset['lang'].tolist()
    language_vector = []
    for item in taal:
        if item=="spanish" or item=="italian":   ##May need to adust language to native language of country studied
            language_vector.append(0)
        else:
            ##If english set 1
            language_vector.append(1)
    language_vector = np.array(language_vector, ndmin=2, dtype="float64").T
    X = np.c_[X, language_vector]
    return(X)
    
def addDroneFeatures(Comb):    
    ##Add text depended features, as 0 or 1
    Comb['dronF'] = [int(x) for x in Comb['text'].str.contains("drone", case=False)] ##looks the best instead of dron
    Comb['rpasF'] = [int(x) for x in Comb['text'].str.contains("rpas", case=False)]
    Comb['uasF'] = [int(x) for x in Comb['text'].str.contains(" uas ", case=False)]
    Comb['uavF'] = [int(x) for x in Comb['text'].str.contains(" uav ", case=False)]    
    ##Add drone features in url, as 0 and 1
    Comb['dronU'] = [int(x) for x in Comb['url'].str.contains("dron", case=False)]
    Comb['rpasU'] = [int(x) for x in Comb['url'].str.contains("rpas", case=False)]
    Comb['uasU'] = [int(x) for x in Comb['url'].str.contains("uas", case=False)]
    Comb['uavU'] = [int(x) for x in Comb['url'].str.contains("uav", case=False)]
    
    return(Comb)

##Function to replace words in text (ONLY USED FOR ITALIAN texts at the moment)
def replaceModel(df, translateFile = transFileLoc):
    ##get translateFile
    trans = pandas.read_csv(translateFile, sep = ";")
    ##count spaces
    trans['nr_spaces'] = 0
    trans['len_w'] = -1
    for i in range(trans.shape[0]):
        ##get italian word
        word = trans.iloc[i, 2]
        cnt = word.count(" ")
        if cnt > 0: 
            trans.iloc[i, 4] = cnt
        trans.iloc[i, 5] = len(word)
    
    ##sort file
    trans = trans.sort_values(["nr_spaces", "len_w"], ascending=False)

    ##create lists   
    trans1 = list(trans.iloc[:,1])
    trans2 = list(trans.iloc[:,2])
    
    ##get columnNames ofdf
    columnNames = list(df.columns)
    ##Get index of text colum    
    txt = columnNames.index('text')
        
    ##translate words in column2 with this in column
    for i in range(df.shape[0]):
        ##get text
        text = df.iloc[i, txt]
        
        ##translate
        if len(text) > 0:
            ##Convert to lower and add spaces at begin and end
            text2 = " " + text.lower() + " "
            ##Check if text need to be replaced
            for j in range(len(trans2)):
                ##get w2 
                word2 = trans2[j]
                word2 = " " + word2 +  " "
                ##Check if it occurs
                if text2.find(word2) > -1:
                    word1 = " " + trans1[j] + " "
                    text2 = text2.replace(word2, word1)
                    text2 = text2.replace(word2, word1)
            
            ##when finished
            text2 = text2.strip()
            
            if not text.lower() == text2:
               ##replace test
                df.iloc[i, txt] = text2
            
            text2 = ""
            text = ""
    
    return(df)

### START #####################################################################
Continue = False
fileName = ''
##endLanguage = 'eng'
runParallel = True

##A. Check input variables
if len(sys.argv) > 1:
    ##additional input provided
    if len(sys.argv) >= 2:
        fileName = str(sys.argv[1])
        fileName1 = localDir + "/" + fileName
        if not os.path.isfile(fileName1):
            print("File " + fileName1 + " does NOT exists")
        else:
            ##Check additional optional input
            if len(sys.argv) >= 3:
                ##Get second optinal argumant (must be translate page)
                translateFile = str(sys.argv[2])
                transFileLoc = modelDir + translateFile
                if not os.path.isfile(transFileLoc):
                    print("File " + transFileLoc + " does NOT exists")
                else:
                    Continue = True
            else:
                translateFile = ""
                ##Continue and load Ini file
                Continue = True
else:
    print("Use 'python3 Script6_Classify2.py <filename.csv> OPTIONAL<translate_file> to run program") 
    print("Make sure model pickle files (3 in total) are located in /Model_LG2 subdir (and optionally the tranlation file)")
   
##B. load ini-file if Continue is True
if Continue and not fileName == '':

    ##get vars and values
    try:
        ##def country and lang
        country = ""
        lang = ""
        
        ##Check file name provided
        if fileName.find("_ES_") > -1:
            country = "es"
            lang = "es"
        elif fileName.find("_IE_") > -1:
            country = "ie"
            lang = "es"
        elif fileName.find("_IT_") > -1:
            country = "it"
            lang = "it"
        else:
            ##end program
            Continue = False
            print("File of unknown country. Please make sure the filename includes ES, IE or IT")
    except:
        ##An erro has occured
        print("An error has occured while checking input file")
        print("Check input provided and its content and retry")
        Continue = False
        ##Check vars needed
else:
    print("Please provide a valid filename")

##Continue if all is well
if Continue:
    try:    
        ##1. get data
        Data = pandas.read_csv(fileName1, sep = ";")
        ##deal with nan in text
        Data['text'] = Data['text'].fillna("")
        ##get colums
        columnNames = list(Data.columns)
        if 'inCountry' in columnNames:
            Data['inCountry'] = Data['inCountry'].fillna("None")

        ##remove emtpty records
        Data = Data.dropna()
        
        ##Check if onlu english texts need to be selected
        if not country.upper() == "ES" and not country.upper() == "IT" and not country.upper() == "IE":
            ##Only keep english texts in file
            if 'language' in columnNames:
                ##rename language columnae to lang
                Data = Data.rename(columns={'language': 'lang'})
            ##Only keep english part of texts
            Data = Data[Data["lang"] == "en"]
        
        ##Reste index
        Data = Data.reset_index(drop=True)

        ##2. Remove text with <10 words
        for i in range(Data.shape[0]):
            text = Data.iloc[i].at['text']
            textL = text.split()
            if len(textL) < 10:
                Data.iloc[i].at['text'] = ""
            
        ##3. Rename language column to lang (required to be included as feature)
        if 'language' in columnNames:
            ##rename language columnae wth lang
            Data = Data.rename(columns={'language': 'lang'})
        if 'org_url' in columnNames:
            ##rename to url
            Data = Data.rename(columns={'org_url': 'url'})

        ##4. Replace text (ONLY FOR ITALY AT THE MOMENT)
        if not translateFile == "":
            ##get cores of machine used
            cores = mp.cpu_count()
            ##create refernce to translation file
    
            ##Check for paralle processing
            if runParallel and cores > 1:
                ##split links list in chunks
                chunks = np.array_split(Data, cores, axis = 0)
   
                ##Use all cores to process file
                pool = mp.Pool(cores)
                resultP = pool.map(replaceModel, [c for c in chunks])
                time.sleep(5)    
                pool.close()
                pool.join()
                ##Combine to new dataframe
                Data = pandas.concat(resultP)
            else:
                ##Not applicable for Ireland
                Data = replaceModel(Data, transFileLoc)

        else:
                ##Translate file is not found
                print("Translate file provided does not exists! (Must be included in ./Model_LG2 map)")
                Continue = False
    except:
        print("An error occured, program halted")
        
##Apply model
if Continue and Data.shape[0] > 0:

    ##Check if data is available
    try:
        ##Add drone features in text, as 0 and 1
        Data = addDroneFeatures(Data)
        
        ##2. Load pickles of trained classifier in binairy format
        ##get algoritm (trained LG l2 algol
        file = open(modelDir + "algLG2Drone5.sav", 'rb')
        alg = pickle.load(file)
        file.close()
      
        file = open(modelDir + "cvLG2Drone5.sav", 'rb')
        cv = pickle.load(file)
        file.close()

        file = open(modelDir + "tfidfLG2Drone5.sav", 'rb')
        tfidfvectorizer = pickle.load(file)
        file.close()     

        ##3. Process text so it can be classified
        ##Check if model works
        word_count_vector_C = cv.transform(Data['text'].tolist())
        c2 = tfidfvectorizer.transform(word_count_vector_C)
        ##Create droneFeature array
        dronF_vectorC = np.array(Data['dronF'], ndmin=2, dtype="float64").T
        rpasF_vectorC = np.array(Data['rpasF'], ndmin=2, dtype="float64").T
        uasF_vectorC = np.array(Data['uasF'], ndmin=2, dtype="float64").T
        uavF_vectorC = np.array(Data['uavF'], ndmin=2, dtype="float64").T
        dronU_vectorC = np.array(Data['dronU'], ndmin=2, dtype="float64").T
        rpasU_vectorC = np.array(Data['rpasU'], ndmin=2, dtype="float64").T
        uasU_vectorC = np.array(Data['uasU'], ndmin=2, dtype="float64").T
        uavU_vectorC = np.array(Data['uavU'], ndmin=2, dtype="float64").T
        ##combine to matrix   
        c1 = np.c_[c2.toarray(), dronF_vectorC, rpasF_vectorC, uasF_vectorC, uavF_vectorC, dronU_vectorC, rpasU_vectorC, uasU_vectorC, uavU_vectorC]
        c = addlanguagefeature(c1, Data)

        ##Apply model and predict proba
        ypred_prob = alg.predict_proba(c)
        ##Apply model and predict binary
        ypred = alg.predict(c)

        ##Add to data_frame
        Data['pred_prob'] = ypred_prob[:,1] ##Only store chance on Drone website (column nr 1)
        Data['prediction'] = ypred

        print("A total of " + str(sum(ypred)) +  " Drone companies detected")

        ##Save file
        outputName = fileName1.replace(".csv", "_pred.csv") ##ADJUST NAME 
        ##outputName = fileDir + "/" + outputName
        Data.to_csv(outputName, sep = ';', index=False)
        
        print("Data classified, file saved and program ended")
        
    except:
        print("An error occured, program halted")
else:
    print("program ends")
    
        
        


