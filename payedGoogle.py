#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 29 12:42:08 2021

@author: Piet Daas
Updated on July 30 2021, version 1.01
"""
##Separat google scrape via PAID value SERP account
##Set correct API_KEY prior to use in code that import this file
##import payedGoogle as pg
##pg.__api_key__ = "KEYVALUE"

##variable for api_key to acces Google payed service
_api_key_ = ""

import requests
import re
import time

##Function to check if _api_key_ is set
def getKey():
    if not _api_key_ == "":
        print(_api_key_)
    else:
        print("No api_key assigned")
        print("Do this by payedGoogle._api_key = 'KEYVALUE'")

##Google payed scrape function (set api_key prio to use)
def queryGoogleV(query, country, waitTime = 0, limit = 0):
    ##Check if api_key has a value
    if not _api_key_ == "":
        urls_found = []
    
        # set up the request parameters
        params = {
            'api_key': _api_key_,
            'q' : query,
            'google_domain' : 'google.' + country,
            'filter' : '0'
        }

        ## Do first requests
        api_result = requests.get('https://api.valueserp.com/search', params)

        ## Extract links and next page from JSON resultP
        result = api_result.json()

        ##Check if all worked well
        res = result['request_info']
                
        ##Check for succes
        if res['success']:
            try:
                ##Get organic_results part
                result2 = result['organic_results']
                ##get links
                for i in result2:
                    ##get link
                    link = i['link']
                    if not link in urls_found:
                        urls_found.append(link)
                
                ##show progress
                print(str(len(urls_found)) + " links found Google payed")
        
                ##get pagination
                pages = result['pagination']
                ##Get next page url
                nextPage = pages['next']    
                ##get number
                pageNum = _getStartNumber([nextPage])

                time.sleep(waitTime)
        
                ##Continue scraping
                while not pageNum == 0: 
                    ##Chcek results
                    if limit == 0 or (limit > 0 and limit > len(urls_found)):
                        ##Continue scraping
                        ##print(pageNum)
                        ##print(nextPage)
                
                        ## set up the request parameters
                        params = {
                            'api_key': _api_key_,
                            ##'q' : query,
                            'url' : nextPage,
                            'google_domain' : 'google.' + country,
                            'filter' : '0'
                        }

                        ## Do requests
                        api_result = requests.get('https://api.valueserp.com/search', params)

                        ## Extract links and next page from JSON resultP
                        result = api_result.json()
                
                        ##Check if all worked well
                        res = result['request_info']
                
                        ##Check for succes
                        if res['success']:
                            ##Get organic_results part
                            result2 = result['organic_results']
                            ##get links
                            for i in result2:
                                ##get link
                                link = i['link']
                                if not link in urls_found:
                                    urls_found.append(link)

                            ##show progress
                            print(str(len(urls_found)) + " links found Google payed")
                
                            ##get pagination
                            pages = result['pagination']
                    
                            ##Error prone part
                            try:
                                ##Get next page url
                                nextPage = pages['next']
                                ##get pagenumber (will be 0 if none is found)
                                pageNum = _getStartNumber([nextPage]) 
                            except:
                                ##print("End reached")
                                ##print(pages)
                                ##end scrape
                                pageNum = 0
                            finally:
                                time.sleep(waitTime)
                        else:
                            ##Scraping failed, check reason
                            if res['topup_credits_remaining'] <= 0:
                                print("Credit has become 0, pay for new queries")
                                pageNum = 0
                            else:
                                print("trying agian")
                                print(nextPage)
                        
                    else:  ##Number of links found
                        print(str(len(urls_found)) + " links found Google payed")
                        if limit < len(urls_found):
                            urls_found = urls_found[0:limit]
                        pageNum = 0
            except:
                print("An error occurred") ##organic_results did not exists
        else:
            if res:
                ##Scraping failed, check reason
                if res['topup_credits_remaining'] <= 0:
                    print("Credit has become 0, pay for new queries")
                else:
                    print("An error occured")
            else:
                print("No access to API")
        return(urls_found)    
    else:
        print("Provide an _api_key_")
        
##Get number of start option in Google url    
def _getStartNumber(urls):
    num1 = 0
    try:
        num = []
        for link in urls:
            if link.find('start') > 0:
                ##get number after start
                begin = link.find('start')
                text = link[begin:len(link)]
                number = re.findall('[0-9]+', text)[0]
                number = int(number)
                num.append(number)
        ##get max
        num1 = max(num)
    except:
        ##An error occured
        num1 = 0
    finally:        
        return(num1)

##Create multprocess function of payed Google
def queryGoogleVmp(query, country, outputQueue, limit = 0, waitTime = 0):
    ##Check if api_key has a value
    if not _api_key_ == "":
        links = []
        try:
            ##get results
            links = queryGoogleV(query, country, waitTime, limit)
        except:
            ##Ann error occured
            pass
        finally:        
            outputQueue.put(links)
    else:
        print("Provide an _api_key_")

##Function to cut out domain name of an url (wtith our without http(s):// part)
def _getDomain(url, prefix = True):
    top = ""
    if len(url) > 0:
        if url.find("/") > 0:
            ##remove any ? containing part (may be added to url)
            url = url.split('?')[0]
            ##Get domain name
            top = url.split('/')[2]
            ##add prefix
            if prefix:
                top = url.split('/')[0] + '//' + top
        else:
            top = url
    return(top)

##Google payed scrape function (set api_key prio to use), to collect ONLY links on first page (no further scraping)
def _queryGoogleV2(query, country, waitTime = 0, limit = 10):
    ##Check if api_key has a value
    if not _api_key_ == "":
        urls_found = []
    
        # set up the request parameters
        params = {
            'api_key': _api_key_,
            'q' : query,
            'google_domain' : 'google.' + country,
            'filter' : '0'
        }

        ## Do first requests
        api_result = requests.get('https://api.valueserp.com/search', params)

        ## Extract links and next page from JSON resultP
        result = api_result.json()

        ##Check if all worked well
        res = result['request_info']
                
        ##Check for succes
        if res['success']:
            try:
                ##Get organic_results part
                result2 = result['organic_results']
                ##get links
                for i in result2:
                    ##get link
                    link = i['link']
                    if not link in urls_found:
                        urls_found.append(link)
                
                ##show progress
                print(str(len(urls_found)) + " links found on first page of Google payed")
            except:
                print("An error occurred") ##organic_results did not exists
        else:
            if res:
                ##Scraping failed, check reason
                if res['topup_credits_remaining'] <= 0:
                    print("Credit has become 0, pay for new queries")
                else:
                    print("An error occured")
            else:
                print("No access to API")
        return(urls_found)    
    else:
        print("Provide an _api_key_")


##Function to find links to specific pdf file in a domain (link may change)
def searchPDFlink(url, country):
    vurl = ""
    
    if url.lower().find('pdf') > 0:
        ##process links
        ##Get domain with first part included
        dom = _getDomain(url, False)
        ##Get pdf part
        res = url.split("/")
        pdf = ''
        for r in res:
            if r.lower().find('pdf') > 0:
                pdf = r
                break
            
        ##Check to continur
        if len(dom) > 0 and len(pdf) > 0:
            ##Construct query
            query = dom + " " + pdf
            ##Get links on first page (limits number of requests)                
            links = _queryGoogleV2(query, country, 0, 10)
            ##Check links, return first links woth dom and pdf included
            for l in links:
                if l.lower().find(dom) > 0 and l.lower().find(pdf) > 0:
                    vurl = l
                    break
        
    return(vurl)
