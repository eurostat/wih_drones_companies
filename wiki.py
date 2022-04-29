### Keyword scraping from wikipedia (in progress...)
###
### approach:
### - determine word frequencies of certain Wikipedia articles (in this case drones)
### - retrieve general frequncies of those words (using Exquisite Corpus via wordfreq)
### - compare them (using the frequency ratio) in order to find topic-specific keywords


# https://www.freecodecamp.org/news/scraping-wikipedia-articles-with-python/
# https://levelup.gitconnected.com/two-simple-ways-to-scrape-text-from-wikipedia-in-python-9ce07426579b
# https://www.datacamp.com/community/tutorials/stemming-lemmatization-python

import requests
from bs4 import BeautifulSoup
import nltk
from nltk.stem import PorterStemmer
from nltk.stem import LancasterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
import re
import wordfreq
import pandas as pd
import numpy as np
import sys

# Extracts keywords from a website
#
# Arguments:
# - url url
# - min_count minimum word count required to be in included in the list
# - filename filename of the csv file
# - language language (in lowercase, known by nltk)
#
# Method
# - words are collected, stopwords removed
# - per word, relative frequency is calculated (freqSrcDoc): occurrences / total words
# - per word, relative frequency in the Exquisite Corpus (https://github.com/LuminosoInsight/exquisite-corpus, used by wordfreq) is calculated (freqExCorp)
# - words are stemmed
# - words are grouped per stem (freqSrcDoc and freqExCrop are summed)
# - per stem, freqRatio = freqSrcDoc / freqExCrop is calculated. Example: for the word 'applic' the freqRatio is 31. This means that the unstemmed variants ('application', 'applications', 'applicable') occur 31 times more in this website than in the corpus.
#
# Output: csv table with the following columns:
# - stem
# - words (the unstemmed words, sorted by occurrence in the corpus (descending))
# - count
# - freqSrcDoc
# - freqExCorp
# - freqRatio
def extract_keyword_profile(url, min_count=5, filename=None, language='english'):
    response = requests.get(url=url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # not needed?
    #title = soup.find(id="firstHeading")
    #print(title.string)

    # Extract the plain text content from paragraphs
    paras = []
    for paragraph in soup.find_all('p'):
        paras.append(str(paragraph.text))

    # Extract text from paragraph headers
    heads = []
    for head in soup.find_all('span', attrs={'mw-headline'}):
        heads.append(str(head.text))

    # Interleave paragraphs & headers
    # text = [val for pair in zip(paras, heads) for val in pair]

    # Combine heads and paragraphs
    text = heads + paras
    text = ' '.join(text)

    # Drop footnote superscripts in brackets
    text = re.sub(r"\[.*?\]+", '', text)

    # Replace '\n' (a new line) with ''
    text = text.replace('\n', '')
    # print(text)

    nltk.download('punkt',quiet=True)
    nltk.download('stopwords',quiet=True)

    words = word_tokenize(text, language=language)
    words = [word.lower() for word in words]
    words = [word for word in words if not word in stopwords.words(language)]
    words = [word for word in words if word.isalnum() and not word.isnumeric()]

    stemmer = nltk.stem.SnowballStemmer(language)

    # find word stems (one for each word)
    wstems = [stemmer.stem(word) for word in words]

    # put in a dictionary (one item per stem)
    nested_stems = {}
    for stem in np.unique(wstems):
        nested_stems[stem] = [words[i] for i in range(len(wstems)) if wstems[i] == stem]

    # remove duplicates for each item
    nested_stems2 = {}
    for key, value in nested_stems.items():
        nested_stems2[key] = list(np.unique(value))

    # put in a list
    nested_stems3 = []
    for n in nested_stems2.values():
        nested_stems3.append(n)

    # count words and put in dictionary
    freq = []
    for w in words:
        freq.append(words.count(w))
    d = dict(zip(words, freq))
    d = {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse=True)}
    df = pd.DataFrame(list(d.items()), columns=['word', 'count'])


    df['stem'] = [stemmer.stem(word) for word in df['word']]
    df['freqExCorp'] = [(wordfreq.word_frequency(k, 'en')) for k, v in d.items()]
    df['freqSrcDoc'] = df['count'] / sum(df['count'])
    df2 = df.groupby('stem', as_index=False).sum()

    allequal = all(map(lambda x, y: x == y, df2['stem'].to_list(), list(nested_stems2.keys())))
    if not allequal:
        sys.exit("stems not equal")

    # order words per stem-group (starting with the most popular)
    nested_stems4 = []
    for wrds in nested_stems3:
        wrds_freq=[df[df['word']==w]['freqExCorp'].to_list()[0] for w in wrds]
        wrds_freq_rev=[w * -1 for w in wrds_freq]
        ind=np.argsort(wrds_freq_rev)
        wrds2 = [wrds[i] for i in ind]
        nested_stems4.append(wrds2)

    df2['words'] = nested_stems4

    df2['freqRatio'] = df2['freqSrcDoc'] / df2['freqExCorp']
    df2 = df2.sort_values(by=['freqRatio'], ascending=False)

    df2 = df2[(df2['count'] > min_count) & (df2['freqExCorp'] > 0)]

    df2.reset_index(drop=True)

    df2['freqRatio'] = round(df2['freqRatio'], 2)
    df2['freqExCorp'] = round(df2['freqExCorp'], 9)
    df2['freqSrcDoc'] = round(df2['freqSrcDoc'], 9)

    df2 = df2[['stem', 'words', 'count', 'freqSrcDoc', 'freqExCorp', 'freqRatio']]

    if filename is not None:
        df2.to_csv(filename, index=False, sep=";")
    return df2


df = extract_keyword_profile(url="https://en.wikipedia.org/wiki/Unmanned_aerial_vehicle", filename="UAV.csv")
df = extract_keyword_profile(url="https://en.wikipedia.org/wiki/Renewable_energy", filename="ren_energy.csv")
df = extract_keyword_profile(url="https://en.wikipedia.org/wiki/Circular_economy", filename="cir_economy.csv")
