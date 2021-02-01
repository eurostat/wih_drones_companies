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

response = requests.get(url="https://en.wikipedia.org/wiki/Unmanned_aerial_vehicle")

soup = BeautifulSoup(response.content, 'html.parser')

title = soup.find(id="firstHeading")
print(title.string)


# Extract the plain text content from paragraphs
paras = []
for paragraph in soup.find_all('p'):
    paras.append(str(paragraph.text))

# Extract text from paragraph headers
heads = []
for head in soup.find_all('span', attrs={'mw-headline'}):
    heads.append(str(head.text))

# Interleave paragraphs & headers
text = [val for pair in zip(paras, heads) for val in pair]
text = ' '.join(text)

# Drop footnote superscripts in brackets
text = re.sub(r"\[.*?\]+", '', text)

# Replace '\n' (a new line) with ''
text = text.replace('\n', '')
print(text)



nltk.download('punkt')
nltk.download('stopwords')

words = word_tokenize(text)
words = [word.lower() for word in words]
words = [word for word in words if not word in stopwords.words()]
words = [word for word in words if word.isalnum()]

#porter = PorterStemmer()
#lancaster = LancasterStemmer()

#wordsP = [porter.stem(word) for word in words]
#wordsL = [lancaster.stem(word) for word in words]

freq = []
for w in words:
    freq.append(words.count(w))


d = dict(zip(words, freq))
d = {k: v for k, v in sorted(d.items(), key=lambda item: item[1], reverse= True)}

df = pd.DataFrame(list(d.items()), columns=['word', 'count'])
df['freq'] = [(wordfreq.word_frequency(k, 'en')) for k, v in d.items()]
df = df[(df.freq != 0)]

df['freqDoc'] = df['count'] / sum(df['count'])

df['freqComp'] = df['freqDoc'] / df['freq']
df = df.sort_values(by=['freqComp'], ascending=False)
