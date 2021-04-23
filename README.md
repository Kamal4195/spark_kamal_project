# Word Count Using Spark with Python
```
import urllib.request

urllib.request.urlretrieve("https://www.gutenberg.org/files/84/84-0.txt","/tmp/kamal.txt")
dbutils.fs.mv("file:/tmp/kamal.txt",'dbfs:/data/kamal.txt')
```
```
nServers=4
textRDD= sc.textFile('dbfs:/data/kamal.txt',nServers)
```
```
from pyspark.ml.feature import StopWordsRemover
remover=StopWordsRemover()
stopwords=remover.getStopWords()
# print(stopwords)

messyRDD=textRDD.flatMap(lambda line:line.strip().split(" "))
```
```
import re
def removePunctuation(text):
    return re.sub('([^\w\s]|_)','',text,0).strip().lower()


nonLetterRDD=messyRDD.map(lambda wrds: removePunctuation(wrds))
cleanRDD=nonLetterRDD.filter(lambda word: word not in stopwords).map(lambda word: (word,1))
cleanRDD.take(30)
```
```
resultRDD=cleanRDD.reduceByKey(lambda x,y: x+y)
results=resultRDD.collect()
```
```
print(results)
```
```
%py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

title = 'Top 10 Words Used in the book' 
xlabel = 'word'
ylabel = 'count'

wordsdf = pd.DataFrame.from_records(results, columns =[xlabel, ylabel])
df2=wordsdf[(wordsdf.word != "")]
top10=df2.nlargest(10,["count"])
print(top10)

sns.barplot(xlabel, ylabel, data=top10, palette="Greens_d").set_title(title)
```
