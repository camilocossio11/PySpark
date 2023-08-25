from pyspark import SparkConf, SparkContext
import re
import os

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
data_route = os.path.join(project_directory, 'Data', 'Book.txt')


input = sc.textFile(data_route)
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()
wordCountsSorted = sorted(wordCounts.items(), key = lambda ele : wordCounts[ele[0]])
print(wordCountsSorted)

for item in wordCountsSorted:
    print(item)
