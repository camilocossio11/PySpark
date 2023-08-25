from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

spark = SparkSession.builder.appName("WordCount").getOrCreate()

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
data_route = os.path.join(project_directory, 'Data', 'Book.txt')

# Read each line of my book into a dataframe
inputDF = spark.read.text(data_route)

# Split using a regular expression that extracts words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")

# Normalize everything to lowercase
lowercaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word")).groupBy("word").count().sort("count")

# Show the results.
lowercaseWords.show(lowercaseWords.count())
