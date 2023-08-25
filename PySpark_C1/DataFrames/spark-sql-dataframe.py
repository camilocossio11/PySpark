from pyspark.sql import SparkSession
from pyspark.sql import functions as func
import os

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
data_route = os.path.join(project_directory, 'Data', 'fakefriends-header.csv')

people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv(data_route)
    
print("Here is our inferred schema:")
people.printSchema()

print("Let's display the name column:")
people.select("name").show()

print("Filter out anyone over 21:")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().orderBy('age').show()

print("Make everyone 10 years older:")
people.select(people.name, people.age + 10).show()

print("Average number of friends")
people.groupBy('age').avg('friends').orderBy('age').show()

print("Average number of friends nicely")
people.groupBy('age').agg(func.round(func.avg('friends'),2).alias('friends_avg')).orderBy('age').show()

spark.stop()

