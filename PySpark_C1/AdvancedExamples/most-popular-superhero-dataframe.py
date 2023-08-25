from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import os

def mostPopularHero():
    spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()
    schema = StructType([ \
                        StructField("id", IntegerType(), True), \
                        StructField("name", StringType(), True)])
    
    # Data location
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)

    #Read files
    names = spark.read.schema(schema).option("sep", " ").csv(os.path.join(project_directory, 'Data', 'Marvel-names.txt'))
    lines = spark.read.text(os.path.join(project_directory, 'Data', 'Marvel-graph.txt'))

    connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
                        .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
                        .groupBy("id").agg(func.sum("connections").alias("connections"))
        
    mostPopular = connections.sort(func.col("connections").desc()).first()

    mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

    print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

if __name__ == '__main__':
    mostPopularHero()