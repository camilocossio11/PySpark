from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs
import os

def loadMovieNames():
    movieNames = {}

    # Get the data location
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)
    data_route = os.path.join(project_directory, 'Data', 'ml-100k/u.item')

    # Read data
    with codecs.open(data_route, "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

# Create a user-defined function to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]

def popularMovies(spark):
    # Create schema when reading u.data
    schema = StructType([StructField("userID", IntegerType(), True),
                        StructField("movieID", IntegerType(), True),
                        StructField("rating", IntegerType(), True),
                        StructField("timestamp", LongType(), True)])
    
    # Get the data location
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)
    data_route = os.path.join(project_directory, 'Data', 'ml-100k/u.data')

    # Load up movie data as dataframe
    moviesDF = spark.read.option("sep", "\t").schema(schema).csv(data_route)
    movieCounts = moviesDF.groupBy("movieID").count()

    return movieCounts

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
    movieCounts = popularMovies(spark)

    nameDict = spark.sparkContext.broadcast(loadMovieNames())

    # Create User Defined Function (UDF)
    lookupNameUDF = func.udf(lookupName)

    # Add a movieTitle column using our new udf
    moviesWithNames = movieCounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))

    # Sort the results
    sortedMoviesWithNames = moviesWithNames.orderBy(func.desc("count"))

    # Grab the top 10
    sortedMoviesWithNames.show(10, False)

    # Stop the session
    spark.stop()