from __future__ import print_function
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import os

def read_process_data():
  try:
    # Data location
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)

    #Read files
    data = spark.read.option("sep", ",").option("header", "true").option("inferSchema", "true")\
            .csv(os.path.join(project_directory, 'Data', 'realestate.csv'))
    assembler = VectorAssembler(inputCols=["HouseAge", "DistanceToMRT", "NumberConvenienceStores"], outputCol="features")
    processed = assembler.transform(data.select(['PriceOfUnitArea','HouseAge','DistanceToMRT','NumberConvenienceStores'])).select('PriceOfUnitArea','features')
    processed = processed.withColumnRenamed("PriceOfUnitArea", "label")
    return processed
  except Exception as e:
    print(f'An error has occurred: {e}')

def split_data(data):
  try:
    trainTest = data.randomSplit([0.7, 0.3])
    trainingDF = trainTest[0]
    testDF = trainTest[1]
    return trainingDF, testDF
  except Exception as e:
    print(f'An error has occurred: {e}')

def decisionTree(trainingData,testData):
    # Now create our linear regression model
    DT = DecisionTreeRegressor()

    # Train the model using our training data
    model = DT.fit(trainingData)

    # Now see if we can predict values in our test data.
    fullPredictions = model.transform(testData).cache()
    fullPredictions.show()

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisionTree").getOrCreate()

    # Load data
    data = read_process_data()

    # Split our data into training data and testing data
    trainingDF, testDF = split_data(data)

    # Run the model and make predictions
    decisionTree(trainingDF, testDF)

    # Stop the session
    spark.stop()
