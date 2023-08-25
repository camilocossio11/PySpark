from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import os

spark = SparkSession.builder.appName('spentByCustomer').getOrCreate()

schema = StructType([StructField('customer_ID',IntegerType(), True),
                    StructField('product_ID',IntegerType(), True),
                    StructField('money_spent',FloatType(), True)])

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
data_route = os.path.join(project_directory, 'Data', 'customer-orders.csv')

df = spark.read.schema(schema).csv(data_route)

total_spent = df.groupBy('customer_ID').agg(func.round(func.sum('money_spent'),2).alias('total_spent')).sort('total_spent')
total_spent.show()