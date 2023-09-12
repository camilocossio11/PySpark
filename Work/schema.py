from pyspark.sql import SparkSession
import json

# Spark Session
spark = SparkSession.builder.appName("VerifyDataType").getOrCreate()

# Load CSV file
df = spark.read.csv("./data/realestate.csv", header=True, inferSchema=True)

# Infered achema
inferd_schema = df.schema.json()

# Export JSON
root = './schemas/schema.json'
with open(root, "w") as output_file:
    json.dump(inferd_schema, output_file, indent=4)

# Cierra la sesión de Spark al finalizar
spark.stop()