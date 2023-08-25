from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

def read_file(file_name):
    lines = sc.textFile(file_name)
    parsedLines = lines.map(parseLine)
    return parsedLines

def min_max_temp(parsedLines, to_find):
    if to_find.upper() == "TMIN":
        temps = parsedLines.filter(lambda x: to_find in x[1])
        stationTemps = temps.map(lambda x: (x[0], x[2]))
        temps = stationTemps.reduceByKey(lambda x, y: min(x,y))
        results = temps.collect()
    else:
        temps = parsedLines.filter(lambda x: "TMAX" in x[1])
        stationTemps = temps.map(lambda x: (x[0], x[2]))
        temps = stationTemps.reduceByKey(lambda x, y: max(x,y))
        results = temps.collect()
    print(f"{to_find}:")
    for result in results:
        print(f"{result[0]}\t{result[1]}F")

current_directory = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(current_directory)
data_route = os.path.join(project_directory, 'Data', '1800.csv')

parsedLines = read_file(data_route)
min_max_temp(parsedLines,'TMAX')