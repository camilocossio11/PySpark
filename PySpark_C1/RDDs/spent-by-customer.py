from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("SpentByCustomer")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    spent = fields[2]
    return (customerID, spent)

if __name__ == "__main__":
    current_directory = os.path.dirname(os.path.abspath(__file__))
    project_directory = os.path.dirname(current_directory)
    data_route = os.path.join(project_directory, 'Data', 'customer-orders.csv')

    lines = sc.textFile(data_route)
    parsedLines = lines.map(parseLine)
    customerSpent = parsedLines.reduceByKey(lambda x,y: round(float(x) + float(y),2)).collectAsMap()
    spentSorted = sorted(customerSpent.items(), key = lambda ele : customerSpent[ele[0]])
    for result in spentSorted:
        print(f"{result[0]}\t{result[1]}$")