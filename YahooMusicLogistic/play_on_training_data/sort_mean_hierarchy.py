from pyspark import SparkContext

user_item_mean_file = "user_item_mean/user_item_mean.csv"

sc = SparkContext()

sorted_user_item_mean_RDD = sc.textFile(user_item_mean_file).\
    map(lambda line: line.split(",")).\
    map(lambda tokens: (int(tokens[0]), (tokens[1:]))).sortByKey()

import os.path
import shutil

def toCSVLine(data):
    values =  ','.join(str(d) for d in data[1])
    return str(data[0]) + "," + values

def writeCSV(dir, saveFile):
    for name in os.listdir(dir):
        if name.startswith("part") and os.path.splitext(dir+name)[1] != '.csv':
            os.rename(dir+name, dir+name+'.csv')

    file_names = [name for name in os.listdir(dir) if name.startswith('part')]
    # os.path.join(dir, name + '.txt')
    print(file_names)

    with open(saveFile,'w') as result:
        for f in file_names:
            f = dir + f
            with open(f,'r') as fd:
                shutil.copyfileobj(fd, result)

def output(rdd, dir):
    lines = rdd.map(toCSVLine)

    if os.path.isdir(dir):
        shutil.rmtree(dir)
    lines.saveAsTextFile(dir)

dir = 'sorted_user_item_mean/'
output(sorted_user_item_mean_RDD, dir)
writeCSV(dir, "sorted_user_item_mean/sorted_user_item_mean.csv")