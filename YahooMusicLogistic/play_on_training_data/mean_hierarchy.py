from pyspark import SparkContext
import numpy as np

user_item_rate_class = "user_item_rate_class/user_item_rate_class.csv"

sc = SparkContext()

user_item_rate_class_RDD = sc.textFile(user_item_rate_class).\
    map(lambda line: line.split(",")).cache()

# print(user_item_rate_class_RDD.take(3))

user_RDD = user_item_rate_class_RDD.map(lambda tokens: (tokens[0], tokens[2])).\
    groupByKey().mapValues(tuple).cache()

def get_round(float_num):
    return round(float_num, 2)

def g_mean(level):
    user_mean_RDD = user_item_rate_class_RDD.\
        filter(lambda x: x[3] == level).\
        map(lambda tokens: (tokens[0], float(tokens[2]))).\
        groupByKey().mapValues(list).mapValues(np.mean).mapValues(get_round)
    # print(user_mean_RDD.take(3))

    # print(user_mean_RDD.count())
    user_no_mean_RDD = user_RDD.map(lambda x: (x[0], 50)).\
        subtractByKey(user_mean_RDD)
    # print(user_no_mean_RDD.take(3))
    return user_mean_RDD.union(user_no_mean_RDD)



user_track_mean_RDD = g_mean("1")
# print(user_track_mean_RDD.count())
user_album_mean_RDD = g_mean("2")
# print(user_album_mean_RDD.count())
user_artist_mean_RDD = g_mean("3")
# print(user_artist_mean_RDD.count())
user_genre_mean_RDD = g_mean("4")
# print(user_genre_mean_RDD.count())

user_item_mean_RDD = user_track_mean_RDD.\
    join(user_album_mean_RDD).\
    join(user_artist_mean_RDD).\
    map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1]))).\
    join(user_genre_mean_RDD).\
    map(lambda x: (int(x[0]), (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1]))).\
    sortByKey()

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

dir = 'user_item_mean/'
output(user_item_mean_RDD, dir)
writeCSV(dir, "user_item_mean/user_item_mean.csv")