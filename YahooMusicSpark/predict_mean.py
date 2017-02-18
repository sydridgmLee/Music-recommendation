from pyspark import SparkContext

predictions_user_track_mean_file = \
    "data/predictions_user_track_mean.csv"
predictions_user_album_mean_file = \
    "data/predictions_user_album_mean.csv"
predictions_user_artist_mean_file = \
    "data/predictions_user_artist_mean.csv"
predictions_user_genre_mean_file = \
    "data/predictions_user_genre_mean.csv"


sc = SparkContext("local", "compute rate mean")

#---------------------------------------------------------------------

predictions_user_album_mean_RDD = sc.textFile(predictions_user_album_mean_file).\
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], round(float(tokens[1]), 2))).cache()
predictions_user_artist_mean_RDD = sc.textFile(predictions_user_artist_mean_file). \
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], round(float(tokens[1]),2))).cache()
predictions_user_genre_mean_RDD = sc.textFile(predictions_user_genre_mean_file). \
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], round(float(tokens[1]), 2))).cache()
predictions_user_track_mean_RDD = sc.textFile(predictions_user_track_mean_file). \
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], round(float(tokens[1]), 2))).cache()

predictions_mean_RDD = predictions_user_album_mean_RDD.union(predictions_user_artist_mean_RDD).\
    union(predictions_user_genre_mean_RDD).union(predictions_user_track_mean_RDD).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).\
    map(lambda x: (x[0], round(sum(x[1])/len(x[0]),2)))
# map(lambda tokens: (tokens[0], (tokens[1][0][0][0], tokens[1][0][0][1], tokens[1][0][1], tokens[1][1]))).\
# print(predictions_mean_RDD.take(10))
#-----------------------------------------------------------------------

import os.path
import shutil

def toCSVLine(data):
    # print(data)
    return ','.join(str(d) for d in data)

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

dir = "result/predictions_mean/"
output(predictions_mean_RDD, dir)
writeCSV(dir, "result/predictions_mean.csv")
