# combine all prediction dataset
# user_id | item1, rate | item2, rate| .......

from pyspark import SparkContext

predictions_user_track_file = "data/predictions_user_track.csv"
predictions_user_album_file = "data/predictions_user_album.csv"
predictions_user_artist_file = "data/predictions_user_artist.csv"
predictions_user_genre_file = "data/predictions_user_genre.csv"

predictions_mean_file = "result/predictions_mean.csv"

# result_file = "/home/sydridgm/Grive/pycharm/YahooMusic/result/result.csv"

sc = SparkContext("local", "final_predict")

# ----------------------------------------------------------------------------

predictions_user_album_RDD = sc.textFile(predictions_user_album_file).\
    map(lambda line: line.split(",")).\
    map(lambda tokens: (tokens[0], (tokens[1], float(tokens[2])))).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).cache()
# print(predictions_user_album_RDD.take(10))

predictions_user_artist_RDD = sc.textFile(predictions_user_artist_file). \
    map(lambda line: line.split(",")).\
    map(lambda tokens: (tokens[0], (tokens[1], float(tokens[2])))).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).cache()

predictions_user_genre_RDD = sc.textFile(predictions_user_genre_file). \
    map(lambda line: line.split(",")).\
    map(lambda tokens: (tokens[0], (tokens[1], float(tokens[2])))). \
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).cache()

predictions_user_track_RDD = sc.textFile(predictions_user_track_file). \
    map(lambda line: line.split(",")).\
    map(lambda tokens: (tokens[0], (tokens[1], float(tokens[2])))). \
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).cache()

predictions_mean_RDD = sc.textFile(predictions_mean_file). \
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], tokens[1])).cache()

#----------------------------------------------------------------------------

def combineTuple(data):
    new_tuple = ()
    for tuple in data[1]:
        new_tuple += tuple
    return (data[0], new_tuple)

# (user_id, ((item1, rate), (item2, rate), .....))
predictions_user_items_with_rate_RDD = predictions_user_track_RDD.union(predictions_user_album_RDD).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).map(combineTuple).\
    union(predictions_user_artist_RDD).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).map(combineTuple).\
    union(predictions_user_genre_RDD).\
    groupByKey().map(lambda x: (x[0], tuple(x[1]))).map(combineTuple)

# print(predictions_user_items_with_rate_RDD.take(1))

#----------------------------------------------------------------------------

import os.path
import shutil

def toCSVLine(data):
    items_str = ""
    for item in data[1]:
        item = item[0] + ',' + str(round(item[1],2))
        items_str += '|' + item
    return data[0] + items_str

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

dir = 'result/user_items_with_rate/'
output(predictions_user_items_with_rate_RDD, dir)
writeCSV(dir, "result/user_items_with_rate.csv")