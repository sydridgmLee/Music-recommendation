from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark import SparkContext
import math
import time

user_item_rate_class_file = "data/user_item_rate_class.csv"
user_album_for_rec_file = "data/user_album_for_rec.csv"

sc = SparkContext("local", "user_album_rate")
sc.setCheckpointDir('checkpoints/')
ALS.checkpointInterval = 9
t0 = time.time()
#-------------------------------------------------------
user_item_rate_class_RDD = sc.textFile(user_item_rate_class_file)

user_album_rate_RDD = user_item_rate_class_RDD.map(lambda line: line.split(",")).\
    filter(lambda x: int(x[3]) == 2).\
    map(lambda tokens: (tokens[0],tokens[1], float(tokens[2]))).cache()

# user_rate_RDD = user_item_rate_class_RDD.map(lambda line: line.split(",")).\
#     map(lambda tokens: (tokens[0],tokens[1], float(tokens[2]))).cache()

training_RDD, validation_RDD, test_RDD = user_album_rate_RDD.randomSplit([18, 1, 1], seed=3L)

validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))
#--------------------------------------------------------

seed = 3L
ranks = [2, 4, 6, 8, 10]
best_rank = -1

#---------------------------best rank----------------------------

best_iteration = 50
err = 0
errors = [0, 0, 0, 0, 0]
min_error = float('inf')

for rank in ranks:
    model = ALS.train(training_RDD, rank, seed = seed, iterations=best_iteration)

    # print(model.predictAll(validation_for_predict_RDD).take(3))
    predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))

    # (userId, movieId), rating
    rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)

    error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    errors[err] = error
    err += 1
    print 'For rank %s the RMSE is %s' % (rank, error)
    if error < min_error:
        min_error = error
        best_rank = rank
print 'The best model was trained with rank %s' % best_rank

#-------------------use __best rank__ on test dataset-----------------------------------

model = ALS.train(training_RDD, best_rank, seed=seed, iterations=best_iteration)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print 'For testing data the RMSE is %s' % (error)

#-------------------------------------------------------------------

def g_rating_mean(data):
    sum_ = sum(data[1])
    average = sum_ / len(data[1])
    return (data[0], average)

user_album_for_rec_RDD = sc.textFile(user_album_for_rec_file).\
    map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], tokens[1]))

model = ALS.train(user_album_rate_RDD, best_rank, seed=3, iterations=50)

predictions = model.predictAll(user_album_for_rec_RDD).\
    map(lambda r: (r.user, r.product, r.rating))

mean_rating = predictions.map(lambda x: (x[0], x[2])).groupByKey().\
    map(g_rating_mean)

# -------------------------------------------------------------------

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

dir = 'data/predictions_album/'
output(predictions, dir)
writeCSV(dir, "data/predictions_user_album.csv")

dir = 'data/user_rate_mean_album/'
output(mean_rating, dir)
writeCSV(dir, "data/predictions_user_album_mean.csv")

# do stuff that takes time
print time.time() - t0