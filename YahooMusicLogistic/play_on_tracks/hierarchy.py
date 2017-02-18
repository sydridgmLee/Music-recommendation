from pyspark import SparkContext

tracks_file = "../raw_data/trackData2.txt"

sc = SparkContext()

tracks_RDD = sc.textFile(tracks_file).map(lambda line: line.split("|")).cache()

print("-------------------album ====> tracks-----------------------")

album_tracks_RDD = tracks_RDD.filter(lambda x: x[1] != "None").\
    map(lambda tokens: (tokens[1], tokens[0])).groupByKey().mapValues(tuple)

# print(album_tracks_RDD.take(3))


print("--------------------artist ===> tracks --------------------")

artist_tracks_RDD = tracks_RDD.filter(lambda x: x[2] != "None").\
    map(lambda tokens: (tokens[2], tokens[0])).groupByKey().mapValues(tuple)

print("--------------------genre ====> tracks---------------------")

def allocate(data):
    genres = []
    for genre in data[1]:
        genres.append((genre, data[0]))
    return tuple(genres)

genre_tracks_RDD = tracks_RDD.filter(lambda x: len(x) > 3).map(lambda x: (x[0], x[3:])). \
    flatMap(allocate).groupByKey().mapValues(tuple)
# print(genre_tracks_RDD.take(3))

print("-------------------artist ====> albums -------------------")

artist_albums_RDD = tracks_RDD.filter(lambda x: x[1] != "None" and x[2] != "None").\
    map(lambda tokens: (tokens[2], tokens[1])).distinct().groupByKey().mapValues(tuple)

print("------------------- genre =====> albums--------------------")

genre_albums_RDD = tracks_RDD.filter(lambda x: x[1] != "None" and len(x) > 3).\
    map(lambda x: (x[1], x[3:])).flatMap(allocate).distinct().groupByKey().mapValues(tuple)

print("--------------------genre ====> artists -------------------")

genre_artists_RDD = tracks_RDD.filter(lambda x: x[2] != "None" and len(x) > 3).\
    map(lambda x: (x[2], x[3:])).flatMap(allocate).distinct().groupByKey().mapValues(tuple)
# print(genre_artists_RDD.take(2))

# store the hierarchy data

import os.path
import shutil

def toCSVLine(data):
    values =  ','.join(str(d) for d in data[1])
    return data[0] + "," + values

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

dir = 'hierarchy_data/album_tracks/'
output(album_tracks_RDD, dir)
writeCSV(dir, "hierarchy_data/album_tracks.csv")

dir = 'hierarchy_data/artist_tracks/'
output(artist_tracks_RDD, dir)
writeCSV(dir, "hierarchy_data/artist_tracks.csv")

dir = 'hierarchy_data/genre_tracks/'
output(genre_tracks_RDD, dir)
writeCSV(dir, "hierarchy_data/genre_tracks.csv")

dir = 'hierarchy_data/artist_albums/'
output(artist_albums_RDD, dir)
writeCSV(dir, "hierarchy_data/artist_albums.csv")

dir = 'hierarchy_data/genre_albums/'
output(genre_albums_RDD, dir)
writeCSV(dir, "hierarchy_data/genre_albums.csv")

dir = 'hierarchy_data/genre_artists/'
output(genre_artists_RDD, dir)
writeCSV(dir, "hierarchy_data/genre_artists.csv")

