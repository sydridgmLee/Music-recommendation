from pyspark import SparkContext
import numpy as np
import statistics

test_user_tracks_info_file = "test_user_tracks_info.csv"

album_tracks_file   = "../play_on_tracks/hierarchy_data/album_tracks.csv"
artist_tracks_file  = "../play_on_tracks/hierarchy_data/artist_tracks.csv"
genre_tracks_file   = "../play_on_tracks/hierarchy_data/genre_tracks.csv"
artist_albums_file  = "../play_on_tracks/hierarchy_data/artist_albums.csv"
genre_albums_file   = "../play_on_tracks/hierarchy_data/genre_albums.csv"
genre_artists_file  = "../play_on_tracks/hierarchy_data/genre_artists.csv"

mean_hierarchy_file = "../play_on_training_data/user_item_mean/user_item_mean.csv"

user_item_rate_class_file = "../play_on_training_data/user_item_rate_class/user_item_rate_class.csv"

factor_matrix = "../result/factor_matrix.csv"

def hierarchy_dictionary(filename):
    dic = {}
    with open(filename) as data:
        for line in data:
            temp = line.strip("\n").split(",", 1)
            dic[temp[0]] = list(temp[1].split(","))
    return dic

test_user_tracks_info_dic = hierarchy_dictionary(test_user_tracks_info_file)

mean_hierarchy_dic = hierarchy_dictionary(mean_hierarchy_file)

sc = SparkContext()
sc.setCheckpointDir("checkPoints/")

album_tracks_RDD = sc.textFile(album_tracks_file).map(lambda line: line.split(",")).cache()
artist_tracks_RDD = sc.textFile(artist_tracks_file).map(lambda line: line.split(",")).cache()
genre_tracks_RDD = sc.textFile(genre_tracks_file).map(lambda line: line.split(",")).cache()
artist_albums_RDD = sc.textFile(artist_albums_file).map(lambda line: line.split(",")).cache()
genre_albums_RDD = sc.textFile(genre_albums_file).map(lambda line: line.split(",")).cache()
genre_artists_RDD = sc.textFile(genre_artists_file).map(lambda line: line.split(",")).cache()



user_item_rate_class_RDD = sc.textFile(user_item_rate_class_file).map(lambda line: line.split(",")).cache()
# print(user_item_rate_class_RDD.take(3))
def fetch_tracks_from_album(album_id):
    # print("alubm_id" + str(album_id))
    return album_tracks_RDD.filter(lambda x: x[0] == str(album_id)).collect()[0]

def fetch_tracks_from_artist(artist_id):
    return artist_tracks_RDD.filter(lambda x: x[0] == str(artist_id)).collect()[0]

def fetch_tracks_from_genre(genre_id):
    return genre_tracks_RDD.filter(lambda x: x[0] == str(genre_id)).collect()[0]

def fetch_albums_from_artist(artist_id):
    return artist_albums_RDD.filter(lambda x: x[0] == str(artist_id)).collect()[0]

def fetch_albums_from_genre(genre_id):
    return genre_albums_RDD.filter(lambda x: x[0] == str(genre_id)).collect()[0]

def fetch_artists_from_genre(genre_id):
    return genre_artists_RDD.filter(lambda x: x[0] == str(genre_id)).collect()[0]

def shatter(list):
    result = []
    for item in list:
        result.append([item])
    return result

def fetch_rates(user_id, item_list, item_class):
    mean_rate = mean_hierarchy_dic[user_id][item_class-1]
    # print(item_list)
    item_list_RDD = sc.parallelize(shatter(item_list)).map(lambda x: (x[0], mean_rate))
    # print(item_list_RDD.take(1))
    # print("item_list_RDD.count " + str(item_list_RDD.count()))
    # print(user_id)
    item_rate_RDD = user_item_rate_class_RDD.\
        filter(lambda x: x[0] == user_id and x[3] == str(item_class)).\
        map(lambda tokens: (tokens[1], tokens[2]))
    # print("item_rate_RDD_count " + str(item_rate_RDD.count()))

    item_no_rate_RDD = item_list_RDD.subtractByKey(item_rate_RDD)
    # print("item_no_rate_RDD.count " + str(item_no_rate_RDD.count()))

    return item_no_rate_RDD.union(item_rate_RDD).\
        map(lambda tokens: float(tokens[1])).collect()

def fetch_rate(user_id, item_id, item_class):
    rate = user_item_rate_class_RDD.filter(lambda x: x[0] == user_id and x[1] == item_id).\
        map(lambda tokens: tokens[2]).collect()
    if rate:
        return rate[0]
    else:
        return mean_hierarchy_dic[user_id][item_class-1]

def g_statistic_feature(user_id, item_list, item_class):
    item_rate_list = fetch_rates(user_id, item_list, item_class)
    # print(item_rate_list)

    # print(len(item_rate_list))
    max_rate = max(item_rate_list)
    min_rate = min(item_rate_list)
    mean_rate = round(np.mean(item_rate_list), 2)
    median_rate = round(np.median(item_rate_list), 2)
    median_low_rate = round(statistics.median_low(item_rate_list), 2)
    median_high_rate = round(statistics.median_high(item_rate_list), 2)
    mode_rate = statistics.mode(item_rate_list)
    high_track_rate_list = filter(lambda x: x > mean_rate, item_rate_list)
    range_high_rate = len(high_track_rate_list)
    low_track_rate_list = filter(lambda x: x < mean_rate, item_rate_list)
    range_low_rate = len(low_track_rate_list)
    return [max_rate, min_rate, mean_rate, median_rate, median_low_rate, median_high_rate, range_high_rate, range_low_rate]


def album_tracks_statistic_feature(user_id,album_id,item_class):
    tracks_list = fetch_tracks_from_album(album_id)
    tracks_list = tracks_list[1:]
    # print("lenght_track_list" + str(len(tracks_list)))
    return g_statistic_feature(user_id, tracks_list,item_class)

def artist_tracks_statistic_feature(user_id, artist_id, item_class):
    tracks_list = fetch_tracks_from_artist(artist_id)
    return g_statistic_feature(user_id, tracks_list, item_class)

def genre_tracks_statistic_feature(user_id,genre_list,item_class):
    tracks_list = []
    for genre_id in genre_list:
        tracks_list += fetch_tracks_from_genre(int(genre_id))
    # print(len(tracks_list))
    return g_statistic_feature(user_id, tracks_list,item_class)

def artist_albums_statistic_feature(user_id,artist_id, item_class):
    albums_list = fetch_albums_from_artist(artist_id)
    return g_statistic_feature(user_id, albums_list,item_class)

def genre_albums_statistic_feature(user_id,genre_list,item_class):
    albums_list = []
    for genre_id in genre_list:
        albums_list += fetch_albums_from_genre(int(genre_id))
    return g_statistic_feature(user_id, albums_list,item_class)

def genre_artists_statistic_feature(user_id,genre_list,item_class):
    artists_list = []
    for genre_id in genre_list:
        artists_list += fetch_artists_from_genre(int(genre_id))
    return g_statistic_feature(user_id, artists_list,item_class)


def main():
    with open(factor_matrix, 'w') as result_data:
        for user_id in test_user_tracks_info_dic:
            info_list = test_user_tracks_info_dic[user_id]

            album_data_feature = []
            artist_data_feature = []
            genre_data_feature = []

            if info_list[1] != "None":
                album_id = int(info_list[1])
                album_data_feature.append(fetch_rate(user_id, [album_id], 2))
                # print(album_data_feature)
                album_tracks_data_feature = album_tracks_statistic_feature(user_id, album_id, 1)
                # print(album_tracks_data_feature)
                album_data_feature += album_tracks_data_feature
            else:
                album_data_feature.append(50)
                album_tracks_data_feature = [50, 50, 50, 50, 50, 50]
                album_data_feature += album_tracks_data_feature

            print(1)
            # print(album_data_feature)

            if info_list[2] != "None":
                artist_id = int(info_list[2])
                artist_data_feature.append(fetch_rate(user_id, [artist_id], 3))
                artist_albums_data_feature = artist_albums_statistic_feature(user_id, artist_id, 2)
                artist_tracks_data_feature = artist_tracks_statistic_feature(user_id, artist_id, 1)
                artist_data_feature += artist_albums_data_feature + artist_tracks_data_feature
            else:
                artist_data_feature.append(50)
                artist_albums_data_feature = [50, 50, 50, 50, 50, 50]
                artist_tracks_data_feature = [50, 50, 50, 50, 50, 50]
                artist_data_feature += artist_albums_data_feature + artist_tracks_data_feature

            print(2)
            # print(artist_data_feature)

            if len(info_list) > 3:
                genre_list = info_list[3:]
                genre_self_data_feature = g_statistic_feature(user_id, genre_list, 4)
                genre_artists_data_feature = genre_artists_statistic_feature(user_id, genre_list, 3)
                genre_albums_data_feature = genre_albums_statistic_feature(user_id, genre_list, 2)
                genre_tracks_data_feature = genre_tracks_statistic_feature(user_id, genre_list,1)
                genre_data_feature += genre_self_data_feature \
                                     + genre_artists_data_feature \
                                     + genre_albums_data_feature \
                                     + genre_tracks_data_feature
            else:
                genre_self_data_feature = [50, 50, 50, 50, 50, 50]
                genre_artists_data_feature = [50, 50, 50, 50, 50, 50]
                genre_albums_data_feature = [50, 50, 50, 50, 50, 50]
                genre_tracks_data_feature = [50, 50, 50, 50, 50, 50]
                genre_data_feature += genre_self_data_feature \
                                      + genre_artists_data_feature \
                                      + genre_albums_data_feature \
                                      + genre_tracks_data_feature
            print(3)
            # print(genre_data_feature)

            result = album_data_feature + artist_data_feature + genre_data_feature
            output = ','.join(str(data) for data in result)
            result_data.write(output + "\n")

main()


