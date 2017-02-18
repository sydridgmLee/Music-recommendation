import numpy as np
import statistics
import time

start_time = time.time()

training_user_tracks_info_file = "training_user_tracks_info.csv"

album_tracks_file   = "../play_on_tracks/hierarchy_data/album_tracks.csv"
artist_tracks_file  = "../play_on_tracks/hierarchy_data/artist_tracks.csv"
genre_tracks_file   = "../play_on_tracks/hierarchy_data/genre_tracks.csv"
artist_albums_file  = "../play_on_tracks/hierarchy_data/artist_albums.csv"
genre_albums_file   = "../play_on_tracks/hierarchy_data/genre_albums.csv"
genre_artists_file  = "../play_on_tracks/hierarchy_data/genre_artists.csv"

mean_hierarchy_file = "../play_on_training_data/sorted_user_item_mean/sorted_user_item_mean.csv"

user_item_rate_class_file = "../play_on_training_data/user_item_rate_class/user_item_rate_class.txt"

factor_matrix = "../result/training_factor_matrix.csv"

def hierarchy_dictionary(filename):
    dic = {}
    with open(filename) as data:
        for line in data:
            temp = line.strip("\n").split(",", 1)
            dic[temp[0]] = list(temp[1].split(","))
    return dic

album_tracks_dic = hierarchy_dictionary(album_tracks_file)
artist_tracks_dic = hierarchy_dictionary(artist_tracks_file)
genre_tracks_dic = hierarchy_dictionary(genre_tracks_file)
artist_albums_dic = hierarchy_dictionary(artist_albums_file)
genre_albums_dic = hierarchy_dictionary(genre_albums_file)
genre_artists_dic = hierarchy_dictionary(genre_artists_file)

training_user_tracks_info_dic = hierarchy_dictionary(training_user_tracks_info_file)

mean_hierarchy_dic = hierarchy_dictionary(mean_hierarchy_file)

def fetch_tracks_from_album(album_id):
    return album_tracks_dic[str(album_id)]

def fetch_tracks_from_artist(artist_id):
    return artist_tracks_dic[str(artist_id)]

def fetch_tracks_from_genre(genre_id):
    return genre_tracks_dic[str(genre_id)]

def fetch_albums_from_artist(artist_id):
    return artist_albums_dic[str(artist_id)]

def fetch_albums_from_genre(genre_id):
    return genre_albums_dic[str(genre_id)]

def fetch_artists_from_genre(genre_id):
    return genre_artists_dic[str(genre_id)]

def fetch_rates(user_id, item_list, rate_dic, item_class):
    rates = []
    for item_id in item_list:
        if item_id in rate_dic:
            rate = rate_dic[item_id]
            rates.append(float(rate))
        else:
            rate = mean_hierarchy_dic[user_id][item_class-1]
            rates.append(float(rate))
    return rates

def g_statistic_feature(user_id, item_list, rate_dic, item_class):
    item_rate_list = fetch_rates(user_id, item_list, rate_dic, item_class)

    max_rate = max(item_rate_list)
    min_rate = min(item_rate_list)
    mean_rate = round(np.mean(item_rate_list), 2)
    median_rate = round(np.median(item_rate_list), 2)
    median_low_rate = round(statistics.median_low(item_rate_list), 2)
    median_high_rate = round(statistics.median_high(item_rate_list), 2)
    high_track_rate_list = filter(lambda x: x > mean_rate, item_rate_list)
    range_high_rate = len(high_track_rate_list)
    low_track_rate_list = filter(lambda x: x < mean_rate, item_rate_list)
    range_low_rate = len(low_track_rate_list)
    return [max_rate, min_rate, mean_rate, median_rate, median_low_rate, median_high_rate, range_high_rate, range_low_rate]

def album_tracks_statistic_feature(user_id,album_id,rate_dic,item_class):
    tracks_list = fetch_tracks_from_album(album_id)
    return g_statistic_feature(user_id, tracks_list,rate_dic,item_class)

def artist_tracks_statistic_feature(user_id, artist_id, rate_dic,item_class):
    tracks_list = fetch_tracks_from_artist(artist_id)
    return g_statistic_feature(user_id, tracks_list, rate_dic,item_class)

def genre_tracks_statistic_feature(user_id,genre_list,rate_dic,item_class):
    tracks_list = []
    for genre_id in genre_list:
        tracks_list += fetch_tracks_from_genre(int(genre_id))
    return g_statistic_feature(user_id, tracks_list,rate_dic,item_class)

def artist_albums_statistic_feature(user_id,artist_id, rate_dic,item_class):
    albums_list = fetch_albums_from_artist(artist_id)
    return g_statistic_feature(user_id, albums_list,rate_dic,item_class)

def genre_albums_statistic_feature(user_id,genre_list,rate_dic,item_class):
    albums_list = []
    for genre_id in genre_list:
        albums_list += fetch_albums_from_genre(int(genre_id))
    return g_statistic_feature(user_id, albums_list,rate_dic,item_class)

def genre_artists_statistic_feature(user_id,genre_list,rate_dic,item_class):
    artists_list = []
    for genre_id in genre_list:
        artists_list += fetch_artists_from_genre(int(genre_id))
    return g_statistic_feature(user_id, artists_list,rate_dic,item_class)

def read_lines(file, num):
    lines = []
    line = file.readline()
    lines.append(line)
    if line:
        for i in range(1,num):
            lines.append(file.readline())
        return lines
    else:
        return line

def main():
    with open(factor_matrix, 'w') as result_data:
        with open(training_user_tracks_info_file) as training_user_tracks_info_data:
            with open(user_item_rate_class_file) as user_item_rate_class_data:
                with open(mean_hierarchy_file) as mean_data:
                    # 6 test song for each user
                    lines_test = read_lines(training_user_tracks_info_data, 6)
                    train_user_id = -1
                    mean_user_id = -1
                    while lines_test:
                        temp = lines_test[0].strip("\n").split(",")
                        current_user_id = temp[0]

                        lines_train = []
                        # Navigate to the current user in training data.
                        while int(train_user_id) < int(current_user_id):
                            lines_train = user_item_rate_class_data.readline()
                            [train_user_id, train_user_rates_num] = lines_train.\
                                strip("\n").split("|")
                            lines_train = read_lines(user_item_rate_class_data, int(train_user_rates_num))

                        rate_dic = {}
                        for line_train in lines_train:
                            item_rate = line_train.strip("\n").split(",")
                            rate_dic[item_rate[0]] = item_rate[1]

                        while  int(mean_user_id) < int(current_user_id):
                            mean_hierarchy_dic.clear()
                            line_mean = mean_data.readline()
                            user_meanRates = line_mean.strip("\n").split(",")
                            mean_user_id = user_meanRates[0]
                            mean_hierarchy_dic[user_meanRates[0]] = user_meanRates[1:]

                        for line_test in lines_test:
                            training_user_tracks_info = line_test.strip("\n").split(",")
                            response = training_user_tracks_info[1]

                            album_data_feature = []
                            artist_data_feature = []
                            genre_data_feature = []

                            if "None" not in training_user_tracks_info[3]:
                                album_id = int(training_user_tracks_info[3])
                                album_self_data_feature = fetch_rates(current_user_id, [album_id], rate_dic, 2)
                                album_tracks_data_feature = album_tracks_statistic_feature(current_user_id, album_id, rate_dic, 1)
                                album_data_feature = album_self_data_feature + album_tracks_data_feature
                            else:
                                album_self_data_feature = [50]
                                album_tracks_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                album_data_feature = album_self_data_feature + album_tracks_data_feature


                            if "None" not in training_user_tracks_info[4]:
                                artist_id = int(training_user_tracks_info[4])
                                artist_self_data_feature = fetch_rates(current_user_id, [artist_id], rate_dic, 3)
                                if str(artist_id) in artist_albums_dic:
                                    artist_albums_data_feature = artist_albums_statistic_feature(current_user_id,artist_id, rate_dic, 2)
                                else:
                                    artist_albums_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                artist_tracks_data_feature = artist_tracks_statistic_feature(current_user_id, artist_id, rate_dic, 1)
                                artist_data_feature = artist_self_data_feature + \
                                                      artist_albums_data_feature + \
                                                      artist_tracks_data_feature
                            else:
                                artist_self_data_feature = [50]
                                artist_albums_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                artist_tracks_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                artist_data_feature = artist_self_data_feature + \
                                                      artist_albums_data_feature + \
                                                      artist_tracks_data_feature


                            if len(training_user_tracks_info) > 5:
                                genre_list = training_user_tracks_info[5:]
                                genre_self_data_feature = g_statistic_feature(current_user_id, genre_list, rate_dic, 4)
                                genre_artists_data_feature = genre_artists_statistic_feature(current_user_id, genre_list,rate_dic, 3)
                                genre_albums_data_feature = genre_albums_statistic_feature(current_user_id, genre_list,rate_dic, 2)
                                genre_tracks_data_feature = genre_tracks_statistic_feature(current_user_id, genre_list,rate_dic, 1)
                                genre_data_feature = genre_self_data_feature \
                                                      + genre_artists_data_feature \
                                                      + genre_albums_data_feature \
                                                      + genre_tracks_data_feature
                            else:
                                genre_self_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                genre_artists_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                genre_albums_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                genre_tracks_data_feature = [50, 50, 50, 50, 50, 50, 50, 50]
                                genre_data_feature = genre_self_data_feature \
                                                      + genre_artists_data_feature \
                                                      + genre_albums_data_feature \
                                                      + genre_tracks_data_feature

                            result = album_data_feature + artist_data_feature + genre_data_feature
                            output = ','.join(str(data) for data in result)
                            result_data.write(response + "," + output + "\n")

                        lines_test = read_lines(training_user_tracks_info_data, 6)
                        print(current_user_id, "%.2f s" % (time.time() - start_time))

main()


