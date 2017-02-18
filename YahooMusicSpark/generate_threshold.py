
test_file = "raw_data/testIdx2.txt"
track_file = "raw_data/trackData2.txt"
predictions_mean_file = "result/predictions_mean.csv"
user_items_with_rate_file = "result/user_items_with_rate.csv"

result_file = "result/final_threshold_result.txt"

user_rates_dic = {}
user_mean_dic = {}
tracks = {}

with open(predictions_mean_file) as mean_data:
    for line in mean_data:
        temp = line.strip("\n").split(",")
        user_mean_dic[temp[0]] = float(temp[1])

with open(user_items_with_rate_file) as rate_data:
    for line in rate_data:
        [user_id, items_with_rate] = line.strip("\n").split("|", 1)
        unit_dic = {}
        items_with_rate = items_with_rate.split("|")
        for item_rate in items_with_rate:
            [item_id, rating] = item_rate.split(",")
            unit_dic[item_id] = float(rating)
        user_rates_dic[user_id] = unit_dic


with open(track_file) as track_data:
    for line in track_data:
        temp = line.strip("\n").split("|")
        tracks[temp[0]] = temp[1:]

# def checkRate(rate):
#     new_rate = rate
#     if rate > 100:
#         new_rate = 100
#     elif rate < 0:
#         new_rate = 0
#     return new_rate

with open(result_file, 'w') as final_threshold_result:
    with open(test_file) as test_data:
        user_id = ""
        for line in test_data:
            if "|" in line:
                test_data_temp = line.strip("\n").split("|")
                user_id = test_data_temp[0]
            else:
                track_rating = "None"
                album_rating = "None"
                artist_rating = "None"
                genres_average_rating = "None"

                final_ratings = {}

                user_mean_rate = user_mean_dic[user_id]
                items_with_rate = user_rates_dic[user_id]

                track_id = line.strip("\n")
                if track_id in items_with_rate.keys():
                    track_rating = items_with_rate[track_id]
                    final_ratings["track_rating"] = track_rating
                else:
                    final_ratings["track_rating"] = user_mean_rate

                if tracks[track_id][0] != "None":
                    album_id = tracks[track_id][0]
                    if album_id in items_with_rate.keys():
                        album_rating = items_with_rate[album_id]
                        final_ratings["album_rating"] = album_rating
                    else:
                        final_ratings["album_rating"] = user_mean_rate
                else:
                    final_ratings["album_rating"] = user_mean_rate

                if tracks[track_id][1] != "None":
                    artist_id = tracks[track_id][1]
                    if artist_id in items_with_rate.keys():
                        artist_rating = items_with_rate[artist_id]
                        final_ratings["artist_rating"] = artist_rating
                    else:
                        final_ratings["artist_rating"] = user_mean_rate
                else:
                    final_ratings["artist_rating"] = user_mean_rate

                if len(tracks[track_id]) > 2:
                    genres = tracks[track_id][2:]
                    genres_rating = []
                    for genre_id in genres:
                        if genre_id in items_with_rate.keys():
                            genre_rating = items_with_rate[genre_id]
                            genres_rating.append(genre_rating)
                        else:
                            genres_rating.append(user_mean_dic[user_id])
                    if len(genres_rating) > 0 :
                        genres_average_rating = round(sum(genres_rating) / len(genres_rating), 2)
                    else:
                        genres_average_rating = user_mean_dic[user_id]
                    final_ratings["genres_average_rating"] = genres_average_rating
                else:
                    final_ratings["genres_average_rating"] = user_mean_rate


                final_rate = final_ratings["track_rating"] * 0.325 + \
                             final_ratings["album_rating"] * 0.275  + \
                             final_ratings["artist_rating"] * 0.225  + \
                             final_ratings["genres_average_rating"] * 0.175

                final_threshold_result.write(str(final_rate) + "\n")
