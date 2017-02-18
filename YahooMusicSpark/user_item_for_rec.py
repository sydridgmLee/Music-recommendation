# testIdx2.txt ===> (user_id, item_id)

test_file = "raw_data/testIdx2.txt"
track_file = "raw_data/trackData2.txt"

user_track_for_rec_file = "data/user_track_for_rec.csv"
user_album_for_rec_file = "data/user_album_for_rec.csv"
user_artist_for_rec_file = "data/user_artist_for_rec.csv"
user_genre_for_rec_file = "data/user_genre_for_rec.csv"

#---------------------------------------------------------------
with open(user_track_for_rec_file, 'w') as user_track_for_rec:
    with open(test_file) as test_data:
        cur_user = ""
        for line in test_data:
            track = ""
            if '|' in line:
                temp = line.strip("\n").split("|")
                cur_user = temp[0]
            else:
                track = line.strip("\n")
                user_track_for_rec.write(cur_user + "," + track + "\n")

with open(user_album_for_rec_file, 'w') as user_album:
    with open(user_artist_for_rec_file, 'w') as user_artist:
        with open(user_genre_for_rec_file, 'w') as user_genre:
            with open(track_file) as track_data:
                tracks = {}
                for line in track_data:
                    temp = line.strip("\n").split("|")
                    tracks[temp[0]] = temp[1:]
                with open(test_file) as test_data:
                    user_id = ""
                    for line in test_data:
                        if "|" in line:
                            test_data_temp = line.strip("\n").split("|")
                            user_id = test_data_temp[0]
                        else:
                            track = line.strip("\n")
                            if tracks[track][0] != "None":
                                album = tracks[track][0]
                                user_album.write(user_id + "," + album + "\n")

                            if tracks[track][1] != "None":
                                artist = tracks[track][1]
                                user_artist.write(user_id + "," + artist + "\n")

                            if len(tracks[track]) > 2:
                                genres = tracks[track][2:]
                                for genre in genres:
                                    user_genre.write(user_id + "," + genre + "\n")


