import operator
import collections

user_item_rate_class_file = "../play_on_training_data/user_item_rate_class/user_item_rate_class.txt"

album_tracks_file   = "../play_on_tracks/hierarchy_data/album_tracks.csv"
artist_tracks_file  = "../play_on_tracks/hierarchy_data/artist_tracks.csv"
genre_tracks_file   = "../play_on_tracks/hierarchy_data/genre_tracks.csv"
artist_albums_file  = "../play_on_tracks/hierarchy_data/artist_albums.csv"
genre_albums_file   = "../play_on_tracks/hierarchy_data/genre_albums.csv"
genre_artists_file  = "../play_on_tracks/hierarchy_data/genre_artists.csv"

training_user_tracks_file = "training_user_tracks.txt"

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
#
# def get_first_three(sorted_rate_dic):
#     rate_dic = {}
#     for i in range(0, 3):
#         rate_dic[sorted_rate_dic[i][0]] = sorted_rate_dic[i][1]
#     return sort_dic(rate_dic)
#
# def get_last_three(sorted_rate_dic):
#     rate_dic = {}
#     size = len(sorted_rate_dic)
#     for i in range(size-3, size):
#         rate_dic[sorted_rate_dic[i][0]] = sorted_rate_dic[i][1]
#     return sort_dic(rate_dic)

def sort_dic(dic):
    return sorted(dic.items(), key=operator.itemgetter(1))

def fetch_tracks_from_album(album_id):
    return album_tracks_dic[str(album_id)]

def fetch_tracks_from_artist(artist_id):
    return artist_tracks_dic[str(artist_id)]

def fetch_tracks_from_genre(genre_id):
    return genre_tracks_dic[str(genre_id)]

def get_tracks_by_itemID(item_id, class_id):
    if class_id == '2':
        return fetch_tracks_from_album(item_id)
    elif class_id == '3':
        return fetch_tracks_from_artist(item_id)
    else:
        return fetch_tracks_from_genre(item_id)

def get_3tracks_to_recommend(sorted_rate_dic, class_dic):
    tracks = []
    for item in reversed(sorted_rate_dic):
        item_id = item[0]
        class_id = class_dic[item_id]
        if class_id == '1':
            tracks.append(item_id)
        else:
            tracks += get_tracks_by_itemID(item_id, class_id)
        if len(tracks) >= 3:
            break

    return tracks[0:3]

def get_3tracks_not_to_recommend(sorted_rate_dic, class_dic):
    tracks = []
    for item in sorted_rate_dic:
        item_id = item[0]
        class_id = class_dic[item_id]
        if class_id == '1':
            tracks.append(item_id)
        else:
            tracks += get_tracks_by_itemID(item_id, class_id)
        if len(tracks) >= 3:
            break

    return tracks[0:3]

# check if album_id, artist_id and genre_id in:
# album_tracks_dic
# artist_tracks_dic
# genre_tracks_dic
# artist_albums_dic
# genre_albums_dic
# genre_artists_dic
def isInHierarcyDic(item_id):
    if item_id in album_tracks_dic:
        return True
    elif item_id in artist_albums_dic:
        return True
    elif item_id in artist_tracks_dic:
        return True
    elif item_id in genre_tracks_dic:
        return True
    elif item_id in genre_albums_dic:
        return True
    elif item_id in genre_artists_dic:
        return True
    else:
        return False

def main():
    with open(training_user_tracks_file, 'w') as training_user_tracks_data:
        with open(user_item_rate_class_file) as user_item_rate_class_data:
            lines_item_rate = user_item_rate_class_data.readline()
            [user_id, user_rates_num] = lines_item_rate.\
                strip("\n").split("|")
            lines_item_rate = read_lines(user_item_rate_class_data, int(user_rates_num))

            while lines_item_rate:
                rate_dic = {}
                class_dic = {}
                for line_item_rate in lines_item_rate:
                    item_rate_class = line_item_rate.strip("\n").split(",")
                    if isInHierarcyDic(item_rate_class[0]):
                        rate_dic[item_rate_class[0]] = item_rate_class[1]
                        class_dic[item_rate_class[0]] = item_rate_class[2]

                sorted_rate_dic = sort_dic(rate_dic) # after sorted it is not a dic but a list
                # print(sorted_rate_dic)

                tracks_to_recommend = []
                if len(tracks_to_recommend) < 3:
                    tracks_to_recommend = get_3tracks_not_to_recommend(sorted_rate_dic, class_dic)
                    # print(tracks_to_recommend)

                tracks_not_to_recommend = []
                if len(tracks_not_to_recommend) < 3:
                    tracks_not_to_recommend = get_3tracks_not_to_recommend(sorted_rate_dic, class_dic)
                # print(tracks_not_to_recommend)

                if tracks_to_recommend != tracks_not_to_recommend:
                    training_user_tracks_data.write(user_id + "|" + "6")
                    for track in tracks_to_recommend:
                        training_user_tracks_data.write(track + "\t" + "1")
                    for track in tracks_not_to_recommend:
                        training_user_tracks_data.write(track + "\t" + "0")
                else:
                    print(sorted_rate_dic)
                    if int(sorted_rate_dic[-1][1]) > 50:
                        for track in tracks_to_recommend:
                            training_user_tracks_data.write(track + "\t" + "1")
                        for track in tracks_not_to_recommend:
                            training_user_tracks_data.write(track + "\t" + "1")
                    else:
                        for track in tracks_to_recommend:
                            training_user_tracks_data.write(track + "\t" + "0")
                        for track in tracks_not_to_recommend:
                            training_user_tracks_data.write(track + "\t" + "0")


                lines_item_rate = user_item_rate_class_data.readline()
                print(lines_item_rate)
                if lines_item_rate:
                    [user_id, user_rates_num] = lines_item_rate. \
                        strip("\n").split("|")
                    lines_item_rate = read_lines(user_item_rate_class_data, int(user_rates_num))

main()



