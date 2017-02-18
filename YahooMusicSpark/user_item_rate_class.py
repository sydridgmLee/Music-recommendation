#classify item in traindx2.txt file, it may be track, album, artist, genre;

rating_file = "raw_data/trainIdx2.txt"
album_data_file = 'raw_data/albumData2.txt'
artist_data_file = 'raw_data/artistData2.txt'
genre_data_file = 'raw_data/genreData2.txt'

# analyze above data and write result in following file
user_item_rate_class_file = "data/user_item_rate_class.csv"

# data structure
classification = {}

##############################
def classifyRating():
    with open(album_data_file) as f:
        for line in f:
            temp = line.strip("\n").split("|")
            classification[temp[0]] = 2
    # Load Artist
    with open(artist_data_file) as f:
        for line in f:
            classification[line.strip("\n")] = 3
    # Load Genre
    with open(genre_data_file) as f:
        for line in f:
            classification[line.strip("\n")] = 4
    writeFile()


def writeFile():
    with open(user_item_rate_class_file,'w') as user_item_rate_class:
        # The source file the contains training track ID
        with open(rating_file) as trainData:
            cur_user = ""
            for line in trainData:
                # Identify the "UserID|RatingCount" line
                if '|' in line:
                    temp = line.strip("\n").split("|")
                    cur_user = temp[0]
                    # print(cur_user)
                    # ratingClassification.write(cur_user+"|"+cur_user_rates+"\n")
                # Read track ID lines
                else:
                    [cur_item,cur_item_rate] = line.strip("\n").split("\t")
                    if cur_item in classification:
                        cur_item_class = classification[cur_item]
                    else:
                        cur_item_class = 1
                    user_item_rate_class.write(cur_user + "," + cur_item+","+cur_item_rate+","+str(cur_item_class)+"\n")

classifyRating()