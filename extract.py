from numpy import *
from tables import *
import sys, os

from time import time as t

assert len(sys.argv) == 3, "Usage: python %s <track folder> <out.h5>" % sys.argv[0]

path_to_raw_data = sys.argv[1] # "../../track1/"

class TrackAlbum(IsDescription):
    track = UInt32Col(pos=0)
    album = UInt32Col(pos=1)

class TrackArtist(IsDescription):
    track  = UInt32Col(pos=0)
    artist = UInt32Col(pos=1)

class TrackGenre(IsDescription):
    track = UInt32Col(pos=0)
    genre = UInt32Col(pos=1)

class AlbumArtist(IsDescription):
    album  = UInt32Col(pos=0)
    artist = UInt32Col(pos=1)

class AlbumGenre(IsDescription):
    album = UInt32Col(pos=0)
    genre = UInt32Col(pos=1)

class AlbumGenre(IsDescription):
    album = UInt32Col(pos=0)
    genre = UInt32Col(pos=1)

class ArtistGenre(IsDescription):
    artist = UInt32Col(pos=0)
    genre  = UInt32Col(pos=1)
    
class T1UserRating(IsDescription):
    user = UInt32Col(pos=0)   # User ID
    item = UInt32Col(pos=1)   # Song ID
    rate = UInt8Col(pos=2)    # Rating [0-100]
    date = UInt32Col(pos=3)   # 0 - ? (number of days from some point . . .)
    time = UInt32Col(pos=4)   # 0 - ? (Min from day start?)

class T2UserRating(IsDescription):
    user = UInt32Col(pos=0)   # User ID
    item = UInt32Col(pos=1)   # Song ID
    rate = UInt8Col(pos=2)    # Rating [0-100]

class T1Test(IsDescription):
    user = UInt32Col(pos=0)   # User ID
    item = UInt32Col(pos=1)   # Song ID
    date = UInt32Col(pos=2)   # 0 - ? (number of days from some point . . .)
    time = UInt32Col(pos=3)   # 0 - ? (Min from day start?)

class T2Test(IsDescription):
    user = UInt32Col(pos=0)   # User ID
    item = UInt32Col(pos=1)   # Song ID

if   "track1" in path_to_raw_data:
    comp_track_number = 1
elif "track2" in path_to_raw_data:
    comp_track_number = 2
else:
    raise ValueError("WTF?")

filters = Filters(complevel=3, complib='blosc', shuffle=True)
h5 = openFile(sys.argv[2], "w", filters=filters)

print "Parsing Album and Track Data"
albums = set([])
artists = set([])
genres = set([])
tracks = set([])

track_info = open(os.path.join(path_to_raw_data, "trackData%i.txt" % comp_track_number))
tax_group = h5.createGroup("/", "tax", "Item Taxonomy information")

t_al  = h5.createTable(tax_group, "track_album",  TrackAlbum , expectedrows=1000000)
t_ar  = h5.createTable(tax_group, "track_artist", TrackArtist, expectedrows=1000000)
t_g   = h5.createTable(tax_group, "track_genre",  TrackGenre , expectedrows=1000000)
al_ar = h5.createTable(tax_group, "album_artist", AlbumArtist, expectedrows=1000000)
al_g  = h5.createTable(tax_group, "album_genre",  AlbumGenre , expectedrows=1000000)
ar_g  = h5.createTable(tax_group, "artist_genre", ArtistGenre, expectedrows=1000000)

lasttime = t()
for n, track in enumerate(track_info):
    track_data = track.strip().split("|") 

    track, album, artist = track_data[0:3]
    gens = [int(g) for g in track_data[3:] if g != "None"]

    assert (track != "None"), "Wtf . . . no track?"    
    tracks.add(int(track))

    # This is really confusing.  My apologies:
    # If there is no genre data, the list comprehensions just dont run
    # Doing it this way minimizes the amount of non-compiled conditional logic

    # Track -> Genres
    [t_g.append([tuple([int(track), g])]) for g in gens]

    # If we have album data in the current record
    if album != "None":
        # Track -> Album
        t_al.append([tuple([int(track), int(album)])])
        # Album -> Genres
        [al_g.append([tuple([int(album), g])]) for g in gens]

    # If we have artist data in the current record
    if artist != "None":
        # Track -> Artist
        t_ar.append([tuple([int(track), int(artist)])])
        # Artist -> Genres
        [ar_g.append([tuple([int(artist), g])]) for g in gens]
    

    # If we have artist and album data in the current record
    if (album != "None") and (artist != "None"):
        # Album -> Artist
        al_ar.append([tuple([int(album), int(artist)])])

    # Item set accounting
    [genres.add(g) for g in gens]
    if artist != "None": artists.add(int(artist))
    if album != "None": albums.add(int(album))

    # Add multiple track data to deal with multi-genera issue
    if ((n % 10000) == 0) and (n != 0):  
        rate = 10000. / (t() - lasttime)
        print "%i done (%f rows/second)" % (n, rate)
        lasttime = t()

    # TODO: Testing only!!
    # if n > 20000: 
    #     print "TODO!"
    #     break 

def ratings_file_to_h5(filepath, h5_obj, h5_path, track=1, test=False, rows=int(1e6)):
    # Pick the right class for track 1v2 data
    if   (track == 1) and (test == False):
        output_format = T1UserRating
    elif (track == 2) and (test == False):
        output_format = T2UserRating
    if   (track == 1) and (test == True):
        output_format = T1Test
    elif (track == 2) and (test == True):
        output_format = T2Test

    # Make a hdf5 table
    rating_table = h5_obj.createTable("/", h5_path, output_format, expectedrows=rows)


    # Start the timer . . .
    global time
    lasttime = t()
    # Open the file to read
    line_count = 0
    training_set = open(filepath, 'r')
    for user_no, line in enumerate(training_set):
        user_number, rating_number = line.strip().split("|")
        user_number = int(user_number)
        rating_number = int(rating_number)
        line_count += 1

        # Catch all the existing/desired ratings for a given user in this list . . . 
        aggregator = []
        for rating_line in range(rating_number):
            rating_data = training_set.next()
            # Diff formats for track 1 vs track 2
            split_up_line = rating_data.strip().split() 
            if   (track == 1) and (test == False):
                # Track one data has date, hh:mm:ss also so. . .
                item_number, rating, date, time = split_up_line
                item_number = int(item_number)
                rating      = int(rating)
                date        = int(date)
                hrs, mins, secs = time.split(":")
                min_time = int(mins) + (60*int(hrs))
                if int(secs) != 0:
                    raise ValueError("WTF")
                row_data = tuple([user_number, item_number, rating, date, min_time])
            elif (track == 1) and (test == True):
                # Track one data has date, hh:mm:ss also so. . .
                item_number, date, time = split_up_line
                item_number = int(item_number)
                date        = int(date)
                hrs, mins, secs = time.split(":")
                min_time = int(mins) + (60*int(hrs))
                if int(secs) != 0:
                    raise ValueError("WTF")
                row_data = tuple([user_number, item_number, date, min_time])
            elif (track==2) and (test==False):
                item_number, rating = rating_data.strip().split()
                item_number = int(item_number)
                rating      = int(rating)
                row_data = tuple([user_number, item_number, rating])
            elif (track==2) and (test==True):
                item_number = rating_data.strip().split()[0]
                item_number = int(item_number)
                row_data = tuple([user_number, item_number])

            aggregator.append(row_data)
            line_count += 1
        rating_table.append(aggregator)

        if ((user_no % 1000) == 0) and (user_no != 0):
            rate =  1000. / (t() - lasttime)
            print "Processed %i users, %i ratings (%f users/second)" % (user_no, line_count, rate)
            lasttime = t()
        # TODO: Debug Only!
        # if user_no > 5000: 
        #     print "TODO!"
        #     break

    print "Creating Index To User and Item"
    start_time = t()
    rating_table.cols.user.createIndex()
    rating_table.cols.item.createIndex()
    print "Done %f seconds. . ." % (t() - start_time)
    
all_items = set([])

if comp_track_number == 1:
    train_rows = 252800275
    test_rows  = 6005940
elif comp_track_number == 2:
    train_rows = 61944406
    test_rows  = 607032

# Both track 1 and 2 have these training/test files . . .
training_path = os.path.join(path_to_raw_data, "trainIdx%i.txt" % comp_track_number)
ratings_file_to_h5(training_path, h5, "training", comp_track_number, test=False, rows=train_rows)

test_path = os.path.join(path_to_raw_data, "testIdx%i.txt" % comp_track_number)
ratings_file_to_h5(test_path, h5, "test", comp_track_number, test=True, rows=test_rows)

# Track one has explicit training-validation sets so . . .
if comp_track_number == 1:
    validation_path = os.path.join(path_to_raw_data, "validationIdx1.txt")
    ratings_file_to_h5(validation_path, h5, "validation", 
                       comp_track_number, test=False, rows=4003960)

# # Item Arithmetic to get song numbers
# all_items.add(h5.training.getCol("item"))
# all_items.add(h5.test.getCol("item"))

# if comp_track_number == 1:
#     all_items.add(h5.validation.getCol("item"))
# all_items nows contains all the items seen in the training/validation/test sets


items_group = h5.createGroup("/", "items", "Lists of item types")

print "\tDone.  Saving unique track list . . . "
tracks = array(list(tracks))
tracks.sort()
trks = h5.createArray(items_group, "tracks", tracks)
print "\t Done"
print trks

print "\tDone.  Saving unique album list . . . "
albums = array(list(albums))
albums.sort()
albs = h5.createArray(items_group, "albums", albums)
print "\t Done"
print albs

print "\tDone.  Saving unique artist list . . . "
artists = array(list(artists))
artists.sort()
arts = h5.createArray(items_group, "artists", artists)
print "\t Done"
print arts

print "\tDone.  Saving unique genre list . . . "
genres = array(list(genres))
genres.sort()
gens = h5.createArray(items_group, "genres", genres)
print "\t Done"
print gens

h5.close()
