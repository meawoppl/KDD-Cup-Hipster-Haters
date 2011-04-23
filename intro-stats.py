from tables import *
from scipy import *

import os

track_no = 1

# Compute the mean and stdev for each user:

class UserMeanStd(IsDescription):
    user = UInt32Col(pos=0)
    num  = UInt32Col(pos=1)
    mean = Float64Col(pos=2)
    std  = Float64Col(pos=3)

class NormedRatingT1(IsDescription):
    item = UInt32Col(pos=0)
    user = UInt32Col(pos=1)
    rate = Float64Col(pos=2)
    time = UInt32Col(pos=3)

class NormedRatingT2(IsDescription):
    item = UInt32Col(pos=0)
    user = UInt32Col(pos=1)
    rate = Float64Col(pos=2)


class ItemMeanStd(IsDescription):
    item = UInt32Col(pos=0)
    mean = Float64Col(pos=1)
    std  = Float64Col(pos=2)


def compute_user_stats(infile, outfile, track): 
    # Open the input/output files
    in_h5 = openFile(infile)
    out_h5 = openFile(outfile, "w")

    # Compute the unique users
    users = array(list(set(in_h5.root.training.cols.user[:])))
    users.sort()

    # Create a table and collector for the per/user statistics
    stats = []
    user_stats = out_h5.createTable("/", "user_rating_stats", UserMeanStd, expectedrows=1000000)
    for user in users:
        # this tables is common to track one and track 2
        user_ratings = in_h5.root.training.readWhere("user==%iL" % user)
    
        # Pop the validation stuff in there too for track 1
        if track == 1:
            user_ratings = concatenate((user_ratings, in_h5.root.validation.readWhere("user==%iL" % user)))

        # Number of ratings, mean and standard deviation
        n = user_ratings["rate"].size
        mean = user_ratings["rate"].mean()
        stdev = user_ratings["rate"].std()

        # Periodic status update . . . this takes about 10 min to run on track 2
        if (user % 1000) == 0:
            print user, n, mean, stdev

        # Outlier detection
        if n > 10000:
            print "Holy crap.  User #%i rated %i songs!" % (user, n)

        # Stick this users data in the aggregator array
        stats.append(tuple([user, n, mean, stdev]))

    # Put the array in the h5 and close it
    user_stats.append(stats)
    out_h5.close()

def compute_normed_ratings(in_ratings_h5, in_stats_h5, out_h5_name, track):
    '''This function computes ratings normalized (z-scored) by the users mean and standard deviation'''
    # Open the input files . . . 
    stats_h5 = openFile(in_stats_h5)
    rates_h5 = openFile(in_ratings_h5)

    # Open the output H5
    filters = Filters(complevel=3, complib='blosc', shuffle=True)
    out_h5 = openFile(out_h5_name, "w", filters=filters)

    # Setup the output table format
    if track == 1:
        format = NormedRatingT1
    elif track == 2:
        format = NormedRatingT2
    normed_rating_table = out_h5.createTable("/", "normalized", format)

    # Load the unique user information (everything in the validation is in here too so . . .)
    unique_users = array(list(set(list(rates_h5.root.training.cols.user[:]))))
    unique_users.sort()

    # Load the user means and stdevs
    un = stats_h5.root.user_rating_stats.cols.user[:]
    me = stats_h5.root.user_rating_stats.cols.mean[:]
    st = stats_h5.root.user_rating_stats.cols.std[:]

    # Make snappy lookups for these. . .
    un_to_mean = {}
    un_to_mean.update( zip(un, me) )

    un_to_std = {}
    un_to_std.update( zip(un, st) )

    # For each user . . .
    for user_number in unique_users:
        # Slice out the points associated with a user
        userdata = rates_h5.root.training.readWhere("user==%iL" % user_number)
        
        if track == 1:
            userdata = concatenate((userdata, rates_h5.root.training.readWhere("user==%iL" % user_number)))

        user, item, rate = userdata["user"], userdata["item"], userdata["rate"]

        # Upcast to deal with non-int
        rate = rate.astype(float64)

        # Find his mean, and std from the earlier computations
        user_mean = un_to_mean[user_number]
        user_std  = un_to_std[user_number]
                
        # Norm the ratings.
        rate = (rate - user_mean) / user_std
        # This raises a lot of zero-div errors, so . . .
        invalid = (isnan(rate) | isinf(rate))
        rate[invalid] = 0

        # Stick it in the table
        if track == 1:
            normed_rating_table.append(zip(item, user, rate, userdata["time"]))
        if track == 2:
            normed_rating_table.append(zip(item, user, rate))

        if (user_number % 1000) == 0: print user_number

    print "Creating Indices"
    normed_rating_table.cols.user.createIndex()
    normed_rating_table.cols.item.createIndex()
    print "Done"
    # Close up shop
    stats_h5.close()
    rates_h5.close()
    out_h5.close()


def compute_item_means(in_h5_path, out_h5_path):
    # Open the normed input file and Open the file we are going to record the item means into
    in_h5 = openFile(in_h5_path)
    out_h5 = openFile(out_h5_path, "w")

    # Make a table for the means
    item_means_table = out_h5.createTable("/", "item_means", ItemMeanStd)    
   
    # Find the unique items
    unique_items = arange(in_h5.root.normalized.cols.item[:].max()+1)

    # Store the results in these
    item_means  = zeros(unique_items.shape, dtype=float64)
    item_stdevs = zeros(unique_items.shape, dtype=float64)

    # Iterate over them
    print len(unique_items)
    for i, item in enumerate(unique_items):
        # Grab the records for that item
        ratings = in_h5.root.normalized.readWhere("item==%iL" % item)
        
        # Calculate the mean and stdev
        this_item_mean = ratings["rate"].mean()
        this_item_std  = ratings["rate"].std()
        if (i % 1000) == 0: print i, ratings["rate"].mean(), ratings["rate"].std()
        
        item_means_table.append([tuple([item, this_item_mean, this_item_std])])

    in_h5.close()
    out_h5.close()
        
# Base "raw" data file
full_data_path = "track%i-data/track%i-full.h5" % (track_no, track_no)

# User based statistics
simp_stats_file = "track%i-data/track%i-userstats.h5" % (track_no, track_no)

# Normalized ratings
normalized_ratings_file = "track%i-data/track%i-normed-ratings.h5" % (track_no, track_no)

# Item mean ratings
item_means_file = "track%i-data/track%i-mean-item-ratings.h5" % (track_no, track_no)

# This computes the user mean, numebr of ratings, and standard deviation
if not os.path.isfile(simp_stats_file):
    print "Computing User Stats:"
    compute_user_stats(full_data_path, simp_stats_file, track_no)
    print "\tDone"
else:
    print "User stats already completed!"

# This normalizes the ratings based on the numbers above
if not os.path.isfile(normalized_ratings_file):
    print "Normalizing the ratings based on the user stats"
    compute_normed_ratings(full_data_path, simp_stats_file, normalized_ratings_file, track_no)
    print "\tDone"
else:
    print "Normalized ratings already completed!"


# Now we want to compute the mean song score.  This is useful for a number of other things.
if not os.path.isfile(item_means_file):
    print "Finding the mean user score for each item"
    compute_item_means(normalized_ratings_file, item_means_file)
    print "\tDone"
else:
    print "Item means already completed!"
