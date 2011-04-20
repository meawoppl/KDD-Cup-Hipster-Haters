from scipy import *
from tables import *

# Load the mean ratings
mean_ratings_h5 = openFile("track1-mean-item-ratings.h5")

i = mean_ratings_h5.root.item_means.cols.item[:]
m = mean_ratings_h5.root.item_means.cols.mean[:]

item_to_mean = {}
item_to_mean.update(zip(i, m))

# Load user mean and standard deviation
print "Loading user statistics . . ."
u_ms_h5 = openFile("track1-userstats.h5")
users = u_ms_h5.root.user_rating_stats.cols.user[:]
means = u_ms_h5.root.user_rating_stats.cols.mean[:]
stdvs = u_ms_h5.root.user_rating_stats.cols.std[:]

user_to_mean = {}
user_to_mean.update( zip( users, means) )

user_to_std  = {}
user_to_std.update( zip( users, means) )
print "/tDone."

# Load the data set which tells us which songs to rate for which user
to_rate_h5 = openFile("track1-full.h5")
sub_file = open("t1-mean-submission.txt", "w")


def score_to_char(integer_score):
    if integer_score < 0:
        integer_score = 0
    if integer_score > 100:
        integer_score = 100

    chr_roundScore = chr(int(2.55*integer_score+0.5))
    return chr_roundScore


for n, row in enumerate( to_rate_h5.root.test.iterrows() ):
    # get the golbal mean rating for the song we have to rate
    item, user = row["item"], row["user"]
    global_mean_rating = item_to_mean[item]

    # Get the users mean and std
    user_mean = user_to_mean[user]
    user_std = user_to_std[user]

    # Project the global mean against the users mean and standard deviation
    user_projected_rating = (global_mean_rating * user_std) + user_mean
    
    # Get the character associated with the score 
    char_to_write = score_to_char(user_projected_rating)
    sub_file.write(char_to_write)

    if (n % 10000) == 0: print n, "ratings down . . . "
