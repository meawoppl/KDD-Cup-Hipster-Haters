from scipy import *
from tables import *

# Load the mean ratings
mean_ratings_h5 = openFile("track2-mean-item-ratings.h5")

i = mean_ratings_h5.root.item_means.cols.item[:]
m = mean_ratings_h5.root.item_means.cols.mean[:]

item_to_mean = {}
item_to_mean.update(zip(i, m))

def top_three(seq):
    m = median(array(seq))
    number_in = 0
    answers = []

    for x in seq:
        if number_in >= 3:
            answers.append(0)
            continue
        if x >= m:
            answers.append(1)
            number_in += 1
            continue
        if x < m:
            answers.append(0)
            continue
    assert sum(array(answers)) == 3, "WTF %s %s" % ( str(seq), answers)
    return answers

# Load the 
to_rate_h5 = openFile("track2-full.h5")

sub_file = open("t2-submission.txt", "w")

current_user = to_rate_h5.root.test[0]["user"]
user_items = []
for row in to_rate_h5.root.test.iterrows():
    # If we have moved onto the next user:
    if row["user"] != current_user:
        mean_ratings = [item_to_mean[i] for i in user_items]
        six_chars = [str(i) for i in top_three(mean_ratings)]
        assert len(six_chars) == 6, "WTF"

        sub_file.write("".join(six_chars))

        if (current_user % 1000) == 0: print current_user
        user_items = []
        current_user = row["user"]

    user_items.append(row["item"])

mean_ratings = [item_to_mean[i] for i in user_items]
six_chars = [str(i) for i in top_three(mean_ratings)]
assert len(six_chars) == 6, "WTF"
sub_file.write("".join(six_chars))
