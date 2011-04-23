from scipy import random
from tables import *
import sys

in_h5 = openFile(sys.argv[1])
out_h5 = openFile(sys.argv[2], "w")

normalized_ratings_table = in_h5.root.training # TODO: change to normalized

training_table = out_h5.createTable("/", "training", normalized_ratings_table.description)
validation_table = out_h5.createTable("/", "validation", normalized_ratings_table.description)

max_user_number = normalized_ratings_table.cols.user[:].max()

for user_number in xrange(max_user_number + 1):
    this_users_ratings = normalized_ratings_table.readWhere("user==%iL" % user_number)

    random.shuffle(this_users_ratings)

    training_table.append(this_users_ratings[4:])
    validation_table.append(this_users_ratings[0:5])

    if user_number % 1000 == 0: print user_number



