from scipy import *
from tables import *
import gensim, sys, itertools

# assert len(sys.argv) == 4

# Load the SVD and accompanying dictionary
svd_obj = gensim.utils.SaveLoad.load(sys.argv[1])
svd_mat = matrix( svd_obj.projection.u ).T
svd_mag = svd_obj.projection.s
print "Loaded SVD", svd_mat.shape, type(svd_mat)

#load the h5 with the users ratings
h5_ntraining = openFile(sys.argv[2])

# Load the h5 file with the data we have to test against
h5_test_data = openFile(sys.argv[3])

# Extract the data we have to predict
h5_user_test = h5_test_data.root.test[:]

# Compute the users that we have to make predictions for
unique_users = list(set(list(h5_user_test["user"])))
unique_users.sort()

rating_vector = zeros(svd_mat.shape[1], dtype=float64)

sub_file = open("submission.txt", "w")

# For each user that we have to make predictions about
for current_user in unique_users:
    # Fetch the test ratings
    is_this_user = h5_user_test["user"] == current_user
    test_songs = h5_user_test["item"][is_this_user]

    # Fetch the users (normalized, nonnegative song ratings)
    training = h5_ntraining.root.training.readWhere("(user == %iL)" % current_user)

    # Compute the user training vector
    # We avoid deleting this large vector to avoid trashing the memory-caching
    rating_vector[:] = 0
    for item, rate in zip(training["item"], training["rate"]):
        rating_vector[item] = rate + 0.001

    # Project the users traing data against the SVD -> store vector
    user_pref_vector = array( svd_mat * rating_vector[:, newaxis]).flatten() * svd_mag

    # Normalize it . . .
    user_pref_vector /= sqrt(sum(user_pref_vector**2))

    # Varibles to store the best projections
    best_projection = -inf       # Very large angle b/t preverences and tried set
    best_tuple      = ()

    # For each possible permutation (3 in 3 out)
    for group_of_three in itertools.combinations(test_songs, 3):
        # Add those three to the user projection
        rating_vector[:] = 0

        for item in group_of_three:
            rating_vector[item] = 1.0
            
        # Current preference vector
        current_pref_vector = array( svd_mat * rating_vector[:, newaxis] ).flatten() * svd_mag

        # Normalize to unit length
        current_pref_vector /= sqrt(sum(current_pref_vector**2))

        # Project the current pref vector vs. the actual
        # Both are already normalized, so this is the cosine distance
        proj_on_actual = dot(user_pref_vector, current_pref_vector) 

        # If this is the best answer so far . . .
        if proj_on_actual > best_projection:
            # print best_projection, "->", proj_on_actual
            best_projection = proj_on_actual
            best_tuple = group_of_three

    # Write answer for highest ranked projection
    for song in test_songs:
        if song in best_tuple:
            sub_file.write("1")
        else:
            sub_file.write("0")
    if (current_user % 1000) < 5: print current_user, best_tuple
