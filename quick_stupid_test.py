from scipy import *
from scipy import sparse
from tables import *

import gensim

h5 = openFile("track2-normed-ratings.h5")

items = h5.root.training.cols.item[:]
uitems = array( list(set(list(items))) )
uitems.sort()

print "Items Stupid Check", all( uitems == arange(uitems.size) )

users = h5.root.training.cols.user[:]
uusers = array( list(set(list(users))) )
uusers.sort()

print "Users Stupid Check", all( uusers == arange(uusers.size) )

rates = h5.root.training.cols.rate[:]

ui_coo_matrix = sparse.coo_matrix( (rates, (users, items)), shape=(uusers.size, uitems.size))

results = gensim.models.lsimodel.stochasticSvd(ui_coo_matrix, 1000, items.size, power_iters=2)
# gensim.models.lsimodel.stochasticSvd(corpus, rank, num_terms, chunks=20000, extra_dims=None, power_iters=0, dtype=<type 'numpy.float64'>, eps=9.9999999999999995e-07)
