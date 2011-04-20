from tables import *
from gensim import corpora, models, similarities
import logging, time, os
from scipy import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

h5 = openFile("../track2-normed-ratings.h5")

dictionary = corpora.Dictionary()

folder_dest = "take3"

class MyCorpus(object):
     def __iter__(self):
         # Grab all the users (texts)
         print "Computing Unique Users"
         users = array(list(set(list(h5.root.training.cols.user[:]))))
         print "Sorting. . ."
         users.sort()

         for user in users:
              # Grab the ratings that the user rated positivly
             data = h5.root.training.readWhere("(user==%iL)" % user)

             # TODO: Add genere-artist-album etc. here?

             if (user % 1000) == 0: print user, "/", users.size

             # Get unique token numbers, and return to serializer
             yield zip(data["item"], data["rate"])
             # yield dictionary.doc2bow(ratings, allowUpdate=True)

                         

                         


print "Serializing Corpus . . .", time.time()
track2 = MyCorpus()
corpus_fn = os.path.join(folder_dest, "track2-corpus.svmlight")
corpora.SvmLightCorpus.serialize(corpus_fn, track2)
print "Done", time.time()

print "Saving Dictionary", time.time()
dict_fn = os.path.join(folder_dest, 'track2.dict')
dictionary.save(dict_fn) # store the dictionary, for future reference
print "Done", time.time()
