from tables import *
from gensim import corpora, models, similarities
import logging, time, os
from scipy import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

track_number = 1
take_number = 1

h5 = openFile("../track%i-normed-ratings.h5" % track_number)
h5_tax = openFile("../track%i-full.h5" % track_number)

print "Loading track %i taxonomy information . . . ." % track_number
print "\tTrack->Artist"
track_artist = {}
track_artist.update( zip(h5_tax.root.tax.track_artist.cols.track[:],
                         h5_tax.root.tax.track_artist.cols.artist[:]))

print "\tTrack->Album"
track_album = {}
track_artist.update( zip(h5_tax.root.tax.track_album.cols.track[:],
                         h5_tax.root.tax.track_album.cols.album[:]))

print "\tTrack->Genre"
track_genre = {}
for t, g in zip(h5_tax.root.tax.track_genre.cols.track[:], h5_tax.root.tax.track_genre.cols.genre[:]):
     if t in track_genre:
          track_genre[t] += [g]
     else:
          track_genre[t] = [g]

folder_dest = os.path.join("track%i" % track_number, "take%i" % take_number)

def complicate(items, ratings):
     # This complicates the ratings data of tracks only.  It should leave the other ones alone . . . 
     items_to_sum = {}
     items_to_num = {}

     # Go through the items-rating pairs supplies for the user
     for item, rating in zip(items, ratings):
          # Make a list of things this rating applies to (nothing should happen for non-track data here)
          associated_items = [item]
          if item in track_artist: associated_items.append( track_artist[item])
          if item in track_album:  associated_items.append(  track_album[item] )
          if item in track_genre:  associated_items   +=     track_genre[item] 

          # Populate the dictionaries that keep a sum of ratings as well as number of values in that sum
          for aitem in associated_items:
               if aitem in items_to_sum:
                    items_to_sum[aitem] += rating
                    items_to_num[aitem] += 1
               else:
                    items_to_sum[aitem] = rating
                    items_to_num[aitem] = 1

     # Now it is possible (likely) that the user has more than one 
     # rating per item (album genre artist etc, so compute the averages
     new_items   = []
     new_ratings = []
     for item in items_to_sum.iterkeys():
          new_items.append(item)
          new_ratings.append(items_to_sum[item] / items_to_num[item])

     return new_items, new_ratings

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

             # TODO: Track 1 also has the validation set, which we will throw in here.
             # if track_number == 1:
             #      data = concatenate( data, h5.root.validation.readWhere("(user==%iL)" % user) )
             
             # Add genere-artist-album etc.
             citems, cratings = complicate( data["item"], data["rate"] )

             # Status update
             if (user % 1000) == 0: print user, "/", users.size
             
             yield zip(citems, cratings)

print "Serializing Corpus . . .", time.time()
track2 = MyCorpus()
corpus_fn = os.path.join(folder_dest, "track%i-corpus.svmlight" % track_number)
corpora.SvmLightCorpus.serialize(corpus_fn, track2)
print "Done", time.time()
