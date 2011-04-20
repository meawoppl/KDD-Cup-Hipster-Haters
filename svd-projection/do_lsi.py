import logging, gensim, bz2, sys
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
# load id->word mapping (the dictionary), one of the results of step 2 above
id2word = gensim.corpora
# load corpus iterator
assert len(sys.argv) == 3
# track_keys = gensim.corpora.Dictionary.load(sys.argv[1])
corp = gensim.corpora.SvmLightCorpus(sys.argv[1])

# lsi = gensim.models.lsimodel.LsiModel(corpus=corp, id2word=track_keys, numTopics=1000, onepass=False, power_iters=20)
lsi = gensim.models.lsimodel.LsiModel(corpus=corp, numTopics=750, onepass=False, power_iters=4)
lsi.save(sys.argv[2])


