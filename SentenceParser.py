from gensim import corpora
from gensim.models import ldamodel, lsimodel
from Setting import *
from DBController import DBController
from TextUtils import TextProcessor

class SentenceParser(object):

	def __init__(self):
		self._db = DBController()
		self.textProcessor = TextProcessor()

	def getTopicModelWordMatrix(self, sentenceList, topicNumber, topicModelAlgorithm, filterMode, keepScore):
		wordMatrix = []
		for sentence in sentenceList:
			wordMatrix.append(self.textProcessor.getFilteredWordList(sentence, filterMode))

		dictionary = corpora.Dictionary(wordMatrix)
		corpus = [dictionary.doc2bow(sentence) for sentence in wordMatrix]

		if topicModelAlgorithm == ALGORITHM_LDA:
			model = ldamodel.LdaModel(corpus, id2word=dictionary, num_topics=topicNumber)
		else:
			model = lsimodel.LsiModel(corpus, id2word=dictionary, num_topics=topicNumber)

		topicModelWordMatrix = []
		for i in range(0, model.num_topics):
			if keepScore:
				topicWordList = []
				for string in model.print_topic(i).split(' + '):
					[score, word] = string.split('*')
					topicWordList += [word, float(score)]
			else:
				topicWordList = [string.split('*')[-1] for string in model.print_topic(i).split(' + ')]
			topicModelWordMatrix.append(topicWordList)
		return topicModelWordMatrix