from nltk import pos_tag, WordNetLemmatizer, word_tokenize
from nltk.corpus.reader import wordnet
from FileUtils import loadWordDict
from Setting import *

class TextProcessor(object):
	def __init__(self):
		self._filterWordDict = loadWordDict(WORD_FILTER)
		self._lemmatizer = WordNetLemmatizer()

	def getWordnetPOSFromTag(self, POSTag):
		if POSTag.startswith('ADJ'):
			POS = wordnet.ADJ
		elif POSTag.startswith('ADV'):
			POS = wordnet.ADV
		elif POSTag.startswith('V'):
			POS = wordnet.VERB
		else:
			POS = wordnet.NOUN
		return POS

	def getFilteredWordList(self, sentenceString, filterMode):
		if filterMode == MODE_BASIC:
			filteredWordList = []
			for word in word_tokenize(sentenceString):
				word = word.lower()
				if not word.isalpha() or len(word) < 3:
					continue
				lemmatizedWordList = [self._lemmatizer.lemmatize(word, wordnet.NOUN), self._lemmatizer.lemmatize(word, wordnet.VERB),
				                      self._lemmatizer.lemmatize(word, wordnet.ADJ), self._lemmatizer.lemmatize(word, wordnet.ADV)]
				for i, lemmatizedWord in enumerate(lemmatizedWordList):
						if lemmatizedWord != word:
							break
				if lemmatizedWordList[i] not in self._filterWordDict:
						filteredWordList.append(lemmatizedWord)
		else:
			wordlist = [word.lower() for word in word_tokenize(sentenceString)]
			filteredWordList = []
			POSTupleList = pos_tag(wordlist)
			for word, POSTag in POSTupleList:
				wordnetPOS = self.getWordnetPOSFromTag(POSTag)
				if wordnetPOS != wordnet.NOUN:
					continue
				word = self._lemmatizer.lemmatize(word, wordnetPOS)
				if word not in self._filterWordDict and word.isalpha() and len(word) > 2:
					filteredWordList.append(word)
		return filteredWordList


def splitListIntoChunk(inputList, chunkNumber):
	chunkList = []
	chunkSize = len(inputList) / chunkNumber
	for i in range(0, len(inputList), chunkSize):
		chunkList.append(inputList[i : i + chunkSize])
	return chunkList