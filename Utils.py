import re
from nltk import pos_tag, WordNetLemmatizer, word_tokenize
from nltk.corpus.reader import wordnet
from Setting import *
from DBController import DBController
import os, csv

lemmatizer = WordNetLemmatizer()


def getWordListFilePath(wordType):
	if wordType == WORD_FILTER:
		return 'word/filterWord.csv'
	elif wordType == WORD_CAUSE_EX:
		return 'word/causality_ext.csv'
	elif wordType == WORD_CAUSE_IN:
		return 'word/causality_int.csv'
	elif wordType == WORD_CONTROL_LOW:
		return 'word/controlability_low.csv'
	elif wordType == WORD_CONTROL_HIGH:
		return 'word/controlability_high.csv'
	elif wordType == WORD_UNCERTAIN:
		return 'word/LoughranMcDonald_Uncertainty.csv'

def getWordList(wordType):
	with open(getWordListFilePath(wordType)) as f:
		return [word.strip().lower() for word in f.readlines()]


def getWordDict(wordType):
	wordList = getWordList(wordType)
	return dict(zip(wordList, [0] * len(wordList)))

def getWordRegexPattern(wordType):
	#check the word is unigram or bigram, then use different pattern paradigm
	wordList = getWordList(wordType)
	wordPatternStringList = []
	for wordString in wordList:
		#patternString = r'\b' + (wordString.split()[0] + r'( [\w\d]+)* ') + wordString.split()[1] + r'\b'
		wordPatternString = wordString if 1 == len(wordString.split()) else ' '.join(wordString.split())
		wordPatternString = r'\b' + wordPatternString + r'\b'
		wordPatternStringList.append(wordPatternString)
	patternString = r'|'.join(wordPatternStringList)
	pattern = re.compile(patternString, re.IGNORECASE)
	return pattern


def getMatchWordListFromPattern(text, pattern, filterWordDict):
	#filter and lemmatize the input text
	text = ' '.join(sentenceToWordList(text, filterWordDict))
	return pattern.findall(text)


def lemmatize(word):
	lemmatizedWord = lemmatizer.lemmatize(word, NOUN)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, VERB)
	if lemmatizedWord != word:
		return lemmatizedWord
	lemmatizedWord = lemmatizer.lemmatize(word, ADJ)
	if lemmatizedWord != word:
		return lemmatizedWord
	return lemmatizer.lemmatize(word, ADV)


def sentenceToWordList(sentence, filterWordDict=None):
	#use this to extract keyword
	if filterWordDict is not None:
		wordList = [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalnum(word)]
		return [word for word in wordList if word not in filterWordDict]
	else:
		return [lemmatize(word.lower().strip()) for word in sentence.split() if unicode.isalnum(word)]


def splitListIntoChunk(inputList, chunkNumber):
	chunkList = []
	chunkSize = 1 if (len(inputList) / chunkNumber == 0) else len(inputList) / chunkNumber
	for i in range(0, len(inputList), chunkSize):
		chunkList.append(inputList[i : i + chunkSize])
	return chunkList


#FILE load
def loadAllDialoguesFromFile(speakerTypeFilePath, folderPath):
	db = DBController()
	db.dropDB()
	ensuredIndex = False
	ADict, CDict, JDict, DotDict = {}, {}, {}, {}
	#load the speaker type csv file
	with open(speakerTypeFilePath, 'rU') as f:
		lines = csv.reader(f)
		for i, line in enumerate(lines):
			if i == 0:
				continue
			speakerName, speakerType, speakerId = line[14].strip(), line[15].strip().upper(), line[16].strip()
			if speakerType == TYPE_ANALYST:
				ADict[speakerName] = speakerId
			elif speakerType == TYPE_CEO:
				CDict[speakerName] = speakerId
			elif speakerType == TYPE_JOURNALIST:
				JDict[speakerName] = speakerId
			elif speakerType == TYPE_DOT:
				DotDict[speakerName] = speakerId
			else:
				print(speakerName, speakerType)


	for dirPath, dirNames, fileNames in os.walk(folderPath):
		print(dirPath)
		if os.path.split(dirPath)[-1].startswith('chunk'):
			for fileName in fileNames:
				try:
					if fileName.endswith('txt'):
						fileNameParts = [part.strip() for part in fileName.split('.txt')[0].split('_')]
						company, time = fileNameParts[0], fileNameParts[1]
						sessionType, sessionOrder, asker, answerer = fileNameParts[2], int(fileNameParts[3]), fileNameParts[4], fileNameParts[5]
						if fileNameParts[-1].endswith('default') or fileNameParts[-1].endswith('copy'):
							continue
						elif fileNameParts[-1][-1].isdigit() and not fileNameParts[-1][-2].isdigit():
							speakerName = fileNameParts[-1][:-1].strip()
							speechOrder = int(fileNameParts[-1][-1:])
						elif fileNameParts[-1][-1].isdigit() and fileNameParts[-1][-2].isdigit():
							speakerName = fileNameParts[-1][:-2].strip()
							speechOrder = int(fileNameParts[-1][-2:])
						else:
							continue
						conference =  db.getConferenceByCompanyTime(company, time)
						if conference is None:
							conference = {'company' : company, 'time' : time}
							conference = db.insertConference(conference)

						session = db.getSessionByConferenceAndOrder(conference['_id'], sessionOrder)
						if session is None:
							session = {'conference' : conference['_id'], 'order' : speechOrder, 'type' : sessionType, 'asker' : asker, 'answerer' : answerer}
							session = db.insertSession(session)

						speech = db.getSpeechByConferenceIdAndSessionIdAndOrder(conference['_id'], session['_id'], speechOrder)
						if speech is None:
							if speakerName in ADict:
								speakerType, speakerId = TYPE_ANALYST, ADict[speakerName]
							elif speakerName in CDict:
								speakerType, speakerId = TYPE_CEO, CDict[speakerName]
							elif speakerName in JDict:
								speakerType, speakerId = TYPE_JOURNALIST, JDict[speakerName]
							elif speakerName in DotDict:
								speakerType, speakerId = TYPE_DOT, DotDict[speakerName]
							else:
								speakerType, speakerId = TYPE_DOT, ''
								print(fileName, speakerName)

							filePath = os.path.join(dirPath, fileName)
							with open(filePath, 'rU') as f:
								text = ' '.join(f.readlines()).strip()
								text = text.decode('ascii', 'ignore').encode('ascii', 'ignore')
								speech = {'conference' : conference['_id'], 'session' : session['_id'], 'order' : speechOrder, 'text' : text,
								          'speakerName' : speakerName, 'speakerType' : speakerType, 'speakerId' : speakerId}
								db.insertSpeech(speech)
						if not ensuredIndex:
							db.ensureIndex()
							ensuredIndex = True
				except Exception as e:
					print(fileName)
					print(e)
