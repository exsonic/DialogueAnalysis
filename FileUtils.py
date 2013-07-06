"""
Bobi Pu, bobi.pu@usc.edu
"""
from DBController import DBController
from Setting import TYPE_ANALYST, TYPE_CEO, TYPE_JOURNALIST, TYPE_DOT, WORD_FILTER
import os, csv

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

def loadWordList(wordType):
	if wordType == WORD_FILTER:
		with open('word/filterWord.txt', 'r') as f:
			wordList = [line.lower().strip() for line in f.readlines()]
	return wordList

def loadWordDict(wordType):
	wordList = loadWordList(wordType)
	return dict(zip(wordList, [''] * len(wordList)))

if __name__ == '__main__':
	loadAllDialoguesFromFile('/Users/exsonic/Developer/DialogueAnalysis/corpus/0.seglist_0701_cleaned.csv', '/Users/exsonic/Developer/Marshall_RA/chunk_done/')