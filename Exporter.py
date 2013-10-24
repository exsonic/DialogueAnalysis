import csv
import os
from threading import Thread
from Queue import Queue
from DBController import DBController
from Setting import *
from SentenceParser import SentenceParser
from Utils import *


class CSVWriterThread(Thread):
	def __init__(self, resultQueue, outputfilePath, attributeLineList, mode='w'):
		super(CSVWriterThread, self).__init__()
		self._resultQueue = resultQueue
		self._outputfilePath = outputfilePath
		self._attributeLineList = attributeLineList
		self._writeMode = mode
		if not os.path.exists('export/'):
			os.makedirs('export/')

	def run(self):
		i = 0
		with open(self._outputfilePath, self._writeMode) as f:
			writer = csv.writer(f)
			if self._attributeLineList:
				writer.writerow(self._attributeLineList)
			while True:
				lineList = self._resultQueue.get()
				print(i)
				i += 1
				if lineList == END_OF_QUEUE:
					self._resultQueue.task_done()
					break
				else:
					try:
						writer.writerow(lineList)
					except Exception as e:
						print(e)
					finally:
						self._resultQueue.task_done()



class ProcessThread(Thread):
	def __init__(self, taskQueue, resultQueue, *args):
		super(ProcessThread, self).__init__()
		self._taskQueue = taskQueue
		self._resultQueue = resultQueue
		self._args = args
		self._executeFunction = None
		self._db = DBController()

	def topicModelByConferenceAnalyst(self):
		[speakerType, topicNum, algorithm, keepScore] = self._args[1:]
		parser = SentenceParser()
		while True:
			conferenceList = self._taskQueue.get()
			if conferenceList == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				for conference in conferenceList:
					try:
						speechList = list(self._db.getAllSpeechByConferenceIdAndSpeakerType(conference['_id'], speakerType))
						speakerDict = {}
						for speech in speechList:
							if speech['speakerId'] not in speakerDict:
								speakerDict[speech['speakerId']] = [speech]
							else:
								speakerDict[speech['speakerId']].append(speech)

						for speakerId, speechList in speakerDict.iteritems():
							speakerName = speechList[0]['speakerName']
							sentenceList = [speech['text'] for speech in speechList]
							topicModelWordMatrix = parser.getTopicModelWordMatrix(sentenceList, topicNum, algorithm, keepScore)
							lineList = []
							for i, topicModelWordList in enumerate(topicModelWordMatrix):
								basicInfo = [conference['_id'], conference['company'], conference['time'], speakerName, speakerType, speakerId, len(speechList)]
								lineList.append(basicInfo + topicModelWordList)
							self._resultQueue.put(lineList)
					except Exception as e:
						print(e)

	def extractKeyword(self):
		speakerType = self._args[0]
		filterWordDict = getWordDict(WORD_FILTER)
		patternList = [getWordRegexPattern(WORD_CAUSE_IN), getWordRegexPattern(WORD_CAUSE_EX), getWordRegexPattern(WORD_CONTROL_LOW), getWordRegexPattern(WORD_CONTROL_HIGH), getWordRegexPattern(WORD_UNCERTAIN)]
		while True:
			conference = self._taskQueue.get()
			if conference == END_OF_QUEUE:
				self._taskQueue.task_done()
				break
			else:
				#aggregate
				speechList = list(self._db.getAllSpeechByConferenceIdAndSpeakerType(conference['_id'], speakerType))
				speakerDict = {}
				for speechDict in speechList:
					if speechDict['speakerId'] not in speakerDict:
						speakerDict[speechDict['speakerId']] = [speechDict]
					else:
						speakerDict[speechDict['speakerId']].append(speechDict)

				for speakerId, speechDictList in speakerDict.iteritems():
					lineList = [conference['_id'], conference['company'], conference['time'], speechDictList[0]['speakerName'], speakerType, speakerId]
					totalText = ' '.join([speechDict['text'] for speechDict in speechDictList])
					lineList.append(len(totalText))
					lineList += [''] * (NUM_OF_WORD_TYPE * 2)
					for i, pattern in enumerate(patternList):
						matchedWordList = getMatchWordListFromPattern(totalText, pattern, filterWordDict)
						lineList[i + 7] = len(matchedWordList)
						lineList[i + NUM_OF_WORD_TYPE + 7] = ', '.join(matchedWordList)
					self._resultQueue.put(lineList)
				self._taskQueue.task_done()


	def run(self):
		self._executeFunction()


class ExportMaster(object):
	def __init__(self):
		self._resultQueue = Queue()
		self._taskQueue = Queue()
		self._db = DBController()
		self._threadNumber = 1
		self._threadList = []

	def exportTopicModel(self, exportFilePath='export/topicModel.csv',  speakerType=TYPE_ANALYST, topicNumber=1, algorithm=ALGORITHM_LDA, keepScore=True,):

		conferenceList = list(self._db.getAllConference())
		attributeList = ['conferenceId', 'company', 'time', 'speaker', 'speaker_type', 'speaker_Id', 'speak_freq']
		for i in range(1, 11):
			attributeList.append('topic_word_' + str(i))
			attributeList.append('word_score_' + str(i))
		chunkList = splitListIntoChunk(conferenceList, self._threadNumber)
		for subConferenceList in chunkList:
			self._taskQueue.put(subConferenceList)

		for i in range(self._threadNumber):
			self._taskQueue.put(END_OF_QUEUE)

		for i in range(self._threadNumber):
			processThread = ProcessThread(self._resultQueue, speakerType, topicNumber, algorithm, keepScore)
			processThread._executeFunction = processThread.topicModelByConferenceAnalyst
			processThread.start()
			self._threadList.append(processThread)

		writerThread = CSVWriterThread(self._resultQueue, exportFilePath, attributeList)
		writerThread.start()

		for processThread in self._threadList:
			processThread.join()
		self._taskQueue.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writerThread.join()

	def exportKeywordMatch(self, filePath='export/conference_keyword.csv', speakerType=TYPE_ANALYST):
		attributeList = ['conferenceId', 'company', 'time', 'speaker', 'speaker_type', 'speaker_Id', 'total_word_count', 'cau_int', 'cau_ext', 'cont_l', 'cont_h', 'uncert', 'cau_int_word', 'cau_ext_words', 'cont_l_words', 'cont_h_words', 'uncert_words']

		conferenceList = self._db.getAllConference()
		for conference in conferenceList:
			self._taskQueue.put(conference)

		for i in range(self._threadNumber):
			self._taskQueue.put(END_OF_QUEUE)

		for i in range(self._threadNumber):
			thread = ProcessThread(self._taskQueue, self._resultQueue, speakerType)
			thread._executeFunction = thread.extractKeyword
			thread.start()
			self._threadList.append(thread)

		writerThread = CSVWriterThread(self._resultQueue, filePath, attributeList)
		writerThread.start()

		for processThread in self._threadList:
			processThread.join()
		self._taskQueue.join()
		self._resultQueue.put(END_OF_QUEUE)
		self._resultQueue.join()
		writerThread.join()


if __name__ == '__main__':
	de = ExportMaster()
	de.exportKeywordMatch()