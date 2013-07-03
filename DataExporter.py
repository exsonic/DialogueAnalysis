import csv
from threading import Thread
from Queue import Queue
from DBController import DBController
from Setting import *
from SentenceParser import SentenceParser
from TextUtils import splitListIntoChunk

class CSVWriterThread(Thread):
	def __init__(self, filePath, attributeList, queue):
		super(CSVWriterThread, self).__init__()
		self.filePath = filePath
		self.attributeList = attributeList
		self.queue = queue

	def run(self):
		with open(self.filePath, 'w') as f:
			writer = csv.writer(f)
			writer.writerow(self.attributeList)
			while True:
				lineList = self.queue.get()
				if lineList is END_OF_QUEUE:
					break
				print(lineList)
				writer.writerow(lineList)
				self.queue.task_done()

class ProcessThread(Thread):
	def __init__(self, queue, *args):
		super(ProcessThread, self).__init__()
		self.queue = queue
		self.db = DBController()
		self.args = args

	def topicModelBySpeaker(self):
		[speakerNameList, topicNum, algorithm, filterMode, keepScore] = self.args[1:]
		parser = SentenceParser()
		for speakerName in speakerNameList:
			try:
				sentenceList = self.db.getAllSpeechTextListBySpeaker(speakerName)
				topicModelWordMatrix = parser.getTopicModelWordMatrix(sentenceList, topicNum, algorithm, filterMode, keepScore)
				for topicModelWordList in topicModelWordMatrix:
					topicModelWordList = [speakerName] + topicModelWordList
					self.queue.put(topicModelWordList)
			except Exception as e:
				print(e)

	def topicModelByConference(self):
		[conferenceList, speakerType, topicNum, algorithm, filterMode, keepScore] = self.args[1:]
		parser = SentenceParser()
		for conference in conferenceList:
			try:
				sentenceList = self.db.getAllSpeechTextByConferenceIdAndSpeakerType(conference['_id'], speakerType)
				topicModelWordMatrix = parser.getTopicModelWordMatrix(sentenceList, topicNum, algorithm, filterMode, keepScore)
				for i, topicModelWordList in enumerate(topicModelWordMatrix):
					topicModelWordList = [conference['_id'], conference['company'], conference['time'], (i + 1)] + topicModelWordList
					self.queue.put(topicModelWordList)
			except Exception as e:
				print(e)

	def run(self):
		functionIndex = self.args[0]
		if functionIndex == FUNC_TOPIC_MODEL_SPEAKER_ANALYST:
			self.topicModelBySpeaker()
		elif functionIndex == FUNC_TOPIC_MODEL_CONFERENCE_ANALYST:
			self.topicModelByConference()


class DataExporter(object):
	def __init__(self):
		self._db = DBController()

	def exportTopicModel(self, exportFileName='export/topicModel.csv',  speakerType=TYPE_ANALYST,
	                     topicNumber=5, algorithm=ALGORITHM_LDA, filterMode=MODE_BASIC, keepScore=True,):
		threadPool = []
		queue = Queue()
		conferenceList = list(self._db.getAllConference())
		attributeList = ['conferenceId', 'company', 'time', 'topic_number']
		for i in range(1, 11):
			attributeList.append('topic_word_' + str(i))
			attributeList.append('word_score_' + str(i))
		chunkList = splitListIntoChunk(conferenceList, THREAD_POOL_SIZE)
		for subConferenceList in chunkList:
			processThread = ProcessThread(queue, FUNC_TOPIC_MODEL_CONFERENCE_ANALYST, subConferenceList, speakerType, topicNumber, algorithm, filterMode, keepScore)
			processThread.start()
			threadPool.append(processThread)
		writerThread = CSVWriterThread(exportFileName, attributeList, queue)
		writerThread.start()

		for processThread in threadPool:
			processThread.join()
		queue.put(END_OF_QUEUE)
		writerThread.join(60)


if __name__ == '__main__':
	de = DataExporter()
	de.exportTopicModel()