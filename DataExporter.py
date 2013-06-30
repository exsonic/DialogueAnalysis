import csv
from threading import Thread
from Queue import Queue
from DBController import DBController
from Setting import *
from SentenceParser import SentenceParser
from TextUtils import splitListIntoChunk
from collections import Counter

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
			overallWordList = []
			while True:
				lineList = self.queue.get()
				if lineList is END_OF_QUEUE:
					overallWordFrequencyList = [wordFrequencyTuple[0] for wordFrequencyTuple in Counter(overallWordList).most_common()][0:50]
					writer.writerow(overallWordFrequencyList)
					break
				print(lineList)
				writer.writerow(lineList)
				self.queue.task_done()
				overallWordList += lineList[1:]

class ProcessThread(Thread):
	def __init__(self, queue, *args):
		super(ProcessThread, self).__init__()
		self.queue = queue
		self.db = DBController()
		self.args = args

	def topicModelBySpeaker(self):
		[speakerNameList, topicNum, algorithm, filterMode] = self.args[1:]
		parser = SentenceParser()
		for speakerName in speakerNameList:
			try:
				sentenceList = self.db.getAllSpeechTextListBySpeaker(speakerName)
				topicModelWordMatrix = parser.getTopicModelWordMatrix(sentenceList, topicNum, algorithm, filterMode)
				for topicModelWordList in topicModelWordMatrix:
					topicModelWordList = [speakerName] + topicModelWordList
					self.queue.put(topicModelWordList)
			except Exception as e:
				print(e)

	def run(self):
		functionIndex = self.args[0]
		if functionIndex == FUNC_TOPIC_MODEL_SPEAKER:
			self.topicModelBySpeaker()


class DataExporter(object):
	def __init__(self):
		self._db = DBController()

	def exportTopicModel(self, topicNumber=DEFAULT_TOPIC_NUM, algorithm=ALGORITHM_LDA, filterMode=MODE_BASIC):
		threadPool = []
		queue = Queue()
		speakerNameList = self._db.getAllAnalystNameList()
		attributeList = ['analyst'] + ['topic_word'] * 10
		chunkList = splitListIntoChunk(speakerNameList, THREAD_POOL_SIZE)
		for nameList in chunkList:
			processThread = ProcessThread(queue, FUNC_TOPIC_MODEL_SPEAKER, nameList, topicNumber, algorithm, filterMode)
			processThread.start()
			threadPool.append(processThread)
		writerThread = CSVWriterThread('export/analystTopic.csv', attributeList, queue)
		writerThread.start()

		for processThread in threadPool:
			processThread.join()
		queue.put(END_OF_QUEUE)
		#producer finished, all work in queue, then wait queue join(consumer task_done all the job in queue), exit main_process
		writerThread.join(60)

def appendOverallLine(filePath):
	with open(filePath, 'rU') as f:
		lines = list(csv.reader(f))
	with open(filePath, 'a') as f:
		writer = csv.writer(f)
		wordList = []
		for line in lines:
			wordList += line
		newLine = [wordFrequencyTuple[0] for wordFrequencyTuple in Counter(wordList).most_common()][0:50]
		writer.writerow(newLine)

if __name__ == '__main__':
	de = DataExporter()
	de.exportTopicModel()