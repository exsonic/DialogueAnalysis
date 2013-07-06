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
					self.queue.task_done()
					break
				else:
					if isinstance(lineList[0], list):
						#lineList includes serveal lines
						for line in lineList:
							print(line)
							writer.writerow(line)
					else:
						print(lineList)
						writer.writerow(lineList)
						self.queue.task_done()

class ProcessThread(Thread):
	def __init__(self, queue, *args):
		super(ProcessThread, self).__init__()
		self.queue = queue
		self.db = DBController()
		self.args = args

	def topicModelByConferenceAnalyst(self):
		[conferenceList, speakerType, topicNum, algorithm, filterMode, keepScore] = self.args[1:]
		parser = SentenceParser()
		for conference in conferenceList:
			try:
				speechList = list(self.db.getAllSpeechByConferenceIdAndSpeakerType(conference['_id'], speakerType))
				speakerDict = {}
				for speech in speechList:
					if speech['speakerId'] not in speakerDict:
						speakerDict[speech['speakerId']] = [speech]
					else:
						speakerDict[speech['speakerId']].append(speech)

				for speakerId, speechList in speakerDict.iteritems():
					speakerName = speechList[0]['speakerName']
					sentenceList = [speech['text'] for speech in speechList]
					topicModelWordMatrix = parser.getTopicModelWordMatrix(sentenceList, topicNum, algorithm, filterMode, keepScore)
					lineList = []
					for i, topicModelWordList in enumerate(topicModelWordMatrix):
						basicInfo = [conference['_id'], conference['company'], conference['time'], speakerName, speakerType, speakerId, len(speechList)]
						lineList.append(basicInfo + topicModelWordList)
					self.queue.put(lineList)
			except Exception as e:
				print(e)

	def run(self):
		functionIndex = self.args[0]
		if functionIndex == FUNC_TOPIC_MODEL_CONFERENCE_ANALYST:
			self.topicModelByConferenceAnalyst()


class DataExporter(object):
	def __init__(self):
		self._db = DBController()

	def exportTopicModel(self, exportFileName='export/topicModel.csv',  speakerType=TYPE_ANALYST,
						 topicNumber=1, algorithm=ALGORITHM_LDA, filterMode=MODE_BASIC, keepScore=True,):
		threadPool = []
		queue = Queue()
		conferenceList = list(self._db.getAllConference())
		attributeList = ['conferenceId', 'company', 'time', 'speaker', 'speaker_type', 'speaker_Id', 'speak_freq']
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