"""
Bobi Pu, bobi.pu@usc.edu
"""

from pymongo import MongoClient
from Setting import TYPE_ANALYST

class DBController(object):
	def __init__(self):
		try:
			self._db = MongoClient().DialogueCorpus
		except Exception as e:
			print e
			exit()

	def dropConference(self):
		self._db.conference.drop()

	def dropSession(self):
		self._db.session.drop()

	def dropSpeech(self):
		self._db.speech.drop()

	def ensureConferenceIndex(self):
		self._db.conference.ensure_index([('company' , 1), ('time', 1)])

	def ensureSessionIndex(self):
		self._db.session.ensure_index([('conference', 1), ('order', 1)])

	def ensureSpeechIndex(self):
		self._db.speech.ensure_index([('conference', 1), ('session', 1), ('order', 1)])

	def insertConference(self, conference):
		conference['_id'] = str(self._db.conference.count())
		self._db.conference.insert(conference)
		return conference

	def insertSession(self, session):
		session['_id'] = str(self._db.session.count())
		self._db.session.insert(session)
		return session

	def insertSpeech(self, speech):
		speech['_id'] = str(self._db.speech.count())
		self._db.speech.insert(speech)
		return speech

	def getAllSpeech(self, limit=0):
		return self._db.speech.find(timeout=False).limit(limit)

	def getAllSpeechByType(self, speechType, limit=0):
		return self._db.speech.find({'type' : speechType}, timeout=False).limit(limit)

	def getAllAnalystNameList(self):
		analystNameList = [speech['speaker'] for speech in self.getAllSpeechByType(TYPE_ANALYST)]
		return list(set(analystNameList))

	def getAllSpeechBySpeaker(self, speaker):
		return self._db.speech.find({'speaker' : speaker}, timeout=False)

	def getAllSpeechTextListBySpeaker(self, speaker):
		return [speech['text'] for speech in self.getAllSpeechBySpeaker(speaker)]

	def getConferenceByCompanyTime(self, company, time):
		return self._db.conference.find_one({'company' : company, 'time' : time})

	def getSessionByConferenceAndOrder(self, conferenceId, order):
		return self._db.session.find_one({'conference' : conferenceId, 'order' : order})

	def getSpeechByConferenceIdAndSessionIdAndOrder(self, conferenceId, sessionId, sessionOrder):
		return self._db.session.find_one({'conference' : conferenceId, 'session' : sessionId, 'order' : sessionOrder})

	def getConferenceById(self, conferenceId):
		return self._db.conference.find_one({'_id' : conferenceId})

	def getSessionById(self, sessionId):
		return self._db.session.find_one({'_id' : sessionId})

	def getSpeechById(self, speechId):
		return self._db.speech.find_one({'_id' : speechId})

# if __name__ == '__main__':
# 	db = DBController()
# 	analystNameList = db.getAllAnalystNameList()
# 	analystSpeechDict = dict(zip(analystNameList, [[]] * len(analystNameList)))
# 	for analystName in analystSpeechDict.iterkeys():
# 		analystSpeechDict[analystName] = list(db.getAllSpeechBySpeaker(analystName))
# 	print(analystSpeechDict)
