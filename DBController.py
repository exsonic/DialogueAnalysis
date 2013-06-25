"""
Bobi Pu, bobi.pu@usc.edu
"""

from pymongo import MongoClient

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