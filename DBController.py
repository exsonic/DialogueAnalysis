"""
Bobi Pu, bobi.pu@usc.edu
"""

from pymongo import MongoClient

class DBController(object):
	def __init__(self):
		try:
			# self.db = self.client.test
			self._db = MongoClient().ChemCorpus
		except Exception as e:
			print e
			exit()

