"""
Bobi Pu, bobi.pu@usc.edu
"""
from DBController import DBController
import os, csv

def loadAllDialoguesFromFile(speakerTypeFilePath, folderPath):
	db = DBController()
	db.dropConference()
	db.dropSession()
	db.dropSpeech()
	ensuredIndex = False
	speakerTypeDict = {}
	#load the speaker type csv file
	with open(speakerTypeFilePath, 'rU') as f:
		lines = csv.reader(f)
		for i, line in enumerate(lines):
			if i == 0:
				continue
			fileName = line[4].strip()
			speakerType = line[13].strip()
			speakerTypeDict[fileName] = speakerType

	for dirPath, dirNames, fileNames in os.walk(folderPath):
		print(dirPath)
		if dirPath.split('/')[-1].startswith('chunk'):
			for fileName in fileNames:
				try:
					if fileName.endswith('txt'):
						fileNameParts = [part.strip() for part in fileName.split('.txt')[0].split('_')]
						company, time = fileNameParts[0], fileNameParts[1]
						sessionType, sessionOrder, asker, answerer = fileNameParts[2], int(fileNameParts[3]), fileNameParts[4], fileNameParts[5]
						if fileNameParts[-1].endswith('default') or fileNameParts[-1].endswith('copy'):
							continue
						elif fileNameParts[-1][-1].isdigit() and not fileNameParts[-1][-2].isdigit():
							speaker = fileNameParts[-1][:-1]
							speechOrder = int(fileNameParts[-1][-1:])
						elif fileNameParts[-1][-1].isdigit() and fileNameParts[-1][-2].isdigit():
							speaker = fileNameParts[-1][:-2]
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
							filePath = dirPath + '/' + fileName
							speakerType = speakerTypeDict[fileName] if fileName in speakerTypeDict else '.'

							with open(filePath, 'r') as f:
								text = ' '.join(f.readlines()).strip()
								text = text.decode('ascii', 'ignore').encode('ascii', 'ignore')
								speech = {'conference' : conference['_id'], 'session' : session['_id'], 'order' : speechOrder, 'speaker' : speaker, 'type' : speakerType, 'text' : text}
								db.insertSpeech(speech)
						if not ensuredIndex:
							db.ensureConferenceIndex()
							db.ensureSessionIndex()
							db.ensureSpeechIndex()
							ensuredIndex = True
				except Exception as e:
					print(fileName)
					print(e)



# if __name__ == '__main__':
# 	loadAllDialoguesFromFile('/Users/exsonic/Developer/DialogueAnalysis/corpus/0.seglist_0503_cleaned.csv', '/Users/exsonic/Developer/Marshall_RA/chunk_done/')