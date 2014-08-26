#!/usr/bin/env python

from elasticsearch import Elasticsearch
import re
import dateutil.parser as parser
import datetime

class jobObject:
	def __init__(self, jobID, shadowStartTime):
		self.jobID = jobID
		self.jobShadowStartTime = shadowStartTime
		self.jobMessages = {} # dict of log entries where key is time and value is the message

	def setEndTime(self, endTime):
		self.jobEndTime = endTime
		diffTime = parser.parse(self.jobEndTime) - parser.parse(self.jobShadowStartTime)
		diffTuple = divmod(diffTime.days * 86400 + diffTime.seconds, 60)
 		self.jobTimeShadow = diffTuple[0] * 60 + diffTuple[1]

	def setLagTime(self, QDate):
		self.queryDate = datetime.datetime.fromtimestamp(QDate/1000).isoformat()
		diffTime = parser.parse(self.jobShadowStartTime) - parser.parse(self.queryDate)
		diffTuple = divmod(diffTime.days * 86400 + diffTime.seconds, 60)
 		self.lagTimeShadow = diffTuple[0] * 60 + diffTuple[1]


es = Elasticsearch()
f = open('/home/scottscott/scripts/ShadowLog', 'r')
jobDict = {} # dict where key = jobId and value = jobObject associated with that jobId
for line in f:
	date = re.search("\d\d/\d\d/\d\d", line) #get the date from log
	time = re.search("\d\d:\d\d:\d\d", line) #get the time from log
	dateTimeText = date.group() + " " + time.group()
	dateTime = parser.parse(dateTimeText)
	jobIDMatch = re.search("VANILLA shadow for job \d+\.\d+", line) # check if log line indicates that a job has started
	if jobIDMatch:
		jobID = re.search("\d+\.\d+", jobIDMatch.group())
		jO1 = jobObject(jobID.group(), dateTime.isoformat()) # create new job object with jobID and the time the job started at
		jobDict[jO1.jobID] = jO1 # put this jobObject in the dict under its jobID
	else:
		jobIDMatch =  re.search("\(\d+\.\d+\)", line)
		if(jobIDMatch): # check if logline is related to a job process
			jobID = re.search("\d+\.\d+", jobIDMatch.group())
			if jobID.group() in jobDict:
				jO1 = jobDict[jobID.group()]
				jobTerminateMatch = re.search("Job \d+\.\d+ terminated", line)
				if not jobTerminateMatch:
					jobTerminateMatch = re.search("terminating job \d+\.\d+", line)
				if jobTerminateMatch:
					jO1.setEndTime(dateTime.isoformat())
					if es.exists(index = "htcondor", doc_type = "mongoData", id = jO1.jobID):
						print "jobID: " + str(jobID.group())
						es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.jobShadowStartTime = dateTime", "params" : { "dateTime" : jO1.jobShadowStartTime } } )
						es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.jobEndTime = dateTime", "params" : { "dateTime" : jO1.jobEndTime } })
						es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.jobTimeSHADOW = diff", "params" : { "diff" : jO1.jobTimeShadow } })
						res = es.get(index = "htcondor", doc_type = "mongoData", id = jO1.jobID)
						if "QDate" in res["_source"]:
							jO1.setLagTime((res["_source"])["QDate"])	
							es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.lagTimeSecondsSHADOW = diff", "params" : { "diff" : jO1.lagTimeShadow } })
							es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.lagTimeMinutesSHADOW = diff", "params" : { "diff" : jO1.lagTimeShadow/60.0 } })
							es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.lagTimeHoursSHADOW = diff", "params" : { "diff" : jO1.lagTimeShadow/3600.0 } })
						es.update(index = "htcondor", doc_type = "mongoData", id = jO1.jobID, body = {"script" : "ctx._source.jobMessages = jM", "params" : { "jM" : jO1.jobMessages } })
				else: # creates new jobMessages entry that includes the jobTimestamp and jobMessage or appends the jobMessage if key already exists
					messageMatch = re.search(": .*", line)
					message = messageMatch.group(0)[2:]
					if dateTime.isoformat() in jO1.jobMessages:
						jO1.jobMessages[dateTime.isoformat()].append(message)
					else:
						jO1.jobMessages[dateTime.isoformat()] = [message]
