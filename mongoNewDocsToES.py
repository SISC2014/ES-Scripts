#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json, ast
from pymongo import MongoClient

#convert from unicode string to int as well as from seconds to milliseconds
def convertToMilliseconds(field, documentBSON):
	if field in documentBSON:
		documentBSON[field] = documentBSON[field] * 1000

#convert from unicode string to int 
def convertToInt(field, documentBSON):
	if field in documentBSON:
		documentBSON[field] = int(float(documentBSON[field]))

#convert from unicode string to string
def convertToString(field, documentBSON):
	if field in documentBSON:
		documentBSON[field] = str(documentBSON[field])

es = Elasticsearch()
client = MongoClient('db.mwt2.org', 27017)
db = client.condor_history
collection = db.history_records
i = 0 
fileOut = open('insertScriptOutput.txt', 'w')
fileOut.truncate()
countDict =  es.count(index = "htcondor", doc_type = "mongoData")
numDocs = countDict["count"]
for documentBSON in collection.find()[numDocs:]:
	ast.literal_eval(json.dumps(documentBSON))

	convertToMilliseconds("JobStartDate", documentBSON)
	convertToMilliseconds("JobCurrentStartDate", documentBSON)
	convertToMilliseconds("JobFinishedHookDone", documentBSON)
	convertToMilliseconds("CompletionDate", documentBSON)
	convertToMilliseconds("QDate", documentBSON)
	convertToMilliseconds("JobCurrentStartExecutingDate", documentBSON)
	convertToMilliseconds("LastJobLeaseRenewal", documentBSON)
	convertToMilliseconds("EnteredCurrentStatus", documentBSON)
	convertToMilliseconds("LastMatchTime", documentBSON) 

	if("QDate" in documentBSON and "JobCurrentStartExecutingDate" in documentBSON):
		documentBSON["LatencyTimeSecondsINSERTED"] = (documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)
		documentBSON["LatencyTimeMinutesINSERTED"] = ((documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 60.0
		documentBSON["LatencyTimeHoursINSERTED"] = ((documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 3600.0

	if("QDate" in documentBSON and "JobStartDate" in documentBSON):
		documentBSON["queueTimeSecondsINSERTED"] = (documentBSON["JobStartDate"] / 1000) - (documentBSON["QDate"] / 1000)
		documentBSON["queueTimeMinutesINSERTED"] = ((documentBSON["JobStartDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 60.0
		documentBSON["queueTimeHoursINSERTED"] = ((documentBSON["JobStartDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 3600.0

	res = es.index(index = "htcondor", doc_type = "mongoData", id = documentBSON["_id"], body = documentBSON)
	fileOut.write("inserted _id: " + str(documentBSON["_id"]) + " index: " + str(numDocs + i) + " \n")		
	i+=1;
#	doc = vars(documentEntry)
#	print doc
