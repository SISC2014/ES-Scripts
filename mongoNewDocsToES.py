#!/usr/bin/env python
from elasticsearch import Elasticsearch
import json, ast
from pymongo import MongoClient

#convert from unicode string to int as well as from seconds to milliseconds
def convertToIntMilliseconds(field, documentBSON):
	if field in documentBSON:
		documentBSON[field] = int(documentBSON[field]) * 1000

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

	convertToIntMilliseconds("JobStartDate", documentBSON)
	convertToIntMilliseconds("JobCurrentStartDate", documentBSON)
	convertToIntMilliseconds("JobFinishedHookDone", documentBSON)
	convertToIntMilliseconds("CompletionDate", documentBSON)
	convertToIntMilliseconds("QDate", documentBSON)
	convertToIntMilliseconds("JobCurrentStartExecutingDate", documentBSON)
	convertToIntMilliseconds("LastJobLeaseRenewal", documentBSON)
	convertToIntMilliseconds("EnteredCurrentStatus", documentBSON)
	convertToIntMilliseconds("LastMatchTime", documentBSON) 
	convertToInt("TotalSuspensions", documentBSON)
	convertToInt("LastJobStatus", documentBSON)
	convertToInt("BufferBlockSize", documentBSON)
	convertToInt("OrigMaxHosts", documentBSON)
	convertToInt("LastHoldReasonCode", documentBSON)
	convertToInt("ExitStatus", documentBSON)
	convertToInt("JobLeaseDuration", documentBSON)
	convertToInt("JobUniverse", documentBSON)
	convertToInt("RequestCpus", documentBSON)
	convertToInt("ExitCode", documentBSON)
	convertToInt("NumSystemHolds", documentBSON)
	convertToInt("ResidentSetSize", documentBSON)
	convertToInt("LastHoldReasonSubCode", documentBSON)
	convertToInt("TransferInputSizeMB", documentBSON)
	convertToInt("CoreSize", documentBSON)
	convertToInt("NumSystemHolds", documentBSON)
	convertToInt("ResidentSetSize", documentBSON)
	convertToInt("LastHoldReasonSubCode", documentBSON)
	convertToInt("TransferInputSizeMB", documentBSON)
	convertToInt("CoreSize", documentBSON)
	convertToInt("ImageSize", documentBSON)
	convertToInt("ImageSize_RAW", documentBSON)
	convertToInt("RemoteWallClockTime", documentBSON)
	convertToInt("ClusterId", documentBSON)
	convertToInt("ProcId", documentBSON)
	convertToInt("LastSuspensionTime", documentBSON)
	convertToInt("DiskUsage", documentBSON)
	convertToInt("JobPrio", documentBSON)
	convertToInt("JobRunCount", documentBSON)
	convertToInt("BufferSize", documentBSON)
	convertToInt("MaxHosts", documentBSON)
	convertToInt("MinHosts", documentBSON)
	convertToInt("JobStatus", documentBSON)
	convertToInt("CumulativeSlotTime", documentBSON)
	convertToInt("CommittedSlotTime", documentBSON)
	convertToInt("BytesRecvd", documentBSON)
	convertToInt("NumCkpts_RAW", documentBSON)
	convertToString("_id", documentBSON)

	if("QDate" in documentBSON and "JobCurrentStartExecutingDate" in documentBSON):
		documentBSON["LatencyTimeSecondsINSERTED"] = (documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)
		documentBSON["LatencyTimeMinutesINSERTED"] = ((documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 60.0
		documentBSON["LatencyTimeHoursINSERTED"] = ((documentBSON["JobCurrentStartExecutingDate"] / 1000) - (documentBSON["QDate"] / 1000)) / 3600.0

#	for key, value in documentBSON.items():
#		print key, value, type(value)
	res = es.index(index = "htcondor", doc_type = "mongoData", id = documentBSON["_id"], body = documentBSON)
	print "inserted _id: " + documentBSON["_id"], numDocs + i;
	fileOut.write("inserted _id: " + str(documentBSON["_id"]) + " index: " + str(numDocs + i) + " \n")		
	i+=1;
#	doc = vars(documentEntry)
#	print doc
