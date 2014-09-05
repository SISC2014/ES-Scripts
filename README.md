ES-Scripts
==========

deleteIndexRecreteAndMapScript:

Bash script that deletes the mongoData doc_type in the htcondor index if it exists, recreates the mongoData doc_type, and sets the mapping for the mongoData doc_type.

mongoNewDocsToES.py:

Python script that takes new job documents from the history_records database in MongoDB and inserts them into the mongoData doc_type of elastic search. Also inserts 6 new fields associated with the queue time, i.e. time difference between when job was submitted and when job started executing. The dIfferent fields measure time in either seconds, minutes, or hours, as well as calculating the queue time as either (JobCurrentStartExecutingTime - QDate) or (JobStartDate - QDate). Writes information about the new jobs inserted (ID and index number) into the text file insertScriptOutput.txt.

getShadowLog:

Bash script that copies the current ShadowLog to the /scripts/ShadowLog directory

shadowLogInsertToMongoIndex.py:

Parses the ShadowLog to get the information about each job such as its StartTime, EndTime, and all the messages associated with it. Inserts this information into the document with the same job ID found in the mongoData doc_type of ElasticSearch.
