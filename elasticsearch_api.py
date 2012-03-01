from json import dumps
from hashlib import sha224

#each document needs to have a totally unique id. if they're ever in conflict, you'll overwrite documents passively
def generate_document_id(items):
	return sha224("".join(items)).hexdigest()

def index(document,bulk=True):
	if not bulk:
		pass

	index_command = {"index":{"_index":"","_type":"","_id":generate_document_id([document["MID"],document["_id"]])}}
	message = dumps(index_command) + "\n" + dumps(document) + "\n"


	return message

def delete(documentid,bulk=True):
	if not bulk:
		pass

	delete_command = {"delete":{"_index":"","_type":"","_id":""}}
	return dumps(delete_command)

#in most cases we may not want to actual insert exactly what is in the mongo, so parse this
def remap(original_document):
	entry = {}
	entry["OID"] = original_document["_id"]
	entry["MID"] = original_document["MID"]
	entry["RE"] = original_document["RE"]
	entry["DE"] = original_document["DE"]
	entry["TA"] = original_document["TA"]
	entry["UID"] = original_document["UID"]
	entry["SID"] = original_document["SID"]
	entry["TY"] = original_document["TY"]
	entry["SE"] = original_document["SE"]
	entry["SU"] = original_document["SU"]
	entry["BT"] = original_document["BT"]
	entry["BI"] = original_document["BI"]
	entry["BounceDescription"] = original_document["BounceDescription"]
	entry["RA"] = str(original_document["RA"])
	if "AT" in original_document and not original_document["AT"] == None and not original_document["AT"] == []:
		entry["AT"] = dumps(original_document["AT"])
	#if email is sent, make sure it's purged
	if entry["TY"] == 1:
		entry["_ttl"] = "15d"

	return entry