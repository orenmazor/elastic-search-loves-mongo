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
	return original_document