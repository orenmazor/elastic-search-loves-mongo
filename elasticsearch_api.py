from json import dumps,loads
from hashlib import sha224
from datetime import datetime
from requests import post,put,get
from time import sleep
import zmq
import sys

class ElasticSearch:
	#we keep deletes and index actions separate to control their purge frequencies
	activity_queue = []
	clear_queue = False

	def __init__(self):
		self.last_queue_purge = datetime.now()
		self.config = loads(open("config.json").read())

	def start(self):
		context = zmq.Context()
		op_queue = context.socket(zmq.PULL)
        op_queue.connect("tcp://127.0.0.1:5555")	
        print "started listening"
        while True:
			message = op_queue.recv()
			operation = loads(message)
			self.index(index=operation["index"],document=operation["data"],doctype=operation["doctype"],bulk=True)
								
	#each document needs to have a totally unique id. if they're ever in conflict, you'll overwrite documents passively
	def generate_document_id(self,items):
		return sha224("".join(map(str,items))).hexdigest()

	def index(self,document,index,doctype,bulk=True):
		if bulk:
			index_command = {"index":{"_index":index,"_type":doctype,"_id":self.generate_document_id([document["MID"],document["OID"]])}}
			message = dumps(index_command) + "\n" + dumps(document) + "\n"
			self.activity_queue.append(message)

			#is it time to purge the queue?
			if len(self.activity_queue) > 1000:
			#if (datetime.now() - self.last_queue_purge).seconds >= self.config["queue_purge_frequency"]:
				self.purge_queue()
				self.last_queue_purge = datetime.now()
		else:
			pass

	def delete(self,documentID,index,doctype,bulk=True,routing=None):
		if bulk:
			delete_command = {}
			if routing == None:
				delete_command = {"delete":{"_index":index,"_type":doctype,"_id":documentID}}
			else:
				delete_command = {"delete":{"_index":index,"_type":doctype,"_id":documentID,"_routing":routing}}

			self.activity_queue.append(dumps(delete_command) + "\n")

			#is it time to purge the queue?
			if len(self.activity_queue) >= 0:
			#if (datetime.now() - self.last_queue_purge).seconds >= self.config["queue_purge_frequency"]:
				self.purge_queue()
				self.last_queue_purge = datetime.now()
		else:
			pass

	def count_matches(self,doctype,query):
		req = get(self.config["elasticsearch"]["connectionString"] + self.config["elasticsearch"]["index"] + "/"+doctype+"/_search",data=dumps(query))
		if req.status_code == 200:
			return loads(req.content)["hits"]["total"]
		
		return 0

	def scroll_search(self,doctype,query):
		
		req = get(self.config["elasticsearch"]["connectionString"]+self.config["elasticsearch"]["index"] +"/"+ doctype+"/_search?search_type=scan&scroll=10m",data=dumps(query))

		if not req.status_code == 200:
			raise Exception(req.content)

		res = loads(req.content)
		total_hits = res['hits']['total']
		print "got back " + str(total_hits) + " results"
		scroll_id = res['_scroll_id']

		new_total_hits = total_hits
		while not new_total_hits == 0:
			new_req = get(self.config["elasticsearch"]["connectionString"]+ '_search/scroll?scroll=10m',data=str(scroll_id))
			if not new_req.status_code == 200:
				raise Exception(new_req.content)

			new_res = loads(new_req.content)
			for esdoc in new_res['hits']['hits']:
				yield esdoc

	def purge_queue(self):
		message = "".join(self.activity_queue)
		#print message
		
		#what if ES is down? we should wait on it.
		res = post(self.config["elasticsearch"]["connectionString"]+self.config["elasticsearch"]["index"]+"/_bulk",data=message)
		print res.content	
		print "purged queue of " + str(len(self.activity_queue)) + " items. ES responded with: " + str(res.status_code)
		self.activity_queue = []
