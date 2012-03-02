from json import dumps,loads
from hashlib import sha224
from datetime import datetime
from threading import Timer
from requests import put,get

class ElasticSearch:
	#for bulk api calls, we'll queue up actions in here
	activity_queue = []
	clear_queue = False

	def __init__(self):
		self.started = datetime.now()
		config = loads(open("config.json").read())
		self.time_to_purge_queue = Timer(config["queue_purge_frequency"],None)
		self.time_to_purge_queue.start()

	#each document needs to have a totally unique id. if they're ever in conflict, you'll overwrite documents passively
	def generate_document_id(self,items):
		return sha224("".join(map(str,items))).hexdigest()

	def index(self,document,index,doctype,bulk=True):
		if bulk:
			index_command = {"index":{"_index":index,"_type":doctype,"_id":self.generate_document_id([document["MID"],document["_id"]])}}
			message = dumps(index_command) + "\n" + dumps(document) + "\n"
			self.activity_queue.append(message)

			#is it time to purge the queue?
			if not self.time_to_purge_queue.isAlive():
				self.purge_queue()
				self.time_to_purge_queue.start()
		else:
			pass

	def delete(self,documentid,index,doctype,bulk=True):
		if bulk:
			delete_command = {"delete":{"_index":index,"_type":doctype,"_id":""}}
			self.activity_queue.append(dumps(delete_command))

			#is it time to purge the queue?
			if not self.time_to_purge_queue.isAlive():
				self.purge_queue()
				self.time_to_purge_queue.start()
		else:
			pass

	def scroll_search(self,doctype,query):
		req = get(self.config["elasticsearch"]["connectionString"]+self.config["elasticsearch"]["index"] + doctype+"/_search?search_type=scan&scroll=10m",data=dumps(query))

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
		message = "\n".join(self.activity_queue)
		res = put(self.config["elasticsearch"]["connectionString"]+self.config["elasticsearch"]["index"]+"/_bulk",data=message)
		print str(datetime.now()) + " - purged queue of " + len(self.activity_queue) + " items. ES responded with: " + str(res.status_code)