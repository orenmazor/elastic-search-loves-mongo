import pymongo
from pymongo.errors import AutoReconnect
from json import loads
import sys

class Oplog:

	def __init__(self):
		self.last_record = None

		try:
			self.config = loads(open("config.json").read())
		except:
			print "failed to read config.json"
			sys.exit()

	def watch_oplog(self):
		oplog_collection = None
		actual_data = None

		#open a connection to the oplog collection
		try:
			connection = pymongo.Connection(self.config["oplog"]["host"],self.config["oplog"]["port"])
			db = connection[self.config["oplog"]["db"]]
			oplog_collection = db[self.config["oplog"]["collection"]]
		except:
			print "failed to connect to oplog"
			sys.exit()

		#open a connection to the db.
		try:
			connection = pymongo.Connection(self.config["data"]["host"],self.config["data"]["port"])
			actual_data = connection[self.config["data"]["db"]]
		except:
			print "failed to connect to actual database"
			sys.exit()


		#run forevermore
		while True:
			try:
				#oplogs are in a natural order, so we dont need to actually
				#iterate to find the latest insertion. we can just grab the last entry.
				#and maybe back up a little bit. you dont have to if you dont want to.
				last_index = oplog_collection.count() - 100 
				cursor = oplog_collection.find(skip=last_index,tailable=True)

				while cursor.alive:
					for op in cursor:

						#parse the namespace of the op to figure out what collection its coming from
						try:
							op_collection_name = op['ns'].split('.')[1]
						except:
							continue

						#if its a collection we want to index, dump it
						if not op_collection_name in self.config["types_we_care_about"]:
							continue

						if op['op'] == 'i':
							yield ('i',op['o'])
						elif op['op'] == 'u':
							#get the latest version of this document. luckily, we get the criteria
							criteria = op['o2']
							data_cursor = actual_data[op_collection_name]
							docs = data_cursor.find(criteria)

							#its basically impossible for there to be more than one document here. 
							#but just in case
							for doc in data_cursor.find(criteria):
								yield ('i',doc)

						elif op['op'] == 'd':
							yield ('d',op['o']['_id'])

			except AutoReconnect:
				#log this terrible fact
				#continue
				pass
