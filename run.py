import daemon
from oplog import Oplog
import elasticsearch_api
import pdb
import datetime
from json import loads
from requests import put

def run():
	op_emitter = Oplog()

	config = loads(open("config.json").read())
	#we'll dump all activity json in here and regularly purge
	activity_queue = []

	started = datetime.datetime.now()
	last_queue_purge = started()
	for operation,data in op_emitter.watch_oplog():
		if operation == "i":
			es_record = elasticsearch_api.remap(data)
			activity_queue.append(elasticsearch_api.index(es_record))
		elif operation == "d":
			activity_queue.append(elasticsearch_api.delete(data))
	
		#would look better with a timer object
		if (datetime.datetime.now() - last_queue_clearing).seconds >= config["queue_purge_frequency"]:
			message = "\n".join(activity_queue)
			res = put(config["elasticsearch"]["connectionString"]+config["elasticsearch"]["index"]+"/_bulk",data=message)
			print "ES replied " + str(res.status_code)
			activity_queue = []

if __name__ == "__main__":
	run()