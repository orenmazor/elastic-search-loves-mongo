import pymongo
from pymongo.errors import AutoReconnect
from json import loads,dumps
import sys
from hooks import es_data_mapping
import zmq
from time import sleep

class Oplog:

    def __init__(self):
        self.last_record = None
        try:
            self.config = loads(open("config.json").read())
        except:
            print "failed to read config.json"
            sys.exit()

    def start(self):
        context = zmq.Context()
        es_queue = context.socket(zmq.PUSH)
        es_queue.bind("tcp://127.0.0.1:5555")
        print "loading oplog...."
        for op,data,namespace in self.watch_oplog():
            if op == "i":
                document = es_data_mapping.remap(data)
                es_queue.send(dumps({"op":"index","data":document,"index":self.config["elasticsearch"]["index"],"doctype":namespace}))
            elif op == "d":
                #dont do this for now
                pass
                #self.es_queue.send(dumps({"op":"delete","data":document,"index":self.config["elasticsearch"]["index"],"doctype":namespace}))
                #mongo_delete_event.delete(index=self.config["elasticsearch"]["index"],doctype=namespace,documentID=data)

    def get_oplog_database(self):
        #open a connection to the oplog collection
        try:
            connection = pymongo.Connection(self.config["oplog"]["host"],self.config["oplog"]["port"])
            db = connection[self.config["oplog"]["db"]]
            oplog_collection = db[self.config["oplog"]["collection"]]
            return oplog_collection
        except:
            print "failed to connect to oplog"
            sys.exit()       

    def get_actual_database(self):
        #open a connection to the db.
        try:
            connection = pymongo.Connection(self.config["data"]["host"],self.config["data"]["port"])
            actual_data = connection[self.config["data"]["db"]]
            return actual_data
        except:
            print "failed to connect to actual database"
            sys.exit()

    def watch_oplog(self):
        oplog_collection = self.get_oplog_database()
        actual_data = self.get_actual_database()

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
                            yield ('i',op['o'],op_collection_name)
                        elif op['op'] == 'u':
                            #get the latest version of this document. luckily, we get the criteria
                            criteria = op['o2']
                            data_cursor = actual_data[op_collection_name]
                            docs = data_cursor.find(criteria)

                            #its basically impossible for there to be more than one document here. 
                            #but just in case
                            for doc in docs:
                                yield ('i',doc,op_collection_name)

                        elif op['op'] == 'd':
                            yield ('d',op['o']['_id'],op_collection_name)

            except AutoReconnect:
                #log this terrible fact
                #continue
                pass