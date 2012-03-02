#sometimes you gotta check that everything's correct
from oplog import Oplog
from elasticsearch_api import ElasticSearch
from json import loads
import pdb
pdb.set_trace()

db = Oplog().get_actual_database()
es = ElasticSearch()

config = loads(open("config.json").read())

query = { "query": { "bool": { "must": [{"term":{"TY":0}}],"must_not": [ ],"should": [ ]}},"from": 0,"size": 50,"sort": [ ],"facets": { }}

#connect to your data from the ES side of things, to avoid blocking mongo.
#we'll iterate over all of the records in ES and make sure they're up to date
#with what is in mongo
for record in es.scroll_search(query,index=config["elasticsearch"]["index"]):
    docs = db.find({"_id":record['_source']['OID']})
    if docs.count() == 0:
        #the document exists in ES but not mongo, so we need to delete
        result = es.delete(record["_id"],bulk=True)
        print "deleted "+ record["_id"] + " from ES because its not in mongo"
    else:
        #this will never be above 1
        for doc in docs:
            #write your own sync code here. this is mine.
            if not doc['TY'] != record['TY']:
                result = es.insert(doc,index=config["elasticsearch"]["index"],doctype="DeliveryEvent",bulk=True)
                print "inserted new version of " + record['_id'] + " because its old."
