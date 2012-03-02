from elasticsearch_api import ElasticSearch
from json import loads

es = ElasticSearch()
config = loads(open("config.json").read())

def delete(namespace,documentID):
    es.delete(documentID,index=config["elasticsearch"]["index"],doctype=namespace,bulk=True)
    