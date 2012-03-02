from elasticsearch_api import ElasticSearch
from json import loads
import hooks

es = ElasticSearch()
config = loads(open("config.json").read())

def add(namespace,data):
    document = hooks.es_data_mapping.remap(data)
    es.insert(document,index=config["elasticsearch"]["index"],doctype=namespace,bulk=True)
    