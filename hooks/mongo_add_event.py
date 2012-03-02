from elasticsearch_api import ElasticSearch
from json import loads
from hooks import es_data_mapping

es = ElasticSearch()

def add(index,doctype,data):
    document = es_data_mapping.remap(data)
    es.index(document,index=index,doctype=doctype,bulk=True)
    
