from elasticsearch_api import ElasticSearch
from json import loads

es = ElasticSearch()

def delete(index,doctype,documentID):
    es.delete(documentID,index=index,doctype=doctype,bulk=True)
    
