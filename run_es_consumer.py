from elasticsearch_api import ElasticSearch
from multiprocessing import Process

def run():
        es = ElasticSearch()
        Process(target=es.start).start()

if __name__ == "__main__":
    run()