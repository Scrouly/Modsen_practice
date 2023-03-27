from elasticsearch import Elasticsearch
import logging


def connect_elasticsearch():
    """"Соединение с Elasticsearch."""
    logging.basicConfig(level=logging.ERROR)
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Elasticsearch is connected')
        return _es
    else:
        print('Elasticsearch could not connect!')
        return None
