import pandas as pd
from elasticsearch import Elasticsearch
import logging
import json


def connect_elasticsearch():
    """"Соединение с Elasticsearch."""
    _es = None
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if _es.ping():
        print('Elasticsearch is connected')
        return _es
    else:
        print('Elasticsearch could not connect!')
        return None


def data_to_elastic(data_file):
    """Конвертация данных в формат elastic'а"""
    for record in data_file.to_dict(orient="records"):
        yield '{ "index" : { "_index" : "%s", "_type" : "%s"}}' % ("documents", "document")
        yield json.dumps(record, default=int)


def elastic_entry(data_file):
    """Добавление данных в Elastic"""
    # Cчитывает колонку text из csv и добавляет колонку id
    df = pd.read_csv(data_file, usecols=[0])
    df["id"] = df.index + 1

    # Поключение к elastic
    es = connect_elasticsearch()
    if es:
        # Создание нового индекса
        if es.indices.exists(index='documents'):
            es.indices.delete(index='documents')
        es.indices.create(index='documents')
        # Запись данных
        data_recording = es.bulk(data_to_elastic(df))
        if data_recording["errors"]:
            print("Failed to write data to elastic")
        else:
            print("Data was successfully written to elastic")
        es.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    file_name = "../Data/posts.csv"
    elastic_entry(file_name)

