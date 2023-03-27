import pandas as pd
import json

from utils.elastic.elastic_connection import connect_elasticsearch


def data_to_elastic(data_file, id=0):
    """Конвертация данных в формат elastic'а"""
    for record in data_file.to_dict(orient="records"):
        id += 1
        yield '{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s"}}' % (
            "documents", "document", id)

        yield json.dumps(record, default=int)


def elastic_entry(es_object, data_file):
    """Добавление данных в Elastic"""
    # Cчитывает колонку text из csv и добавляет колонку id
    df = pd.read_csv(data_file, usecols=[0])
    df["id"] = df.index + 1

    # Поключение к elastic
    if es_object:
        # Создание нового индекса
        if es_object.indices.exists(index='documents'):
            es_object.indices.delete(index='documents')
        es_object.indices.create(index='documents')
        # Запись данных
        data_recording = es_object.bulk(data_to_elastic(df))
        if data_recording["errors"]:
            print("Failed to write data to elastic")
        else:
            print("Data was successfully written to elastic")
        es_object.close()


if __name__ == "__main__":
    file_name = "../../data/posts.csv"
    es = connect_elasticsearch()
    elastic_entry(es, file_name)
