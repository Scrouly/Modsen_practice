from pprint import pprint
from utils.elastic.elastic_entry import connect_elasticsearch
import json


def search_in_elastic(es_object, index_name, search_query):
    """"Поиск документов в индексе по текстовому запросу, возвращает список id'шников"""
    # Запрос на поиск
    search_object = {
        "size": 10000,
        "query": {
            "match": {
                "text": search_query
            }
        },
        "sort": [
            {
                "id": {
                    "order": "asc"
                }
            }
        ]
    }
    result = es_object.search(index=index_name, body=json.dumps(search_object))
    es_object.close()
    result = [{"id": item["_source"]["id"]} for item in result["hits"]["hits"]]
    if not result:
        print("Nothing was found for your query")
        return None
    else:
        pprint(result)
        return result


if __name__ == "__main__":
    file_name = "../../data/posts.csv"
    es = connect_elasticsearch()
    if es:
        s = search_in_elastic(es, 'documents', 'я')
