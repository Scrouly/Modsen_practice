# This is a sample Python script.
from datetime import time
from pprint import pprint

import uvicorn
from fastapi import FastAPI

from utils.elastic.elastic_connection import connect_elasticsearch
from utils.elastic.elastic_delete import delete_by_id_elastic
from utils.elastic.elastic_search import search_in_elastic
from utils.sql_alchemy.database_connection import create_session
from utils.sql_alchemy.database_delete import delete_by_id_db
from utils.sql_alchemy.database_search import database_search

app = FastAPI(
    title="SearchEngine"
)


@app.get('/documents/search')
def search_by_text(search_query: str):
    search_list = search_in_elastic(connect_elasticsearch(), 'documents', search_query)
    result = database_search(create_session(), search_list)
    return {"result": result}


@app.get('/documents/search')
def search_by_text(search_query: str):
    search_list = search_in_elastic(connect_elasticsearch(), 'documents', search_query)
    result = database_search(create_session(), search_list)
    return {"result": result}


@app.post('/documents/remove')
def delete_by_id(document_id: int):
    es_result = delete_by_id_elastic(connect_elasticsearch(), 'documents', document_id)
    db_result = delete_by_id_db(create_session(), document_id)
    return {"result": (es_result,db_result)}


if __name__ == "__main__":
    uvicorn.run("main:app", reload=True, use_colors=True)
# print([letter for letter in arr if unique_string[0] in letter])
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
