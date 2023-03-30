import uvicorn
from fastapi import FastAPI

from utils.elastic.elastic_connection import connect_elasticsearch
from utils.elastic.elastic_delete import delete_by_id_elastic
from utils.elastic.elastic_search import search_in_elastic
from utils.database.database_connection import create_session
from utils.database.database_delete import delete_by_id_db
from utils.database.database_search import database_search

app = FastAPI(
    title="SearchEngine"
)


@app.get('/documents/search')
def search_by_text(search_query: str):
    """Поиск документов исходя из текстового запроса"""
    es_object = connect_elasticsearch()
    if es_object:
        search_list = search_in_elastic(es_object, 'documents', search_query)
        result = database_search(create_session(), search_list)
        return {"result": result}
    else:
        return {"result": 'Connection Error'}


@app.post('/documents/remove')
def delete_by_id(document_id: int):
    """Удаление документов из Elastic и MySQL по id"""
    es_object = connect_elasticsearch()
    if es_object:
        es_result = delete_by_id_elastic(es_object, 'documents', document_id)
        db_result = delete_by_id_db(create_session(), document_id)
        return {"result": (es_result, db_result)}
    else:
        return {"result": 'Connection Error'}


if __name__ == "__main__":
    uvicorn.run("main:app", reload=True, use_colors=True)
