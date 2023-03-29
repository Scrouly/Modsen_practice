from pprint import pprint
import os
import pandas as pd
from utils.elastic.elastic_entry import connect_elasticsearch
from utils.elastic.elastic_search import search_in_elastic
from utils.sql_alchemy.database_connection import create_session

from utils.sql_alchemy.database_models import Document


def database_search(session, id_list):
    """"Принимает id после поиска в эластике, возвращает 20 первых постов по дате(max -> min)"""
    try:

        with session() as session:
            select_query = session.query(Document).filter(Document.id.in_([_id['id'] for _id in id_list])).order_by(
            Document.created_date.desc()).limit(20)
            result = select_query.all()
            print(f'Number of returned documents: {len(result)}')

        # Записывает результат возвращённый из бд в .csv
        posts = pd.DataFrame([(post.id, post.rubrics, post.text, post.created_date) for post in result],
                             columns=['id', 'rubrics', 'text', 'created_date'])
        posts.to_csv(os.path.abspath("posts_search_result.csv"), index=False)

        return result
    except Exception as er:
        print(f'DB|Error:{er}')



if __name__ == "__main__":
    es = connect_elasticsearch()
    index_name = 'documents'
    search_query = 'Перевед'
    search_list = search_in_elastic(es, index_name, search_query)
    database_search(create_session(), search_list)
