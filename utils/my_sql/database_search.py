from pprint import pprint
import pandas as pd
from utils.elastic.elastic_entry import connect_elasticsearch
from utils.elastic.elastic_search import search_in_elastic
from utils.my_sql.database_connection import connect_to_db
from mysql.connector import Error


def database_search(id_list):
    """"Принимает id после поска в эластике, возвращает 20 первых постов по дате(max -> min)"""
    try:
        connection = connect_to_db()

        select_query = f"SELECT * FROM document WHERE id IN ({','.join([str(_id['id']) for _id in id_list])}) " \
                       f"ORDER BY -created_date LIMIT 20"

        with connection.cursor() as cursor:
            cursor.execute(select_query)
            result = cursor.fetchall()

        # Записывает результат возвращённый из бд в .csv
        posts = pd.DataFrame(result, columns=['id', 'rubrics', 'text', 'created_date'])
        posts.to_csv('../../data/posts_search_result.csv', index=False)

        return result
    except Error as er:
        print(f'DB|Error:{er}')
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    es = connect_elasticsearch()
    index_name = 'documents'
    search_query = 'ищу девушку'
    search_list = search_in_elastic(es, index_name, search_query)
    database_search(search_list)
