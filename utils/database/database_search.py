import os
import pandas as pd

from utils.database.database_models import Document


def database_search(session, id_list):
    """"Принимает id после поиска в эластике, возвращает 20 первых постов по дате(max -> min)"""
    try:
        if id_list:
            with session() as session:
                select_query = session.query(Document).filter(Document.id.in_([_id['id'] for _id in id_list])).order_by(
                    Document.created_date.desc()).limit(20)
                result = select_query.all()
                print(f'Number of returned documents: {len(result)}')

            # Записывает результат возвращённый из бд в .csv

            posts = pd.DataFrame([(post.id, post.rubrics, post.text, post.created_date) for post in result],
                                 columns=['id', 'rubrics', 'text', 'created_date'])
            posts.to_csv(os.path.abspath("data/posts_search_result.csv"), index=False)

            return result
        else:
            return "Nothing was found for your query"
    except Exception as er:
        print(f'DB|Error:{er}')
