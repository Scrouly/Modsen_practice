from utils.database.database_connection import create_session
from utils.database.database_models import Document
import json


def database_get_all(session):
    """"Возвращает все записи из бд"""
    try:
        with session() as session:
            select_query = session.query(Document.id, Document.text)
            result = select_query.all()
            print(f'Number of returned documents: {len(result)}')
            result_list = [({'id': post.id, 'text': post.text}) for post in result]

    except Exception as er:
        print(f'DB|Error:{er}')

    for document in result_list:
        yield '{ "index" : { "_index" : "%s", "_type" : "%s", "_id" : "%s"}}' % (
            "documents", "document", document["id"])

        yield json.dumps(document, default=int)


if __name__ == "__main__":
    session = create_session()

