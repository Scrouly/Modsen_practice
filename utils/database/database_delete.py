from utils.database.database_connection import create_session
from utils.database.database_models import Document


def delete_by_id_db(session, id):
    """"Удаляет документ по его id из базы данных"""
    try:
        with session() as session:
            document = session.query(Document).filter(Document.id == id).first()
            if document is not None:
                session.delete(document)
                session.commit()
                return f'DB|The document with id {id} was deleted'
            else:
                return f'DB|Error: document with id {id} does not exist'
    except Exception as er:
        print(f'DB|Error:{er}')
