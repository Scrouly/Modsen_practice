from mysql.connector import Error

from utils.my_sql.database_connection import connect_to_db
from utils.sql_alchemy.database_connection import create_session
from utils.sql_alchemy.database_models import Document


def delete_by_id_db(id):
    """"Удаляет документ по его id из базы данных"""
    try:
        connection = connect_to_db()
        delete_query = f"DELETE FROM document WHERE id = {id}"
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            connection.commit()
        print('DB|The document was deleted')

    except Error as er:
        print(f'DB|Error:{er}')

    finally:
        if connection:
            connection.close()


def delete_by_id_db(session, id):
    """"Удаляет документ по его id из базы данных"""
    try:
        with session() as session:
            document = session.query(Document).filter(Document.id == id).first()
            if document is not None:
                session.delete(document)
                session.commit()
                print(f'DB|The document with id {id} was deleted')
            else:
                print(f'DB|Error: document with id {id} does not exist')
    except Exception as er:
        print(f'DB|Error:{er}')
    finally:
        session.close()

if __name__ == "__main__":
    delete_by_id_db(create_session(),3)

