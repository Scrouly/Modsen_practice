from mysql.connector import Error

from utils.my_sql.database_connection import connect_to_db


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


if __name__ == "__main__":
    delete_by_id_db(3)
