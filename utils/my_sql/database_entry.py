import pandas as pd

from utils.my_sql.database_connection import connect_to_db
from mysql.connector import Error


def data_parse(data_file) -> list:
    """"Считывает данные из csv файла, создает копию данных и преобразует их в список кортежей."""
    df = pd.read_csv(data_file)
    data = df.copy()
    data_tuples = [tuple(row) for row in data.values]
    return data_tuples


def database_entry(data):
    """"Создаёт бд documents если её нету, после чего создаёт таблицу и заполняет её данными"""
    try:
        connection = connect_to_db()
        create_db_query = "CREATE DATABASE documents"
        show_db_query = "SHOW DATABASES"

        # Создает базу данных с именем documents, если она еще не существует.
        with connection.cursor() as cursor:
            cursor.execute(show_db_query)
            if 'documents' not in [name[0] for name in list(cursor)]:
                cursor.execute(create_db_query)
                print("Database successfully created")

        create_table_query = "CREATE TABLE document(id INT PRIMARY KEY AUTO_INCREMENT," \
                             "rubrics varchar(2040) NOT NULL," \
                             "text text NOT NULL," \
                             "created_date date NOT NULL)"
        drop_table_query = "DROP TABLE IF EXISTS document"
        fill_db_query = "INSERT INTO document (text,created_date,rubrics) VALUES (%s, %s, %s)"
        connection.database = "documents"

        # Создает таблицу с именем document в базе данных documents.
        # Заполняет таблицу document данными из списка data.
        with connection.cursor() as cursor:
            cursor.execute(drop_table_query)
            cursor.execute(create_table_query)
            print("Table in database successfully created")
            cursor.executemany(fill_db_query, data)
            connection.commit()
    except Error as er:
        print(f'DB|Error:{er}')
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    data_f = '../../data/posts.csv'
    data_t = data_parse(data_f)
    database_entry(data_t)
