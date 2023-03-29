import pandas as pd
from sqlalchemy import inspect

from sqlalchemy_utils import database_exists, create_database, drop_database

from utils.sql_alchemy.database_connection import make_engine, create_session
from utils.sql_alchemy.database_models import Document, Base


def data_parse(data_file) -> list:
    """"Считывает данные из csv файла, создает копию данных и преобразует их в список кортежей."""
    df = pd.read_csv(data_file)
    data = df.copy()
    data_tuples = [tuple(row) for row in data.values]
    return data_tuples


def database_entry(session, engine, data):
    """"Создаёт бд documents если её нету, после чего создаёт таблицу и заполняет её данными"""
    try:
        if not database_exists(engine.url):
            create_database(engine.url, encoding='utf8mb4')
            print("Database successfully created")


        Base.metadata.create_all(engine)
        print("Table successfully created")

        with session() as session:
            documents = [Document(text=text, created_date=date, rubrics=rubrics) for text, date, rubrics in data]
            session.add_all(documents)
            session.commit()
            print("Data successfully inserted into the Table")
    except Exception as er:
        print(f'DB|Error:{er}')


if __name__ == "__main__":
    database_entry(create_session(), make_engine(), data_parse('../../data/posts.csv'))
