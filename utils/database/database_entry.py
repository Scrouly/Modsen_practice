import pandas as pd

from sqlalchemy_utils import database_exists, create_database

from utils.database.database_models import Document, Base


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
            print("DB|Database successfully created")

        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
        print("DB|Table successfully created")

        with session() as session:

            documents = [Document(text=text, created_date=date, rubrics=rubrics) for text, date, rubrics in data]
            session.add_all(documents)
            session.commit()
            print("DB|Data successfully inserted into the Table")
    except Exception as er:
        print(f'DB|Error:{er}')
