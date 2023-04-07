import os

from utils.database.database_get_all_data import database_get_all
from utils.elastic.elastic_connection import connect_elasticsearch
from utils.elastic.elastic_entry import elastic_entry
from utils.database.database_entry import data_parse
from utils.database.database_connection import create_session, make_engine
from utils.database.database_entry import database_entry

file_name = os.path.abspath("data/posts.csv")
es = connect_elasticsearch()
session = create_session()
engine = make_engine()


if __name__ == "__main__":
    #Вызов функций по заполнению/перезаписи Elastic и MySQL
    database_entry(session, make_engine(), data_parse(file_name))
    data = database_get_all(session)
    elastic_entry(es, data)
