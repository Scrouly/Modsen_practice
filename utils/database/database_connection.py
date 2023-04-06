from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dotenv import load_dotenv, find_dotenv
import os


def make_engine():
    load_dotenv(find_dotenv())
    return create_engine(
        f'mysql+mysqlconnector://{os.getenv("mysql_username")}:{os.getenv("mysql_password")}@localhost/documents',
        echo=False)


def create_session():
    return sessionmaker(bind=make_engine())
