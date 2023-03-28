from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine


def make_engine():
    return create_engine('mysql+mysqlconnector://root:root@localhost/documents', echo=False)


def create_session():
    return sessionmaker(bind=make_engine())
