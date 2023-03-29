from sqlalchemy import ARRAY, Column, Date, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Document(Base):
    __tablename__ = 'document'
    id = Column(Integer, primary_key=True)
    rubrics = Column(Text, nullable=False)
    text = Column(Text, nullable=False)
    created_date = Column(Date, nullable=False)
