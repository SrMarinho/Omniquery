from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class NfFaturemento(Base):
    __tablename__ = "nf_faturemento"
    id = Column(
        Integer().with_variant(Numeric(10, 0), "mssql"),
        primary_key=True,
        autoincrement=True,
    )
    name = Column(String)