from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker

engine = create_engine("sqlite:///logs.db")
Session = sessionmaker(bind=engine)
Base = declarative_base()

class Log(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    level = Column(String)
    service = Column(String)
    host = Column(String)
    message = Column(String)

Base.metadata.create_all(engine)
