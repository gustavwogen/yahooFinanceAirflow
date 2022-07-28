from sqlalchemy import Integer, Float, Numeric, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class FinanceData(Base):
    __tablename__ = 'FinanceData'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    lastTradeUTC = Column(DateTime)
    symbol = Column(String)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    change = Column(Float)
    changePercent = Column(Float)
    twoHundredDayAverage = Column(Float)
    twoHundredDayAverageChange = Column(Float)
    twoHundredDayAverageChangePercent = Column(Float)