from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float
from sqlalchemy.orm import sessionmaker
import yfinance as yf

app = Flask(__name__)
# Connect to the MySQL database
connection_string = "mysql+mysqlconnector://root:Sharmeen_22@localhost:3306/stock_db"
engine = create_engine(connection_string)

Session = sessionmaker(bind=engine)
session = Session()


@app.route('/load_data', methods=['POST'])
def load_data():
    # Retrieve the data from the request
    tickers = request.json['tickers']
    start_date = request.json['start_date']
    end_date = request.json['end_date']

    # Download the data using yfinance
    data = yf.download(tickers, start=start_date, end=end_date, group_by='ticker', interval='1d',
                       auto_adjust=True, prepost=True, actions=True, threads=True, proxy=None, rounding=False)

    # Drop all rows with NaN values
    data = data.dropna()

    Base = declarative_base()

    class Stock(Base):
        __tablename__ = 'yfinance_stocks'
        stock_ticker = Column(String(255), primary_key=True)
        date = Column(Date, primary_key=True)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        volume = Column(Integer)
        dividend = Column(Float)

    Base.metadata.create_all(engine)

    for ticker in tickers:
        for index, row in data[ticker].iterrows():
            stock = Stock(stock_ticker=ticker, date=index,
                          open=row["Open"], high=row["High"], low=row["Low"], close=row["Close"], volume=row["Volume"], dividend=row["Dividends"])
            try:
                session.add(stock)
                session.commit()
            except:
                session.rollback()
                return jsonify({'error': 'Resource not found'}), 404
                # raise
    return jsonify({'status': 'success'}), 200


# Retrieve a stock
@app.route('/latesStockDetails', methods=['GET'])
def retrieve_stock():
    # Retrieve the data from the request
    tickers = request.json['tickers']
    start_date = request.json['start_date']
    end_date = request.json['end_date']

    Base = declarative_base()

    class Stock(Base):
        __tablename__ = 'yfinance_stocks'
        stock_ticker = Column(String(255), primary_key=True)
        date = Column(Date, primary_key=True)
        open = Column(Float)
        high = Column(Float)
        low = Column(Float)
        close = Column(Float)
        volume = Column(Integer)
        dividend = Column(Float)

    # stock = session.query(Stock).filter_by(stock_ticker=ticker).order_by(Stock.date.desc()).first()
    stock = session.query(Stock).filter(Stock.stock_ticker.in_(
        tickers), Stock.date >= start_date, Stock.date <= end_date).order_by(Stock.date.desc()).all()
    if stock:
        stocks = []
        for s in stock:
            stock_dict = {}
            for col in Stock.__table__.columns:
                stock_dict[col.name] = getattr(s, col.name)
            stocks.append(stock_dict)
        return jsonify(stocks), 200
    else:
        return jsonify({'error': 'stock not found'}), 404


if __name__ == '__main__':
    app.run()
