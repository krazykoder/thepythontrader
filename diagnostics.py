# diagnostics.py


# Thread Pool executor Example   ###############3

from yahoofinancials import YahooFinancials
from datetime import date, datetime, timedelta
import pystore
from pandas import DataFrame
from dateutil import parser
import yfinance as yf
import pandas_datareader.data as web
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from time import sleep

# the function to run on a thread


def return_after_n_secs(message, n=10):
    sleep(n)
    print('Slept ', n, 'sec. returning message')
    return message


pool = ThreadPoolExecutor(3)

future1 = pool.submit(return_after_n_secs, ("hello5"))
future2 = pool.submit(return_after_n_secs, ("hello3"))

print('Is future done: ', future1.done())
print('Is future done: ', future2.done())

print('sleeping 5 [main]')
sleep(5)
print('slept 5 [main')

print('Is future done: ', future1.done())
print('Is future done: ', future2.done())

print(future1.result())
print(future2.result())


#####################    COLLECTION LOADER    ##############

# Set storage path
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')

# Connect to datastore (create it if not exist)
store = pystore.store('mydatastore_debug')
store.list_collections()

# Access a collection (create it if not exist)
symbols = store.collection('NASDAQ.SYMBOLS')
symbols.list_items()

collection = store.collection('NASDAQ.D1')
collection = store.collection('NASDAQ.H1')
collection = store.collection('NASDAQ.M15')
collection = store.collection('NASDAQ.M5')

collection.list_items()


# check last item for an item = 'AMD'
collection.item('AMD').to_pandas().tail(10)
collection.item('AMD').to_pandas().tail()


#####################          YF TICK download historical           ##############

# select Tick
stock = 'AMD'
TICK = yf.Ticker(stock)

delta = timedelta(minutes=30)
startDate = '2020-05-29 17:00'
startDate = parser.parse(startDate)
endDate = startDate+delta
print(startDate, endDate)

period = '5d'
interval = '60m'
prepost = True
start = parser.parse('2020-06-02 09:00')
delta = timedelta(minutes=30)
end = start + delta
print(start, end)
stock_df = TICK.history(period=period, interval=interval,
                        prepost=prepost, start=start, end=end)
stock_df


period = '5d'
interval = '5m'
prepost = True
start = parser.parse('2020-05-15 09:00')
delta = timedelta(minutes=30)
end = start + delta
print(start, end)
stock_df = TICK.history(period=period, interval=interval,
                        prepost=prepost, start=start, end=end)
stock_df

#####################       yFinance          ##############


tickers = yf.Tickers("msft aapl goog")
# ^ returns a named tuple of Ticker objects

# access each ticker using (example)
tickers.tickers.MSFT.info
tickers.aapl.history(period="1mo")
tickers.goog.actions

# Fetching data for multiple tickers
# multiindex

%time data = yf.download(["SPY", "AAPL", "AMD", "KLAC", "NVDA", 'MSFT', 'BRK.B'], start="2017-01-01", end="2017-01-30")


%time data = yf.download(["SPY", "AAPL", "INTC"], start="2017-01-01", end="2017-01-30")
data.dtypes
# access
data['Close']
%time p = data['Close']


%time data = yf.download(["SPY", "AAPL"], start="2017-01-01", end="2017-01-30", group_by='ticker')
data.dtypes
# access
data['AAPL']
%time p = data['AAPL']


# try larger list of stocks: 98 stocks listed here
stocklist = ['MMM', 'ABT', 'ABBV', 'ABMD', 'ACN', 'ATVI', 'ADBE', 'AMD', 'AAP', 'AES', 'AFL', 'A', 'APD', 'AKAM', 'ALK', 'ALB', 'ARE', 'ALXN', 'ALGN', 'ALLE', 'ADS', 'LNT', 'ALL', 'GOOGL', 'GOOG', 'MO', 'AMZN', 'AMCR', 'AEE', 'AAL', 'AEP', 'AXP', 'AIG', 'AMT', 'AWK', 'AMP', 'ABC', 'AME', 'AMGN', 'APH', 'ADI', 'ANSS', 'ANTM', 'AON', 'AOS', 'APA', 'AIV', 'AAPL',
             'AMAT', 'APTV', 'ADM', 'ANET', 'AJG', 'AIZ', 'T', 'ATO', 'ADSK', 'ADP', 'AZO', 'AVB', 'AVY', 'BKR', 'BLL', 'BAC', 'BK', 'BAX', 'BDX', 'BBY', 'BIIB', 'BLK', 'BA', 'BKNG', 'BWA', 'BXP', 'BSX', 'BMY', 'AVGO', 'BR', 'CHRW', 'COG', 'CDNS', 'CPB', 'COF', 'CAH', 'KMX', 'CCL', 'CARR', 'CAT', 'CBOE', 'CBRE', 'CDW', 'CE', 'CNC', 'CNP', 'CTL', 'CERN', 'CF', 'SCHW']
len(stocklist)

# download list w/ groupby columns (default)
%time dataL = yf.download(stocklist, start="2019-01-01", end="2019-01-05")
# access
dataL['Close']
%time p = dataL['Close']

# download list w/ groupby tickers - little easier for implementation
%time dataD = yf.download(stocklist, start="2017-01-01", end="2017-01-05", group_by='ticker')
dataD['AAPL']
%time p = data['AAPL']

#####################       yahoofinancials          ##############
# this is not fast enough to beat 
# however it does get the data as JSON which might be useful for rendering instead
# https://pypi.org/project/yahoofinancials/

# ! pip install yahoofinancials
# ! pip install upgrade pip

from yahoofinancials import YahooFinancials

# basic usage 
yahoo_financials = YahooFinancials('TSLA')
data = yahoo_financials.get_historical_price_data(start_date='2000-01-01', end_date='2019-12-31',time_interval='weekly')

# bulk usage 
tech_stocks = ['AAPL', 'SPY', 'INTC']
yahoo_financials_tech = YahooFinancials(tech_stocks)

yahoo_financials_tech = YahooFinancials(stocklist[:10])

%time p = yahoo_financials_tech.get_historical_price_data('2017-01-01', '2017-01-05', 'daily')

# not working 
yahoo_financials_tech.get_200day_moving_avg()
yahoo_financials_tech.get_current_price()
yahoo_financials_tech.get_market_cap()
yahoo_financials_tech.get_stock_earnings_data()
yahoo_financials_tech.get_stock_earnings_data(reformat=True)



######## Compare fetch times
TICKER = 'AMD'
yahoo_financials = YahooFinancials(TICKER)
%time data = yahoo_financials.get_historical_price_data(start_date='2017-01-01', end_date='2017-01-05',time_interval='daily')
%time dataD = yf.download(TICKER, start="2017-01-01", end="2017-01-05",interval='1d')

TICK = yf.Ticker(TICKER)
%time dataP = TICK.history(interval='1d',start="2017-01-01", end="2017-01-05", prepost=True)

TICKER = 'BRB.B'
TICK = yf.Ticker(TICKER)
TICK.history(interval='1d',start="2017-01-01", end="2017-01-05", prepost=True)

TICK.get_recommendations()


#####################       NEW           ##############
