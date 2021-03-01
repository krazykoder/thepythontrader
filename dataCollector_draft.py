from datetime import datetime
from concurrent import futures

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web
import yfinance as yf

import pystore

# Set storage path
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')


# import quandl
# aapl = quandl.get('WIKI/AAPL')
# aapl.head()

# select Tick
stock = 'AMD'

TICK = yf.Ticker(stock)
# stock_df = TICK.history(period='60d', interval='5m',prepost=True) # prepost market 
# stock_df = TICK.history(period='3y', interval='1d') # only market hrs data
# stock_df = TICK.history(period='1d', interval='1d') # only market hrs data
# stock_df.tail()

stock_df = TICK.history(period='730d', interval='60m', prepost=True) # only market hrs data
# stock_df = TICK.history(period='60d', interval='15m') # only market hrs data
from dateutil import parser 
from datetime import date, timedelta, datetime

end=parser.parse('2020-04-16')
start = end - timedelta(days=1)
print (start, end )
stock_df = TICK.history(period='2y', interval='1d', prepost=True, start=start, end=end)
stock_df.tail()


period='1y'; interval='1d'; prepost=True; 
start= parser.parse('2020-04-15'); 
delta = timedelta(days=1); 
end = start + delta
print (start, end )
stock_df = TICK.history(period=period, interval=interval, prepost=prepost, start= start, end=end)
stock_df



period='1y'; interval='60m'; prepost=True; 
start= parser.parse('2020-04-15'); 
delta = timedelta(days=1); 
end = start + delta
print (start, end )
stock_df = TICK.history(period=period, interval=interval, prepost=prepost, start= start, end=end)
stock_df

# from dateutil import parser 
# from datetime import date, timedelta, datetime

# end= parser.parse('2020-06-25 15:00:00')
# start = end - timedelta(hours=6)
# print (start, end)
# stock_df = TICK.history(period='5d', interval='15m', prepost=True, start=start, end=end)
# stock_df.tail(8)


# remove cols that are not in use 
stock_df = stock_df.drop([ 'Dividends', 'Stock Splits'] ,axis=1)

# modify date timezone to local and reindex dates
stock_df['Date'] = stock_df.index
stock_df['Date']= stock_df['Date'].dt.tz_localize(None)
stock_df.set_index('Date', inplace=True)
            
# List stores
pystore.list_stores()  # returns []

# Connect to datastore (create it if not exist)
store = pystore.store('mydatastore')
store.list_collections()

# Access a collection (create it if not exist)
collection = store.collection('NASDAQ.H1')

collection.list_items()

# Store the data in the collection under AAPL
# %time collection.write('AAPL', stock_df[:-1], metadata={'source': 'Yhoo'})
collection.write( stock, stock_df[:-5], metadata={'source': 'Yhoo'}, overwrite=True)

# List all items in the collection
collection.list_items()

# Reading the item’s data
# %time item = collection.item('AAPL')

d = collection.item('KLAC')
d.data
collection.item('KLAC').to_pandas().tail()

# how to append overlapping data slices (index takes care of overlap)
collection.append('KLAC', stock_df[-35:])

# show tail 
collection.item('KLAC').to_pandas().tail(10)

df = collection.item('KLAC').to_pandas()
df.loc['2020-05-21 09:45']

#######################################


import pandas as pd

# pandas options 
pd.options.display.precision = 2
pd.options.display.max_rows = 10


df.Open[:200].plot()
df.Close[:200].plot()


from datetime import datetime
import time
import pytz


startTime = datetime(2020, 7, 21, 6, 25, tzinfo=pytz.timezone('US/Eastern'))


startTime = datetime(2020, 7, 21, 6, 25)
ind = pytz.timezone('US/Eastern')


startTime = ind.localize(startTime)
startTime

print (startTime)

import os, time 
os.environ['TZ'] = 'US/Eastern'
time.tzset()

