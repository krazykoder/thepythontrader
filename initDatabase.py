''' init_database.py
'''
from datetime import datetime
from concurrent import futures
import functools

# initialiaze and populate database 
import pystore

# Set storage path
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')

pystore.list_stores() # returns: [‘mydatastore’]

# Connect to datastore (create it if not exist)
store = pystore.store('mydatastore')
store.list_collections()


# Access a collection (create it if not exist)
collection = store.collection('NASDAQ.SYMBOLS')


# Download lis to sp500 amd tickers
import pandas as pd
table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
# more  here: ftp://ftp.nasdaqtrader.com/symboldirectory/
df = table[0]
df.to_csv('./data/S&P500-Info.csv')
df.to_csv("./data/S&P500-Symbols.csv", columns=['Symbol'])
collection.write('Symbols', df, metadata={'source': 'Yhoo'}, overwrite=True)



# Finance libraries - all import 
import yfinance as yf
import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web


# select Tick
# stock = 'AMD'

# TICK = yf.Ticker(stock)
# datasets and timeframes 

# stock_df = TICK.history(period='60d', interval='5m',prepost=True) # prepost market 
# stock_df = TICK.history(period='60d', interval='1d') # only market hrs data
# stock_df = TICK.history(period='1d', interval='5m', prepost=True) # only market hrs data

rootdataPath = './data/NASDAQ/'
dataPath =  './data/NASDAQ/'

bad_names =[] #to keep track of failed queries

def download_stock(stock, period='2y', interval='1d', prepost=False) : 
    try:
        TICK = yf.Ticker(stock)
        stock_df = TICK.history(period=period, interval=interval, prepost=prepost)
        # remove cols that are not in use 
        stock_df = stock_df.drop([ 'Dividends', 'Stock Splits'] ,axis=1)

        # modify date timezone to local and reindex dates
        stock_df['Date'] = stock_df.index
        stock_df['Date']= stock_df['Date'].dt.tz_localize(None)
        stock_df.set_index('Date', inplace=True)

        collection.write( stock, stock_df, metadata={'source': 'Yahoo', 'interval': interval}, overwrite=True)

        # stock_df = web.DataReader(stock,'yahoo', start_time, now_time)
        stock_df['Name'] = stock
        output_name = stock + '_'+interval+'.csv'

        # print(stock, output_name)
        stock_df.to_csv( dataPath + output_name)

    except:
        bad_names.append(stock)
        print('bad: %s' % (stock))
    return stock_df

stockList = collection.item('Symbols').to_pandas().Symbol.to_list()

# create directory is not exist to save data csv files 
from pathlib import Path
Path(dataPath).mkdir(parents=True, exist_ok=True)

#  Record start time 
now_time = datetime.now()
print ("Initializing/Creating database...", now_time)
# stockList = stockList[:10]

#set the maximum thread number
max_workers = 50


# Download 1d 3y 
print ("Download 1d data")
period='3y'; interval='1d'; prepost=False;
dataPath = rootdataPath + '/' + interval +'/'
Path(dataPath).mkdir(parents=True, exist_ok=True)
collection = store.collection('NASDAQ.D1')
workers = min(max_workers, len(stockList)) #in case a smaller number of stocks than threads was passed in
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(download_stock, period=period, interval=interval, prepost=prepost), stockList)


# Download 1h(60m) 730d
print ("Download 60m data")
period='730d'; interval='60m';  prepost=True; 
dataPath = rootdataPath + '/' + interval +'/'
Path(dataPath).mkdir(parents=True, exist_ok=True)
collection = store.collection('NASDAQ.H1')
workers = min(max_workers, len(stockList)) #in case a smaller number of stocks than threads was passed in
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(download_stock, period=period, interval=interval, prepost=prepost), stockList)


# Download 15m 60d 
print ("Download 15m data")
period='60d'; interval='15m'; prepost=True;
dataPath = rootdataPath + '/' + interval +'/'
Path(dataPath).mkdir(parents=True, exist_ok=True)
collection = store.collection('NASDAQ.M15')
workers = min(max_workers, len(stockList)) #in case a smaller number of stocks than threads was passed in
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(download_stock, period=period, interval=interval, prepost=prepost), stockList)


# Download 5m 60d 
print ("Download 5m data")
period='60d'; interval='5m'; prepost=True;
dataPath = rootdataPath + '/' + interval +'/'
Path(dataPath).mkdir(parents=True, exist_ok=True)
collection = store.collection('NASDAQ.M5')
workers = min(max_workers, len(stockList)) #in case a smaller number of stocks than threads was passed in
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(download_stock, period=period, interval=interval, prepost=prepost), stockList)


""" Save failed queries to a text file to retry """
if len(bad_names) > 0:
    with open('./data/failed_queries_60m_2Y.txt','w') as outfile:
        for name in bad_names:
            outfile.write(name+'\n')

#timing:
finish_time = datetime.now()
duration = finish_time - now_time
minutes, seconds = divmod(duration.seconds, 60)
print (bad_names)
print('The threaded script took {} minutes and {} seconds to run.'.format(minutes, seconds) )




############################  MAP FUNCTION 

# def add(x, y):
#     print ('Add', x+y)
#     return x + y

# a = [1, 2, 3]
# b = [10,3,5]

# print (list (map (add, a, b)))


# result = map(functools.partial(add, y=2), a)
# print (list(result))