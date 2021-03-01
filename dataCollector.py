import functools
from concurrent import futures
from datetime import date, datetime, timedelta
from threading import Event, Thread

import dask
import pandas as pd
import pandas_datareader.data as web
import yfinance as yf
from dateutil import parser
from pandas import DataFrame

import pystore


class dataCollector(Thread):

    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def initialize(self, frequency, period, interval, prepost, delta, startDate, pystorePath, storeName, symbolName, collectionName):
        self.frequency = frequency 
        self.period = period 
        self.interval = interval 
        self.prepost = prepost 
        self.delta = delta 
        self.startDate = startDate 
        self.pystorePath = pystorePath 
        self.storeName = storeName 
        self.symbolName = symbolName 
        self.collectionName = collectionName 

    def download_stock(self, stock, period, interval, prepost, collection, startDate, delta):

        # startDate=parser.parse(startDate)
        end = startDate + delta

        # print (stock, "i am here", startDate, end)
        try:
            TICK = yf.Ticker(stock)
            stock_df = TICK.history(
                period=period, interval=interval, prepost=prepost, start=startDate, end=end, threads=False)
            # remove cols that are not in use
            stock_df = stock_df.drop(['Dividends', 'Stock Splits'], axis=1)

            # modify date timezone to local and reindex dates
            stock_df['Date'] = stock_df.index
            stock_df['Date'] = stock_df['Date'].dt.tz_localize(None)
            stock_df.set_index('Date', inplace=True)

            # print (stock, "still here")
            # print(stock_df)

            # Append data to collection
            try:
                work = dask.delayed(self.daskAppend)(
                    stock, stock_df, collection)
                # collection.append(stock, stock_df)  # too slow +3s / 10 stock append

                return work  # compute once all operations ready in main function

            except Exception as e:
                print(e)
                print('Append Error', stock)
                return None
                pass

        except Exception as e:
            # bad_names.append(stock)
            # print('Bad: %s' % (stock))
            print (e) 
            return None
            pass

    def daskAppend(self, stock, stock_df, collection):

        # print (stock, stock_df)
        # item = collection.item(stock).data
        # item.
        collection.append(stock, stock_df)

    def daskAppend2(self, stock, stock_df, collection):

        # print (stock, stock_df)
        item = collection.item(stock).data

        # item.
        # collection.append(stock, stock_df)


    def run(self):
        ## Thread configurations:
        # Download 1d 3y
        # frequency = 1  # seconds
        # period = '5d'
        # interval = '1d'
        # prepost = False
        # delta = timedelta(days=1)
        # startDate = parser.parse('2020-05-26')
        # pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
        # storeName = 'mydatastore_debug'
        # symbolName = 'NASDAQ.SYMBOLS'
        # collectionName = 'NASDAQ.D1'

        frequency = self.frequency 
        period = self.period 
        interval = self.interval 
        prepost = self.prepost 
        delta = self.delta 
        startDate = parser.parse(self.startDate)
        pystorePath = self.pystorePath 
        storeName = self.storeName 
        symbolName = self.symbolName 
        collectionName = self.collectionName 

        # Set storage path
        pystore.set_path(pystorePath)

        # Connect to datastore (create it if not exist)
        store = pystore.store(storeName)

        collection = store.collection(collectionName)
        symbols = store.collection(symbolName)

        # Access a collection (create it if not exist)
        stockList = []
        stockList = symbols.item('Symbols').to_pandas().Symbol.to_list()
        stockList = stockList[:100]
        stockList.remove('BRK.B')
        stockList.remove('BF.B')


        print(stockList)

        # the infinite timer loop
        while not self.stopped.wait(frequency):

            now_time = datetime.now()

            startDate = startDate+delta

            print(collectionName, period, "Updating  for:", startDate)

            # set the maximum thread number
            max_workers = 50

            # in case a smaller number of stocks than threads was passed in
            workers = min(max_workers, len(stockList))

            res = []  # collection of work # dask delayed work
            with futures.ThreadPoolExecutor(workers) as executor:
                res = executor.map(functools.partial(self.download_stock, period=period, interval=interval,
                                                     prepost=prepost, collection=collection, startDate=startDate, delta=delta), stockList)

            # collector = []
            # for stock in stockList:
            #     s = self.download_stock(stock, period, interval, prepost, collection, startDate, delta)
            #     if s is not None :
            #         collector.append ((stock, s))

            # for item in collector :
            #     stock, stock_df = item
            #     try :
            #         collection.append(stock, stock_df)
            #     except:
            #         pass

            dask.compute(*res)
            print(collectionName, period, len(stockList), 'stock took ', datetime.now()-now_time)


# Debugging functions ()

# stopFlag = Event()
# frequency = 1  # seconds
# period = '5d'
# interval = '1d'
# prepost = False
# delta = timedelta(days=1)
# startDate = '2020-05-26'
# pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
# storeName = 'mydatastore_debug'
# symbolName = 'NASDAQ.SYMBOLS'
# collectionName = 'NASDAQ.D1'

# mythread = dataCollector(stopFlag)
# mythread.initialize(frequency, period, interval, prepost, delta, startDate, pystorePath, storeName, symbolName, collectionName)
# mythread.start()
