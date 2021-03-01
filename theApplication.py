# theApplication.py

from datetime import date, datetime, timedelta
from threading import Event, Thread

from dateutil import parser
from pandas import DataFrame

from dataCollector import dataCollector






# 4 threads of data collectors with configs for each 
def initDataCollectors() : 
    stopFlag = Event()
    mythreadD1 = dataCollector(stopFlag)
    mythreadH1 = dataCollector(stopFlag)
    mythreadM5 = dataCollector(stopFlag)
    mythreadM15 = dataCollector(stopFlag)

    frequency = 1  # seconds
    period = '1d'
    interval = '1d'
    prepost = False
    delta = timedelta(days=1)
    startDate = '2020-05-26'
    pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
    storeName = 'mydatastore_debug'
    symbolName = 'NASDAQ.SYMBOLS'
    collectionName = 'NASDAQ.D1'

    mythreadD1.initialize(frequency, period, interval, prepost, delta,
                          startDate, pystorePath, storeName, symbolName, collectionName)

    mythreadD1.start()


    # frequency = 1  # seconds
    # period = '5d'
    # interval = '60m'
    # prepost = False
    # delta = timedelta(minutes=60)
    # startDate = '2020-06-05 06:20' # local time if time is specified
    # pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
    # storeName = 'mydatastore_debug'
    # symbolName = 'NASDAQ.SYMBOLS'
    # collectionName = 'NASDAQ.H1'

    # mythreadH1.initialize(frequency, period, interval, prepost, delta,
    #                       startDate, pystorePath, storeName, symbolName, collectionName)

    # mythreadH1.start()


    frequency = 1  # seconds
    period = '1d'
    interval = '15m'
    prepost = True
    delta = timedelta(minutes=15)
    startDate = '2020-06-01 06:00'   # local time if time is specified
    pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
    storeName = 'mydatastore_debug'
    symbolName = 'NASDAQ.SYMBOLS'
    collectionName = 'NASDAQ.M15'

    mythreadM5.initialize(frequency, period, interval, prepost, delta,
                          startDate, pystorePath, storeName, symbolName, collectionName)

    mythreadM5.start()



    # frequency = 1  # seconds
    # period = '5d'
    # interval = '5m'
    # prepost = True
    # delta = timedelta(minutes=5)
    # startDate = '2020-06-01 06:20'   # local time if time is specified
    # pystorePath = '/home/towshif/code/python/thepythontrader/pystore'
    # storeName = 'mydatastore_debug'
    # symbolName = 'NASDAQ.SYMBOLS'
    # collectionName = 'NASDAQ.M5'

    # mythreadM5.initialize(frequency, period, interval, prepost, delta,
    #                       startDate, pystorePath, storeName, symbolName, collectionName)

    # mythreadM5.start()
 

if __name__ == '__main__':

    """ Start the main application """

    # init thread datacollector 
    initDataCollectors()



