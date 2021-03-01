# initIndicators.py
import pystore
import functools
from concurrent import futures
from datetime import datetime
import time

import numpy as np
import pandas as pd
import talib as ta
import tulipy as ti


def addIndicators(stock, collectionName, sourceStore, destStore):
    
    sourceCollection = sourceStore.collection(collectionName)
    destCollection = destStore.collection(collectionName)

    # Add indicators

    # print ('Starting Extraction', stock)
    # extract the dataframe
    i = sourceCollection.item(stock).to_pandas()

    # i=i[:-200]

    startTime = time.time()

    # print ('Starting Indicator Generation ')

    try:        
        i['SMASlow'] = ta.SMA(i.Close, timeperiod=26)
        i['SMAFast'] = ta.SMA(i.Close, timeperiod=12)
        i['SMAFaster'] = ta.SMA(i.Close, timeperiod=5)

        i['SMASlope'] = ta.LINEARREG_SLOPE(i['SMAFast'], timeperiod=9)

        i['TRIX'] = ta.TRIX(i.Close, timeperiod=14)
        i['TRIXSlope'] = ta.LINEARREG_SLOPE(i['TRIX'], timeperiod=9)
        
        i['MACDValue'], i['MACDSignal'], i['MACDHist'] = ta.MACD(
            i.Close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        i['RSI'] = ta.RSI(i.Close, timeperiod=14)
        
        i['MOM'] = ta.MOM(i.Close, timeperiod=10)
        
        i['ROC'] = ta.ROC(i.Close, timeperiod=10)
        
        i['ADX'] = ta.ADX(i.High, i.Low, i.Close, timeperiod=14)
        i['ADXR'] = ta.ADXR(i.High, i.Low, i.Close, timeperiod=10)
        i['PLUS_DI'] = ta.PLUS_DI(i.High, i.Low, i.Close, timeperiod=14)
        i['MINUS_DI'] = ta.MINUS_DI(i.High, i.Low, i.Close, timeperiod=14)
        i['ADX_HIST'] = i['PLUS_DI'] - i['MINUS_DI']
        
        i['STOCH_slowk'], i['STOCH_slowd'] = ta.STOCH(
            i.High, i.Low, i.Close, fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
        
        i['WILLR'] = ta.WILLR(i.High, i.Low, i.Close, timeperiod=98)
        
        i['STOCHRSI_fastk'], i['STOCHRSI_fastd'] = ta.STOCHRSI(
            i.Close, timeperiod=14, fastk_period=5, fastd_period=3, fastd_matype=0)
        
        i['ULTOSC'] = ta.ULTOSC(i.High, i.Low, i.Close,
                                timeperiod1=7, timeperiod2=14, timeperiod3=28)
        
        i['ADOSC'] = ta.ADOSC(i.High, i.Low, i.Close, i.Volume,
                            fastperiod=3, slowperiod=10)
        i['AD'] = ta.AD(i.High, i.Low, i.Close, i.Volume)
        
        """
        Percentage Volume Oscillator (PVO): 
        PVO = ((12-day EMA of Volume - 26-day EMA of Volume)/26-day EMA of Volume) x 100
        Signal Line: 9-day EMA of PVO
        PVO Histogram: PVO - Signal Line
        """
        # try:
        #     v12ema = ta.EMA(i.Volume, timeperiod=12)
        #     v26ema = ta.EMA(i.Volume, timeperiod=26)
        #     pvo = 100 * (v12ema - v26ema)/v26ema
        #     pvo_sig = ta.EMA(pvo, timeperiod=9)
        #     pvo_hist = pvo - pvo_sig
        #     i['PVO'] = pvo
        #     i['PVOSignal'] = pvo_sig
        #     i['PVOHist'] = pvo_hist
        # except:
        #     print('PVO Error', j)
        #     pass

        # ROC : rate of Change :  ((price/prevPrice)-1)*100
        # i['ROC']  = ta.ROC(i.Close)

        i['ROC'] = 100 * (i['Close'] / i['Close'].shift(1) - 1)

    except Exception as e:
        print(e)
        print('Append Error', stock)
        return None

    
    endTime = time.time()
    timeDelta = endTime - startTime

    print(stock,'Indicator Generation  took {} secs'.format(timeDelta))



    destCollection.write( stock, i, metadata={'source': 'Yahoo'}, overwrite=True)

    return None


# initialiaze and populate database
# Set storage path
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')

pystore.list_stores()  # returns: [‘mydatastore’]

############### SOURCE COLLECTION 
# Connect to datastore (create it if not exist)
store1 = pystore.store('mydatastore_debug')
store1.list_collections()

# Access a collection (create it if not exist)
collection1 = store1.collection('NASDAQ.SYMBOLS')
stockList = collection1.item('Symbols').to_pandas().Symbol.to_list()



############### DESTINATION COLLECTION 
store2 = pystore.store('mydatastoreProcessed_debug')
store2.list_collections()
# Access a collection (create it if not exist)
collection2 = store2.collection('NASDAQ.SYMBOLS')


collectionName = 'NASDAQ.M15'

#  Record start time
now_time = datetime.now()
print("Initializing/Creating database...", now_time)
# initialize threads for indicator calculations

max_workers = 50
stockList = stockList[:10]
print (stockList)

# in case a smaller number of stocks than threads was passed in
workers = min(max_workers, len(stockList))
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(
        addIndicators, collectionName=collectionName, sourceStore=store1, destStore=store2), stockList)

#timing:
finish_time = datetime.now()
duration = finish_time - now_time
minutes, seconds = divmod(duration.seconds, 60)
print('The threaded script took {} minutes and {} seconds to run.'.format(minutes, seconds) )

