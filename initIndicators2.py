# %%
# initIndicators.py\
import functools
import time
from concurrent import futures
from datetime import datetime

import numpy as np
import pandas as pd
import pandas_ta as ta  # seems to be best, mature 
import ta as ta3
import talib as ta2  # best but some functions missing

import pystore


# %%

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

    print(stock, 'Indicator Generation  took {} secs'.format(timeDelta))

    destCollection.write(stock, i, metadata={'source': 'Yahoo'}, overwrite=True)

    return None


# %%

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
collectionName = 'NASDAQ.D1'

#  Record start time
now_time = datetime.now()
print("Initializing/Creating database...", now_time)
# initialize threads for indicator calculations

max_workers = 50
stockList = stockList[:10]
print(stockList)

# in case a smaller number of stocks than threads was passed in
workers = min(max_workers, len(stockList))
with futures.ThreadPoolExecutor(workers) as executor:
    res = executor.map(functools.partial(
        addIndicators, collectionName=collectionName, sourceStore=store1, destStore=store2), stockList)

# timing:
finish_time = datetime.now()
duration = finish_time - now_time
minutes, seconds = divmod(duration.seconds, 60)
print('The threaded script took {} minutes and {} seconds to run.'.format(minutes, seconds))

# %%

############################   Trying something new 
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')
pystore.list_stores()  # returns: [‘mydatastore’]
collectionName = 'NASDAQ.D1'
# store2 = pystore.store('mydatastoreProcessed_debug')
store2 = pystore.store('mydatastore_debug')
store2.list_collections()
destCollection = store2.collection(collectionName)
destCollection.list_items()
df = stockList = destCollection.item('AMD').to_pandas()


# %%
#
# import AlphaVantage
# def farm(ticker = 'SPY', drop=['dividend', 'split_coefficient']):
#     AV = AlphaVantage(api_key="YOUR API KEY", premium=False, clean=True, output_size='full')
#     df = AV.data(symbol=ticker, function='D')
#     df.set_index(['date'], inplace=True)
#     df.drop(['dividend', 'split_coefficient'], axis=1, inplace=True) if 'dividend' in df.columns and 'split_coefficient' in df.columns else None
#     df.name = ticker
#     return df

def ctitle(indicator_name, ticker='SPY', length=100):
    return f"{ticker}: {indicator_name} from {recent_startdate} to {recent_startdate} ({length})"


def cscheme(colors):
    aliases = {
        'BkBu': ['black', 'blue'],
        'gr': ['green', 'red'],
        'grays': ['silver', 'gray'],
        'mas': ['black', 'green', 'orange', 'red'],
    }
    aliases['default'] = aliases['gr']
    return aliases[colors]


def machart(kind, fast, medium, slow, append=True, last=last_, figsize=price_size, colors=cscheme('mas')):
    title = ctitle(f"{kind.upper()}s", ticker=ticker, length=last)
    ma1 = df.ta(kind=kind, length=fast, append=append)
    ma2 = df.ta(kind=kind, length=medium, append=append)
    ma3 = df.ta(kind=kind, length=slow, append=append)

    madf = pd.concat([closedf, df[[ma1.name, ma2.name, ma3.name]]], axis=1, sort=False).tail(last)
    madf.plot(figsize=figsize, title=title, color=colors, grid=True)


def volumechart(kind, length=10, last=last_, figsize=ind_size, alpha=0.7, colors=cscheme('gr')):
    title = ctitle("Volume", ticker=ticker, length=last)
    volume = pd.DataFrame({'V+': volumedf[closedf > opendf], 'V-': volumedf[closedf < opendf]}).tail(last)

    volume.plot(kind='bar', figsize=figsize, width=0.5, color=colors, alpha=alpha, stacked=True)
    vadf = df.ta(kind=kind, close=volumedf, length=length).tail(last)
    vadf.plot(figsize=figsize, lw=1.4, color='black', title=title, rot=45, grid=True)


price_size = (16, 8)
ind_size = (16, 2)
ticker = 'SPY'
recent = 126
half_of_recent = int(0.5 * recent)

last_ = df.shape[0]
recent_startdate = df.tail(recent).index[0]
recent_enddate = df.tail(recent).index[-1]
# print(f"{df.name}{df.shape} from {recent_startdate} to {recent_enddate}\n{df.describe()}")
df.head()

# rename columns
# df.columns = ['open', 'high', 'low', 'close', 'volume', 'SMASlow', 'SMAFast',
#        'SMAFaster', 'SMASlope', 'TRIX', 'TRIXSlope', 'MACDValue', 'MACDSignal',
#        'MACDHist', 'RSI', 'MOM', 'ROC', 'ADX', 'ADXR', 'PLUS_DI', 'MINUS_DI',
#        'ADX_HIST', 'STOCH_slowk', 'STOCH_slowd', 'WILLR', 'STOCHRSI_fastk',
#        'STOCHRSI_fastd', 'ULTOSC', 'ADOSC', 'AD']

# collectionName = 'NASDAQ.D1'
# # store2 = pystore.store('mydatastoreProcessed_debug')
# store2 = pystore.store('mydatastore_debug')
# store2.list_collections()
# destCollection = store2.collection(collectionName)

# %%
df.columns = ['open', 'high', 'low', 'close', 'volume']

opendf = df['open']
closedf = df['close']
volumedf = df['volume']

import matplotlib.pyplot as plt

# ways to generate indicators : Examples:: pandas-ta = ta , ta = ta3, ta-lib=ta2
macddf = df.ta.macd(fast=8, slow=21, signal=9, min_periods=None, append=True)
df_ta_new = ta3.add_volatility_ta(df=df, high="high", low="low", close="close", fillna=True)

# %%
# plot plots

macddf[[macddf.columns[0], macddf.columns[2]]].tail(recent).plot(figsize=(16, 2), color=cscheme('BkBu'), linewidth=1.3)
macddf[macddf.columns[1]].tail(recent).plot.area(figsize=ind_size, stacked=False, color=['silver'], linewidth=1,
                                                 title=ctitle(macddf.name, ticker=ticker, length=recent),
                                                 grid=True).axhline(y=0, color="black", lw=1.1)

machart('ema', 8, 21, 50, last=recent)
volumechart('ema', last=recent)

rsiWilder = df.ta.rsi(period=14)

matype = 'sma'
fast_length = 10
medfast_length = 20
slow_length = 50

aobvdf = ta.aobv(close=closedf, volume=volumedf, mamode=matype, fast=fast_length, slow=medfast_length)
aobv_colors = ['black', 'silver', 'silver', 'green', 'red']
aobv_trenddf = aobvdf[aobvdf.columns[-2:]]
aobv_trenddf.name = f"{aobvdf.name} Trends"

machart(matype, fast_length, medfast_length, slow_length,
        last=recent)  # Price Chart so we can see the association with AOBV
volumechart('ema', length=5, last=recent)
aobvdf[aobvdf.columns[:5]].tail(recent).plot(figsize=ind_size, color=aobv_colors,
                                             title=ctitle(aobvdf.name, ticker=ticker, length=recent), grid=True)
aobv_trenddf.tail(recent).plot(kind='area', figsize=(16, 0.35), color=cscheme('gr'), alpha=0.5,
                               title=ctitle(aobv_trenddf.name), stacked=False)

# %%
# Volume Profile 
import matplotlib.pyplot as plt

newdf = df.ta.vp()
# newdf
plt.bar(newdf.mean_close, newdf.total_volume)  # plot a bar chart of the volume vs price distribution
