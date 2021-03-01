import pystore

# Set path to pystore
pystore.set_path('/home/towshif/code/python/thepythontrader/pystore')


# List stores
pystore.list_stores()  # returns []

# Connect to datastore (create it if not exist)
store = pystore.store('mydatastore_debug')
store.list_collections()
# ['NASDAQ.H1', 'NASDAQ.M15', 'NASDAQ.M5', 'NASDAQ.SYMBOLS', 'NASDAQ.D1']

# Access a collection (create it if not exist)
# collection = store.collection('NASDAQ.D1')
collection = store.collection('NASDAQ.M15')

# List all items in the collection
collection.list_items()

####################### Reading the item’s data


# %time item = collection.item('AAPL')

TICK = 'AMD'
d = collection.item(TICK)
d.data
collection.item(TICK).to_pandas().tail()


######################  WRITING DATA ITEMS






######################  APPENDING DATA (new time entry) TO ITEMS





#################################################################################
#                 SPECIAL JOB (Ignore for all other purposes)                   #
#################################################################################

# Objective: PORT from pystore items to pickles as save to another directory 

store = pystore.store('mydatastore')
store.list_collections()
collection = store.collection('NASDAQ.H1')

TICK = 'KLAC'
# d = collection.item(TICK)
df = collection.item(TICK).to_pandas()
df.tail()

saveDataToPickle(df, '/home/towshif/code/python/pTradesDraft/datastore/D1', TICK)

import pickle
def saveDataToPickle(df, path, savefile='testRGB_Data'):
    ''' Save data to piclke after each operation or at a fixed interval '''

    # save the tree #example # treeName = 'testRGB_Data.pickle'
    outfile = open(path + '/' + savefile + '.pickle', 'wb')
    pickle.dump(df, outfile)
    print (path + '/' + savefile, 'saved.' )

# looping through collections and saving pickle to directories

TICK = 'GOOG'   # do manually or loop through a list ['AMD', 'TSLA', 'KLAC']

print ("-----------------", TICK, "-----------------")
for coll in store.list_collections() :
    collection = store.collection(coll)
    timeframe = coll.split('.')[1]
    print ('Accessing collection ::', coll, '| TimeFrame =', timeframe)
    # d = collection.item(TICK)
    try :
        df = collection.item(TICK).to_pandas()
        print (df.tail())
        saveDataToPickle(df, '/home/towshif/code/python/pTradesDraft/datastore/'+ timeframe, TICK)
    except :
        print (coll, TICK, 'does not exist')
        pass

