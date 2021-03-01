# Execute function at a particular time. 
# https://stackoverflow.com/questions/15088037/python-script-to-do-something-at-the-same-time-every-day
# You can do that like this:

from datetime import datetime, timedelta
from threading import Timer

x=datetime.today()
y = x.replace(day=x.day, hour=0, minute=0, second=4, microsecond=0) + timedelta(seconds=5)
delta_t=y-x

# secs=delta_t.total_seconds()
secs = 3

def hello_world():
    print ("hello world")
    #...

t = Timer(secs, hello_world)
t.start()

# This will execute a function (eg. hello_world) in the next day at 1a.m.

# Decisions 
# 15 min, 1H, 4H, 1D 
# EMA : Price :: a few cases 
# RSI vs MA(20) vs MA(40)
# MACD cross 
# Golden Cross 
# BB 
# Volume Profile (PV)