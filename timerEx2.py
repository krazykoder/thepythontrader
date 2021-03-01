# # timerEx2.py

# # https://stackoverflow.com/questions/12435211/python-threading-timer-repeat-function-every-n-seconds
# # The best way is to start the timer thread once. Inside your timer thread you'd code the following


from threading import Thread, Event
import time, datetime

class MyThread(Thread):
    def __init__(self, event):
        Thread.__init__(self)
        self.stopped = event

    def run(self):
        print ('Starting run now')
        while not self.stopped.wait(timeout=2):  #interval set to 1s ; will execute function every interval
            print(time.ctime(), "my thread is running",  flush=True)
            # call a function
            # self.printMe()
        print ('Exiting Run now.')

    def printMe(self): 
        print ('Timer running',  flush=True)


# In the code that started the timer, you can then set the stopped event to stop the timer.

stopFlag = Event()
thread = MyThread(stopFlag)
thread.start()
# stopFlag.set()

'''
stopFlag = Event()
thread = MyThread(stopFlag)
thread.start()
# this will stop the timer
stopFlag.set()
'''




# import threading, time
# def fun1(a, b):
#     c = a + b
#     interval = 5 # secs 
#     time.sleep(interval)
#     print(c, 'after', interval, 'secs')

# thread1 = threading.Thread(target = fun1, args = (12, 10))
# thread1.start()

# # thread1.cancel()