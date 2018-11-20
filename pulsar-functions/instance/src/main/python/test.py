import threading
import functools
import time

# def set_interval(_func=None, *, rate=60):
#
#     def decorator_interval(func):
#         @functools.wraps(func)
#         def func_wrapper():
#             set_interval(func, rate=rate)
#             func()
#         t = threading.Timer(rate, func_wrapper)
#         t.start()
#         return t
#     if _func is None:
#         return decorator_interval
#     else:
#         return decorator_interval(_func)
#
#
# @set_interval(rate=1)
# def printfoo():
#     print("foo")


from threading import Timer, Thread, Event
from datetime import datetime


class perpetualTimer():

    def __init__(self, t, hFunction):
        self.t = t
        self.hFunction = hFunction
        self.thread = Timer(self.t, self.handle_function)

    def handle_function(self):
        self.hFunction()
        self.thread = Timer(self.t, self.handle_function)
        self.thread.start()

    def start(self):
        self.thread.start()

    def cancel(self):
        self.thread.cancel()


def printer():
    tempo = datetime.today()
    print("{}:{}:{}".format(tempo.hour, tempo.minute, tempo.second))


t = perpetualTimer(1, printer)
t.start()




