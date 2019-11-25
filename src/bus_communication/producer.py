import threading

import time


def a():
    print("in A before")
    time.sleep(5)
    print("in A after")


t = threading.Thread(target=a)
t.start()

time.sleep(0.1)
assert t.is_alive()
