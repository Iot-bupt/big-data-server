import threading
import time

class MyThread(threading.Thread):
    def run(self):
        while(True):
            time.sleep(1)
            print("son running")

if __name__ == '__main__':
    t = MyThread()
    t.start()
    print('main finished')
    exit(0)
