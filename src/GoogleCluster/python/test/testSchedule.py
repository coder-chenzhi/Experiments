import os
import csv
import time
import threading

INPUT_PATH = "/home/ubuntu/test/"
INPUT_FILES = os.listdir(INPUT_PATH)
TIME_CONVERT = 1000 * 1000 * 300
# print INPUT_FILES


def execute(row):
    '''
    code that will be operated when trigger
    :param row:
    :return: no return
    '''
    print 'time ' + str(time.time()) + ' ' + str(row)


def simulate():
    print 'thread %s is running...' % threading.current_thread().name

    with open(INPUT_PATH + threading.currentThread().name, 'r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        # operate the first row
        row = reader.next()
        execute(row)
        pre = row
        row = reader.next()
        try:
            while row is not None:
                # s.enter((int(row[0]) - int(pre[0])) * 1.0 / TIME_CONVERT, 1, execute(row), ())
                print 'sleep time ' + str((int(row[0]) - int(pre[0])) * 1.0 / TIME_CONVERT)
                time.sleep((int(row[0]) - int(pre[0])) * 1.0 / TIME_CONVERT)
                execute(row)
                pre = row
                row = reader.next()
        except StopIteration:
            pass
    print 'Insert %s ended.' % threading.current_thread().name

print 'thread %s is running...' % threading.current_thread().name

for fileName in INPUT_FILES:
    t = threading.Thread(target=simulate, name=fileName)
    t.start()

# t = threading.Thread(target=simulate, name=INPUT_FILES[0])
# t.start()