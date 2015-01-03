"""
@author: zhichen
@date: 16, Dec, 2014

simulate the process of GoogleCluster data's generation
this is the primary version, which every machine will fork 300 threads to simulate 300 machines

"""

import os
import csv
import logging
import MySQLdb
import traceback
import time
import threading

INPUT_PATH = "/home/ubuntu/Documents/"
LOG_PATH = '/home/ubuntu/Documents/log/'
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
TIME_CONVERT = 1000 * 1000 * 600
INSERT_TASK_EVENTS = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_USAGE = '''insert into task_usage values (%s, %s, %s, %s, %s, %s, %s,
                                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''


def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string


def execute(db, cur, row):
    """
    code that will be operated when trigger
    :param row:
    :return: no return
    """
    # logging insert history, but there are too many insert, so just logging fail insert.
    # logging.info('Insert ' + str(time.time()) + ' ' + str(row[0:6]))
    try:
        cur.execute(INSERT_TASK_EVENTS, row)
        db.commit()
    except Exception, e:
        logging.info('fail to insert value' + str(row))
        logging.info(traceback.format_exc())
    db.rollback()


def simulate():
    print 'Start to insert %s ...' % threading.current_thread().name
    logging.info('Start to insert %s ...' % threading.current_thread().name)

    db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                      user="root",  # your username
                      passwd="123",  # your password
                      db="GoogleCluster")  # name of the data base

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()

    with open(INPUT_PATH + threading.currentThread().name, 'r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        # operate the first row
        row = reader.next()
        execute(db, cur, row)
        pre = row
        row = reader.next()
        try:
            while row is not None:
                # s.enter((int(row[0]) - int(pre[0])) * 1.0 / TIME_CONVERT, 1, execute(row), ())
                logging.info('sleep time ' + str((float(row[0]) - float(pre[0])) / TIME_CONVERT))
                time.sleep((float(row[0]) - float(pre[0])) / TIME_CONVERT)
                execute(db, cur, row)
                pre = row
                row = reader.next()
        except StopIteration:
            pass
    db.close()
    print 'Insert %s ended.' % threading.current_thread().name
    logging.info('Insert %s ended.' % threading.current_thread().name)

if __name__ == "__main__":
    start_time = time.time()
    print 'thread %s is running...' % threading.current_thread().name
    logging.basicConfig(filename=LOG_PATH + get_current_time() + '.log',level=logging.DEBUG)
    logging.info('thread %s is running...' % threading.current_thread().name)
    thread_pool = []
    for fileName in INPUT_FILE_LIST:
        t = threading.Thread(target=simulate, name=fileName)
        t.start()
        thread_pool.append(t)

    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'All insert end and cost', str(time.time() - start_time), 's.'