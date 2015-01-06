"""
@author: ZhiChen
@date: 4, Jan, 2015

"""

"""
test throughput of database
"""

import os
import csv
import time
import MySQLdb
import threading

INPUT_PATH = "/home/chenzhi/Documents/test/throughput/split_equally_32/"
# INPUT_FILE_LIST = ["0_500000.csv"]
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
INSERT_TASK_EVENTS = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_13 = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_9 = '''insert into task_events_9 values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_8 = '''insert into task_events_8 values (%s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_70 = '''insert into task_events_70 values (%s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_71 = '''insert into task_events_71 values (%s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_72 = '''insert into task_events_72 values (%s, %s, %s, %s, %s, %s, %s)'''


def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string


def execute(db, cur, row, query):
    """
    code that will be operated when trigger
    :param row:
    :param db:
    :return: no return
    """
    # logging insert history, but there are too many insert, so just logging fail insert.
    # logging.info('Insert ' + str(time.time()) + ' ' + str(row[0:6]))
    try:
        cur.execute(query, row)
        db.commit()
    except Exception, e:
        # logging.info('fail to insert value' + str(row))
        # logging.info(traceback.format_exc())
        pass
    db.rollback()


def simulate(db, input_file):
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
    print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    row = reader.next()
    try:
        while row is not None:
            # no split
            # execute(db, cur, row)

            # split two tables
            tmp = row[0:9]
            index = [0,2,3,5,9,10,11,12]
            tmp1 = [row[i] for i in index]
            execute(db, cur, tmp, INSERT_TASK_EVENTS_9)
            execute(db, cur, tmp1, INSERT_TASK_EVENTS_8)

            # split three tables
            # tmp = row[0:7]
            # index = [0,2,3,5,7,8,9]
            # tmp1 = [row[i] for i in index]
            # index = [0, 2, 3, 5, 10, 11, 12]
            # tmp2 = [row[i] for i in index]
            # execute(db, cur, tmp, INSERT_TASK_EVENTS_70)
            # execute(db, cur, tmp1, INSERT_TASK_EVENTS_71)
            # execute(db, cur, tmp2, INSERT_TASK_EVENTS_72)

            row = reader.next()
    except StopIteration:
        pass
    input_file.close()
    db.close()
    print 'Insert %s ended.' % threading.current_thread().name

if __name__ == "__main__":
    start_time = time.time()

    thread_pool = []
    # prepare for simulate
    for fileName in INPUT_FILE_LIST:
        db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                          user="root",  # your username
                          passwd="123",  # your password
                          db="GoogleCluster")  # name of the data base
        open_file = open(INPUT_PATH + fileName, 'r')
        t = threading.Thread(target=simulate, name=fileName, args=(db, open_file,))
        thread_pool.append(t)

    for thread in thread_pool:
        thread.start()


    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'All insert end and cost', str(time.time() - start_time), 's.'
