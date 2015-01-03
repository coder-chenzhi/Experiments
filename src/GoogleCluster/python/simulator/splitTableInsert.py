'''
@author: zhichen
@date: 19, Dec, 2014

Simulate the process of GoogleCluster data's generation.
This the improved version. Compared with primary version, only one process insert data into database,
due to 300 files have been combined together.

'''


import os
import csv
import logging
import MySQLdb
import traceback
import time
import threading

INPUT_PATH = "/home/chenzhi/Documents/test/"
LOG_PATH = '/home/chenzhi/Documents/test/log/'

TIME_CONVERT = 1000 * 1000 * 300
INSERT_PERIOD = 120
SCALE_TIMES = 8 # how many times to scale
SCALE_RANGE = 9
SCALE_COLUMN_INDEX = 5 # which column to scale, use event_type column now
INPUT_FILE_LIST = ["0_" + str(SCALE_TIMES) + ".csv"]
INSERT_TASK_EVENTS_1 = '''insert into task_events_1 values (%s, %s, %s, %s, %s, %s)'''
INSERT_TASK_EVENTS_2 = '''insert into task_events_2 values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
INSERT_TASK_USAGE = '''insert into task_usage values (%s, %s, %s, %s, %s, %s, %s,
                                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''


def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string


def execute(db, cur, row, query):
    """
    code that will be operated when trigger
    :param row:
    :return: no return
    """
    # logging insert history, but there are too many insert, so just logging fail insert.
    # logging.info('Insert ' + str(time.time()) + ' ' + str(row[0:6]))
    try:
        cur.execute(query, row)
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
        row = reader.next()
        try:
            insert_start = time.time()
            total_wait_time = 0
            insert_count = 0
            # print insert_start
            time_period = 1
            # time_period determine the scaled time the records be inserted in this while loop belong to
            while row is not None:
                # judge if the timestamp belong to the scaled time this while loop should operate,
                # if it is, insert this record into database, if not, judge if current time exceed
                # the time_period this while loop should operate, if it is, continue to insert and
                # increment the time_period, if not, finish this while loop and wait until time has
                # reached next time_period.
                if float(row[0]) / TIME_CONVERT > INSERT_PERIOD:
                    break
                if float(row[0]) / TIME_CONVERT < time_period or time.time() > insert_start + time_period:
                    # print 'insert'
                    if time.time() > insert_start + time_period:
                        time_period += 1
                    tmp = row[0:6]
                    index = [0,2,3,5,6,7,8,9,10,11,12]
                    tmp2 = [row[i] for i in index]
                    execute(db, cur, tmp, INSERT_TASK_EVENTS_1)
                    execute(db, cur, tmp2, INSERT_TASK_EVENTS_2)
                    insert_count += 1
                    row = reader.next()
                else:
                    print 'wait'
                    wait_start = time.time()
                    time_period += 1
                    while time.time() < insert_start + (time_period - 1):
                        pass
                    total_wait_time += (time.time() - wait_start)
        except StopIteration:
            pass
    db.close()
    print 'Insert %s ended.' % threading.current_thread().name
    print 'Insert %s records.' % insert_count
    print 'Total wait time', total_wait_time
    logging.info('Insert %s ended.' % threading.current_thread().name)


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