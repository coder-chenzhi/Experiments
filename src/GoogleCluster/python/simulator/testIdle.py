'''
@author: zhichen
@date: 19, Dec, 2014

test idle time, when all threads are waiting for next second.

'''


import os
import csv
import MySQLdb
import traceback
import time
import threading

INPUT_PATH = "/home/chenzhi/Documents/test/idle/split_by_machine_32/"
LOG_PATH = '/home/chenzhi/Documents/test/idle/log/'
TIME_CONVERT = 1000 * 1000 * 300
INSERT_PERIOD = 120
# INPUT_FILE_LIST = ["0_" + str(SCALE_TIMES) + ".csv"]
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
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
    try:
        cur.execute(INSERT_TASK_EVENTS, row)
        db.commit()
    except Exception, e:
        # logging.info('fail to insert value' + str(row))
        # logging.info(traceback.format_exc())
        pass
    db.rollback()


def simulate(db, in_file, log_file):
    # print 'Start to insert %s ...' % threading.current_thread().name
    time.sleep(0.1) # force current thread to release time slice, so other threads can gain time slice
    print threading.currentThread().name, 'start at', get_current_time()

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    cur = db.cursor()
    reader = csv.reader(in_file, delimiter=',', quoting=csv.QUOTE_NONE)
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
            maybe_wait_time = time.time()
            if float(row[0]) / TIME_CONVERT < time_period or maybe_wait_time > insert_start + time_period:
                # print 'insert'
                if time.time() > insert_start + time_period:
                    time_period += 1
                execute(db, cur, row)
                insert_count += 1
                row = reader.next()
                maybe_wait_time = time.time()
            else:
                # print threading.currentThread().name, 'wait'
                wait_start = maybe_wait_time
                log_file.write(str(wait_start - insert_start) + ",")
                time_period += 1
                while time.time() < insert_start + (time_period - 1):
                    pass
                total_wait_time += (time.time() - wait_start)
    except StopIteration:
        pass
    in_file.close()
    log_file.write("\n")
    log_file.close()
    db.close()
    # print 'Insert %s ended.' % threading.current_thread().name
    # print 'Insert %s records.' % insert_count, threading.currentThread().name
    print threading.currentThread().name, 'total wait time', total_wait_time
    # print threading.currentThread().name, 'total execute time', total_execute_time
    # print 'start at', start_time_string, 'end at', get_current_time()

if __name__ == "__main__":
    start_time = time.time()
    print 'thread %s is running...' % threading.current_thread().name

    thread_pool = []
    # prepare for simulate
    for fileName in INPUT_FILE_LIST:
        db = MySQLdb.connect(host="localhost",  # your host, usually localhost
                          user="root",  # your username
                          passwd="123",  # your password
                          db="GoogleCluster")  # name of the data base
        open_file = open(INPUT_PATH + fileName, 'r')
        log_file = open(LOG_PATH + fileName, "w")
        t = threading.Thread(target=simulate, name=fileName, args=(db, open_file, log_file))
        thread_pool.append(t)

    for thread in thread_pool:
        thread.start()


    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'All insert end and cost', str(time.time() - start_time), 's.'