"""
test throughput of database
"""

import os
import csv
import time
import MySQLdb
import threading

# INPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/UsedData/throughput/split_equally_1/"
# INPUT_FILE_LIST = sorted([INPUT_PATH + name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
# INPUT_FILE_LIST = ["/media/EEAEDA2FAED9EFD7/GoogleCluster/task_events_one_day/1_1.csv"]
INPUT_FILE_LIST = ["/home/chenzhi/Documents/tmp/1000k.csv"]

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
            execute_time = time.time()
            execute(db, cur, row)
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
                          db="googlecluster")  # name of the data base
        open_file = open(fileName, 'r')
        t = threading.Thread(target=simulate, name=fileName, args=(db, open_file,))
        thread_pool.append(t)

    for thread in thread_pool:
        thread.start()


    # wait all threads end.
    for thread in thread_pool:
        thread.join()

    print 'All insert end and cost', str(time.time() - start_time), 's.'
