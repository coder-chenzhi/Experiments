

import os
import time
import threading
import csv


INPUT_PATH = "/home/chenzhi/Documents/test/each_machine/"
# INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
INPUT_FILE_LIST = []
for name in os.listdir(INPUT_PATH):
    if name.endswith('.csv') and '_' not in name:
        INPUT_FILE_LIST.append(name)
INPUT_FILE_LIST = sorted(INPUT_FILE_LIST)
TIME_CONVERT = 1000 * 1000 * 300
INSERT_PERIOD = 120
SCALE_TIMES = 32 # how many times to scale
SCALE_RANGE = 9
SCALE_COLUMN_INDEX = 5 # which column to scale, use event_type column now

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string


def simulate():
    print 'Start to scale %s ...' % threading.current_thread().name

    # you must create a Cursor object. It will let
    #  you execute all the queries you need
    output_filename = threading.currentThread().name.split('.')[0] + '_' + str(SCALE_TIMES) + '.csv'
    output_file = open(INPUT_PATH + output_filename, "w")
    writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_NONE)
    print 'scale', SCALE_TIMES, 'times' + 'and write to', output_filename, '...'
    with open(INPUT_PATH + threading.currentThread().name, 'r') as inFile:
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        row = reader.next()
        try:
            insert_count = 0
            # print insert_start
            time_period = 1
            # time_period determine the scaled time the records be inserted in this while loop belong to
            while row is not None and float(row[0]) / TIME_CONVERT < INSERT_PERIOD:
                    for i in range(SCALE_TIMES):
                        scaled_row = row
                        scaled_row[SCALE_COLUMN_INDEX] = int(row[SCALE_COLUMN_INDEX]) + i * SCALE_RANGE
                        writer.writerow(scaled_row)
                        insert_count += 1
                    row = reader.next()
        except StopIteration:
            pass
        output_file.close()
    print 'Insert %s ended.' % threading.current_thread().name
    print 'Insert %s records.' % insert_count


start_time = time.time()
print 'thread %s is running...' % threading.current_thread().name
thread_pool = []
for file_name in INPUT_FILE_LIST:
    t = threading.Thread(target=simulate, name=file_name)
    t.start()
    thread_pool.append(t)

# wait all threads end.
for thread in thread_pool:
    thread.join()

print 'All scale done and cost', str(time.time() - start_time), 's.'