"""
cut data

"""

import csv
import time
import os

# INPUT_PATH = "/home/chenzhi/Documents/test/"
OUTPUT_PATH = "/home/chenzhi/Documents/tmp/"
# INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
INPUT_FILE_LIST = ["/media/EEAEDA2FAED9EFD7/GoogleCluster/task_events_by_300_machines/0.csv"]
NUM = 1000000

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

if __name__ == "__main__":
    start_time = time.time()
    print 'start at', get_current_time()
    output_file = open(OUTPUT_PATH + "1M.csv", "a")
    writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_NONE)
    for filename in INPUT_FILE_LIST:
        input_file = open(filename, "r")
        reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
        count = 0
        for row in reader:
            if count < NUM:
                writer.writerow(row)
                count += 1
            else:
                break
        input_file.close()
    output_file.close()
    print "Done. Cost", str(time.time() - start_time), "s."