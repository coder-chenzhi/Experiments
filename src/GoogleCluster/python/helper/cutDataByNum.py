"""
cut data

"""

import csv
import time
import os

INPUT_PATH = "/home/chenzhi/Documents/test/"
OUTPUT_PATH = "/home/chenzhi/Documents/test/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
NUM = 500000

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

if __name__ == "__main__":
    start_time = time.time()
    print 'start at', get_current_time()
    output_file = open(INPUT_PATH + "New10M.csv", "a")
    writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_NONE)
    for filename in INPUT_FILE_LIST:
        input_file = open(INPUT_PATH + filename, "r")
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