"""
cut data by time

"""

import csv
import time

INPUT_PATH = "/home/chenzhi/Documents/test/"
OUTPUT_PATH = "/home/chenzhi/Documents/test/"
FILE_NAME = "0.csv"
TIME_CONVERT = 1000 * 1000 * 300
TIME = 120

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%Y-%m-%d %H:%M:%S", localtime)
    return time_string

if __name__ == "__main__":
    start_time = time.time()
    print 'start at', get_current_time()
    input_file = open(INPUT_PATH + FILE_NAME, "r")
    output_file = open(INPUT_PATH + FILE_NAME.split('.')[0] + '_' + str(TIME) + "s.csv", "a")
    reader = csv.reader(input_file, delimiter=',', quoting=csv.QUOTE_NONE)
    writer = csv.writer(output_file, delimiter=',', quoting=csv.QUOTE_NONE)
    for row in reader:
        if float(row[0]) <= TIME * TIME_CONVERT:
            writer.writerow(row)
    input_file.close()
    output_file.close()
    print "Done. Cost", str(time.time() - start_time), "s."