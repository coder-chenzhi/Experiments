"""

"""
import os
import csv
import time

NUM = 2
INPUT_PATH = ""
OUTPUT_PATH = ""
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
OUTPUT_FILE_POOL = [open(OUTPUT_PATH + "New" + i + ".csv", 'w') for i in range(NUM)]

if __name__ == "__main__":
    count = 0
    for filename in INPUT_FILE_LIST:
        print "Start to parse", filename
        reader = open(INPUT_PATH + filename, 'r')
        for line in reader:
            OUTPUT_FILE_POOL[count % NUM].write(line)
            count = (count + 1) % NUM
        print "Parse", filename, 'done.'
    for output in OUTPUT_FILE_POOL:
        output.close()
    print "Done!"