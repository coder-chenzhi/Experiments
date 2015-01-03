'''
@author: zhichen
@date: 17, Dec, 2014

count the total line number of csv files contained by certain folder

'''

import os

INPUT_PATH = "/home/ubuntu/Documents/task_events_by_300_machines/"
INPUT_FILES = os.listdir(INPUT_PATH)

if __name__ == '__main__':
    row_count = 0
    for fileName in INPUT_FILES:
        with open(INPUT_PATH + fileName, 'r') as inFile:
            count = sum(1 for row in inFile)
        print 'File', fileName, count
        row_count += count
    print row_count