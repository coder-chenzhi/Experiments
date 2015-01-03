"""
@author: zhichen
@date: 8, Dec, 2014

"""

import os
import csv
import time
import cProfile
import pstats
from os.path import join


START=0 # INCLUDE
END=0 # INCLUDE
OUTPUT_PATH = '/home/chenzhi/Documents/test/idle/split_by_machine_315/'
INPUT_PATH = '/home/chenzhi/Documents/test/idle/split_by_machine_1/'
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
INPUT_FILE_LIST = INPUT_FILE_LIST[START:END+1]
MAX_OPEN_FILE = 15000
MACHINE_ID_COL_INDEX = 4  # start from 0


class FileHandler(object):
    def __init__(self, max_size):
        self.map = {}
        self.max_size = max_size

    def __contains__(self, item):
        return item in self.map

    def __getitem__(self, item):
        return self.map[item]

    def __setitem__(self, key, value):
        if len(self.map) > self.max_size:
            count = self.max_size / 2
            del_key_list = []
            for k in self.map:
                if count > 0:
                    self.map[k].close()
                    del_key_list.append(k)
                count -= 1
            for k in del_key_list:
                self.map.pop(k, None)
        self.map[key] = value

    def __iter__(self):
        return self.map.iterkeys()

def csvWriterRow(fileObj, row):
    """

    :param fileObj:  file object get from open() function
    :param row: a list, get from csv.reader object's next() function
    :return: None
    """
    writer = csv.writer(fileObj, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_NONE)
    writer.writerow(row)

def main():
    print 'All files should be parsed:'
    print INPUT_FILE_LIST
    start_time = time.time()
    outputFileHandler = FileHandler(MAX_OPEN_FILE)
    for filename in INPUT_FILE_LIST:
        file_start_time = time.time()
        print 'Start parse file ' + filename + '...'
        path = join(INPUT_PATH, filename)
        inFile = open(path, 'r')
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        # file operation
        for row in reader:
            outFile = None
            id = row[MACHINE_ID_COL_INDEX]
            if id == '':
                id = 'empty'
            if id in outputFileHandler:
                outFile = outputFileHandler[id]
            else:
                path = join(OUTPUT_PATH, id + '.csv')
                outFile = open(path, 'a')
                outputFileHandler[id] = outFile
            csvWriterRow(outFile, row)
        # close input file
        inFile.close()
        print 'Finish parse ' + filename + ' and cost ' + str(time.time() - file_start_time) + ' s.'
    # close output files
    for key in outputFileHandler:
        outputFileHandler[key].close()
    print 'Finish parse all files and cost ' + str(time.time() - start_time) + ' s.'

if __name__ == '__main__':
    cProfile.run('main()','log')
    p = pstats.Stats('log')
    p.sort_stats('cumulative').print_stats()