"""

"""

import os
import csv
import time
import cProfile
import pstats

FILE_NUM = 64
OUTPUT_PATH = '/home/chenzhi/Documents/test/idle/split_by_machine_64/'
INPUT_PATH = '/home/chenzhi/Documents/test/idle/split_by_machine_1/'
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
MACHINE_ID_COL_INDEX = 4  # start from 0
MAX_OPEN_FILE = 1000


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
    output_files = []
    id_to_file = {}
    count = 0
    start_time = time.time()

    for i in range(FILE_NUM):
        file_name = open(OUTPUT_PATH + str(i) + ".csv", "w")
        output_files.append(file_name)

    for file_name in INPUT_FILE_LIST:
        file_start_time = time.time()
        print 'Start parse file ' + file_name + '...'
        path = os.path.join(INPUT_PATH, file_name)
        inFile = open(path, 'r')
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        # file operation
        for row in reader:
            out_file = None
            id = row[MACHINE_ID_COL_INDEX]
            if id in id_to_file:
                out_file = id_to_file[id]
            else:
                id_to_file[id] = output_files[count % FILE_NUM]
                out_file = output_files[count % FILE_NUM]
                count += 1
            csvWriterRow(out_file, row)
        # close input file
        inFile.close()
        print "count", count
        print 'Finish parse ' + file_name + ' and cost ' + str(time.time() - file_start_time) + ' s.'
    # close output files
    for file in output_files:
        file.close()
    print 'Finish parse all files and cost ' + str(time.time() - start_time) + ' s.'

if __name__ == '__main__':
    cProfile.run('main()','log')
    p = pstats.Stats('log')
    p.sort_stats('cumulative').print_stats()