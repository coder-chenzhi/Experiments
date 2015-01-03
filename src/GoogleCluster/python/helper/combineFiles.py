"""
@author: zhichen
@date: 19, Dec, 2014

Because of new version of simulator, we should combine all files assigned to each machine to one single file,
so we can use just one process to insert data.

"""

import os
import time
import csv
import cProfile
import pstats

INPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/machine_events/"
OUTPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/machine_events_by_300_machines/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
FILE_NUM = 2 # how many files the primary data will be stored in
MACHINE_ID_COL_INDEX = 1  # start from 0


def csv_writer_row(fileObj, row):
    """

    :param fileObj:  file object get from open() function
    :param row: a list, get from csv.reader object's next() function
    :return: None
    """
    writer = csv.writer(fileObj, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_NONE)
    writer.writerow(row)


def get_id_to_file_map(file_name):
    """

    :param file_name: absolute path of file
    :return: a map from machine id to file id
    """
    id_to_file = {}
    file_id = None
    with open(file_name, 'r') as fileObj:
        is_file_id = True
        for row in fileObj:
            if is_file_id:
                file_id = int(row.strip('\n'))
                is_file_id = False
            else:
                machine_id_list = row.strip('\n').split(',')
                for machine_id in machine_id_list:
                    id_to_file[machine_id] = file_id
                is_file_id = True
    return id_to_file


def parse_task_events():
    start_time = time.time()
    id_to_file = {}  # store the map from machineID to file in which this machine's data should be stored
    file_handler = []
    for id in range(FILE_NUM):
        path = os.path.join(OUTPUT_PATH, str(id) + '.csv')
        outFile = open(path, 'a')
        file_handler.append(outFile)

    count = 0
    for file_name in INPUT_FILE_LIST:
        file_start_time = time.time()
        print 'Start parse file ' + file_name + '...'
        path = os.path.join(INPUT_PATH, file_name)
        inFile = open(path, 'r')
        reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
        # file operation
        for row in reader:
            outFile = None
            id = row[MACHINE_ID_COL_INDEX]
            if id in id_to_file:
                outFile = file_handler[id_to_file[id]]
            else:
                id_to_file[id] = count % FILE_NUM
                outFile = file_handler[count % FILE_NUM]
                count += 1
            csv_writer_row(outFile, row)
        inFile.close()
        print 'Finish parse ' + file_name + ' and cost ' + str(time.time() - file_start_time) + ' s.'
    for file_obj in file_handler:
        file_obj.close()
    print 'Finish parse all files and cost ' + str(time.time() - start_time) + ' s.'

    file_to_id = {}
    for file in range(FILE_NUM):
        file_to_id[file] = []
    for id in id_to_file:
        file_to_id[id_to_file[id]].append(id)
    for file in file_to_id:
        print file, sorted(file_to_id[file])


def parse_task_usage():
    file_contain_id = "/media/EEAEDA2FAED9EFD7/GoogleCluster/task_events_by_300_machines/files_contain_id"
    start_time = time.time()
    id_to_file = get_id_to_file_map(file_contain_id)
    file_handler = []
    for id in range(FILE_NUM):
        path = os.path.join(OUTPUT_PATH, str(id) + '.csv')
        out_file = open(path, 'a')
        file_handler.append(out_file)

    count = 0
    miss_hit = 0
    miss_hit_list = []
    for file_name in INPUT_FILE_LIST:
        file_start_time = time.time()
        print 'Start parse file ' + file_name + '...'
        path = os.path.join(INPUT_PATH, file_name)
        in_file = open(path, 'r')
        reader = csv.reader(in_file, delimiter=',', quoting=csv.QUOTE_NONE)
        # file operation
        for row in reader:
            out_file = None
            id = row[MACHINE_ID_COL_INDEX]
            if id in id_to_file:
                out_file = file_handler[id_to_file[id]]
            else:
                miss_hit += 1
                miss_hit_list.append(id)
                id_to_file[id] = count % FILE_NUM
                out_file = file_handler[count % FILE_NUM]
                count += 1
            csv_writer_row(out_file, row)
        in_file.close()
        print 'Finish parse ' + file_name + ' and cost ' + str(time.time() - file_start_time) + ' s.'
    for file_obj in file_handler:
        file_obj.close()
    print 'Finish parse all files and cost ' + str(time.time() - start_time) + ' s.'
    print 'miss hit', str(miss_hit), 'times. As follow:'
    print miss_hit_list

    # file_to_id = {}
    # for file_id in range(FILE_NUM):
    #     file_to_id[file_id] = []
    # for machine_id in id_to_file:
    #     file_to_id[id_to_file[machine_id]].append(machine_id)
    # for file_id in file_to_id:
    #     print file_id, sorted(file_to_id[file_id])


if __name__ == '__main__':
    cProfile.run('parse_task_events()','log')
    p = pstats.Stats('log')
    p.sort_stats('cumulative').print_stats()