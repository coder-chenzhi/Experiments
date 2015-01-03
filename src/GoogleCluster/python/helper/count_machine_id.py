'''
@author: zhichen
@date: 17, Dec, 2014

count the number of unique machine id contained by certain file

'''

import os
import csv

INPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/task_events_by_300_machines/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])


if __name__ == '__main__':
    for file_name in INPUT_FILE_LIST:
        unique_machine_id = set()
        with open(INPUT_PATH + file_name, 'r') as inFile:
            reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
            for row in reader:
                unique_machine_id.add(row[4])
        print file_name, len(unique_machine_id)
        print sorted(list(unique_machine_id))