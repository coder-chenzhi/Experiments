'''
@author: zhichen
@date: 9, Dec, 2014

'''

import os
from subprocess import call

FILE = '/home/chenzhi/test_16'
CPU = [1, 3]
CPU_LIST = []
MEM = [4, 4]
MEM_LIST = []
DISK = [8, 5]
DISK_LIST = []
LOOP_LINES = 16

if __name__ == '__main__':
    # call(['mkdir', 'test'])
    with open(FILE, 'r') as output:
        row = output.readline()
        row_cache = []
        while row != '':
            row_cache.append(row.strip("\n").split())
            for i in range(LOOP_LINES - 1):
                row_cache.append(output.readline().strip("\n").split())
            CPU_LIST.append(float(row_cache[CPU[0]][CPU[1]]))
            MEM_LIST.append(float(row_cache[MEM[0]][MEM[1]]))
            DISK_LIST.append(float(row_cache[DISK[0]][DISK[1]]))
            row = output.readline()
            row_cache = []
print CPU_LIST
print MEM_LIST
print DISK_LIST

