__author__ = 'ubuntu'

import os
import sys

# INPUT_FILE = "/home/chenzhi/log/1/172.18.9.42-23:11:53.log"
# OUTPUT_FILE = "/home/chenzhi/out.csv"
INPUT_PATH = "/home/chenzhi/log/32/"
CYCLE = 8
CPU_INDEX = [1, 8]
MEM_INDEX = [3, 4]
DISK_INDEX = [5, 5] # three value [5, 5] [6, 5] [7, 5]

if __name__ == "__main__":
    for file_name in sorted(os.listdir(INPUT_PATH)):
        matrix = []
        cpu_metrics = []
        mem_metrics = []
        disk_metrics = []
        reader = open(os.path.join(INPUT_PATH, file_name), 'r')
        # ignore the first two line
        reader.readline()
        reader.readline()
        print 'Begin to read...'
        line = reader.readline()
        while line != '':
            if line == '\n':
                line = reader.readline()
                continue
            else:
                matrix.append(line.split())
                line = reader.readline()
        reader.close()
        print "Finish to read..."
        print 'Total length', len(matrix)

        if len(matrix) % CYCLE != 0:
            print "Can't be divide exactly!!!"
            sys.exit()
        for i in range(len(matrix)/CYCLE - 1): # minus 1 to omit last cycle, because the last is summary
            cpu_metrics.append(str(100 - float(matrix[CPU_INDEX[0] + i * CYCLE][CPU_INDEX[1]])))
            mem_metrics.append(matrix[MEM_INDEX[0] + i * CYCLE][MEM_INDEX[1]])
            disk_metrics.append(str((float(matrix[DISK_INDEX[0] + i * CYCLE][DISK_INDEX[1]]) +
                                float(matrix[DISK_INDEX[0] + i * CYCLE + 1][DISK_INDEX[1]]) +
                                float(matrix[DISK_INDEX[0] + i * CYCLE + 2][DISK_INDEX[1]])) / 3))
        # print cpu_metrics
        # print mem_metrics
        # print disk_metrics
        print 'Begin to output...'
        # output = open(OUTPUT_FILE, 'w')
        # output.write(",".join(cpu_metrics) + '\n')
        # output.write(",".join(mem_metrics) + '\n')
        # output.write(",".join(disk_metrics) + '\n')
        # output.close()
        print file_name
        print("\t".join(cpu_metrics) + '\n')
        # print(",".join(mem_metrics) + '\n')
        # print(",".join(disk_metrics) + '\n')
