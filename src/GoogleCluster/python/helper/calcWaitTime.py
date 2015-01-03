"""

"""

import os

INPUT_PATH = "/home/chenzhi/Documents/test/idle/log/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])

def merge_min_list(list1, list2):
    tmp = []
    if len(list1) != len(list2):
        print "Can't merge two unequal length list!"
        raise Exception
    else:
        for i in range(len(list1)):
            tmp.append(min(list1[i], list2[i]))
    return tmp


if __name__ == "__main__":
    total_wait_list = [1] * 120
    for INPUT_FILE in INPUT_FILE_LIST:
        current_wait_list = [0] * 120
        with open(INPUT_PATH + INPUT_FILE, "r") as input_file:
            total_wait_time = 0
            row = input_file.readline()
            timestamps = row.split(',')
            # test if the last one is empty
            if timestamps[-1] == '\n':
                timestamps = timestamps[:len(timestamps) - 1]
            timestamps = [float(timestamp) for timestamp in timestamps]
            for i in range(len(timestamps)):
                current_wait_list[long(timestamps[i])] = long(timestamps[i]) + 1 - timestamps[i]
        total_wait_list = merge_min_list(total_wait_list, current_wait_list)

    print "total_wait_time", sum(total_wait_list)