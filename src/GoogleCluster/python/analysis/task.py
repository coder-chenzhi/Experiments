__author__ = 'chenzhi'


import os
import csv
import matplotlib.pyplot as plt

INPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/full_task_events/"
OUTPUT_FILE = "/home/chenzhi/Documents/OneHourTaskSubmit"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
TIME_CONVERT = 1000 * 1000 * 60 * 60

def main():
    submit_task = []
    for file_name in INPUT_FILE_LIST:
        content = open(INPUT_PATH + file_name, "r")
        for line in content:
            task_event = line.split(",")
            if task_event[5] == "1":
                try:
                    # the reason why use long(float()) is there are few scientific notation record like 2e+10
                    if len(submit_task) - 1 < long(float(task_event[0])) / TIME_CONVERT:
                        submit_task.extend([0] * (long(float(task_event[0])) / TIME_CONVERT - len(submit_task) + 1))
                except:
                    print file_name, task_event[0]
                submit_task[-1] += 1
        print file_name + " done."
        content.close()
    writer = open(OUTPUT_FILE, "w")
    for record in submit_task:
        writer.write(str(record) + "\n")
    writer.close()
    # remove the first record when plot
    plt.plot(range(len(submit_task) - 1), submit_task[1:])
    plt.show()

if __name__ == "__main__":
    main()


