"""

"""

import os
import csv

INPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/task_usage/"
INPUT_FILE_LIST = sorted([name for name in os.listdir(INPUT_PATH) if name.endswith('.csv')])
OUTPUT_PATH = "/media/EEAEDA2FAED9EFD7/GoogleCluster/task_usage_one_day/"
BY_HOUR = 1
ONE_HOUR = 1000 * 1000 * 60 * 60
ONE_DAY = 24 * ONE_HOUR

def csv_writer_row(fileObj, row):
    """

    :param fileObj:  file object get from open() function
    :param row: a list, get from csv.reader object's next() function
    :return: None
    """
    writer = csv.writer(fileObj, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_NONE)
    writer.writerow(row)

if __name__ == "__main__":
    count = 1
    out_file = open(OUTPUT_PATH + str(count) + "_" + str(BY_HOUR) + ".csv", "w")
    print "open to write", out_file.name
    done = False
    for filename in INPUT_FILE_LIST:
        if done:
            break
        in_file = open(INPUT_PATH + filename, "r")
        print "open to read", in_file.name
        reader = csv.reader(in_file, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            if long(row[0]) < ONE_DAY:
                if long(row[0]) > count * BY_HOUR * ONE_HOUR:
                    count += 1
                    out_file.close()
                    out_file = open(OUTPUT_PATH + str(count) + "_" + str(BY_HOUR) + ".csv", "w")
                    print "open to write", out_file.name
                csv_writer_row(out_file, row)
            else:
                done = True
                break
        in_file.close()


