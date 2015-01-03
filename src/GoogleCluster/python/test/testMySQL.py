'''

'''

import MySQLdb
import csv
import os
import traceback

INPUT_PATH = "/home/ubuntu/Documents/task_events_by_machine/"
INPUT_FILES = os.listdir(INPUT_PATH)
INPUT_FILE = INPUT_FILES[0]


# read from csv file
inFile = open(INPUT_PATH + INPUT_FILE, 'r')
reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)
# print dir(reader)
row = reader.next()
inFile.close()
print row
db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="root", # your username
                      passwd="123", # your password
                      db="GoogleCluster") # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
query = '''insert into task_events values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
try:
    cur.execute(query, row)
    db.commit()
except Exception, e:
    print 'fail to insert value' + str(row)
    print traceback.format_exc()
    db.rollback()

db.close()
