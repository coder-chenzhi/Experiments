'''

'''

import MySQLdb
import csv
import os
import traceback


def test_insert():
    row = ["3600373469", "", "515042969", "11", "369730124", "1", "/fk1fVcVxZ6iM6gHZzqbIyq56m5zrmHfpdcZ/zzkq4c=",
           "2", "0", "0.01562", "0.01553", "0.0002155", "0"]
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                         user="root", # your username
                          passwd="123", # your password
                          db="googlecluster") # name of the data base

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
    print 'Insert Success.'
    db.close()

def test_connection():
    try:
        db = MySQLdb.connect(host="localhost", # your host, usually localhost
                             user="root", # your username
                              passwd="123", # your password
                              db="googlecluster") # name of the data base

        # you must create a Cursor object. It will let
        #  you execute all the queries you need
    except Exception as e:
        print 'Fail to connect'
        print traceback.format_exc()

    print 'Connection Success.'
    db.close()

if __name__ == "__main__":
    test_insert()