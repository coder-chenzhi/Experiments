"""
test threading.Thread's parameter 'args' pass object, not reference

"""


import threading
import MySQLdb

db1 = MySQLdb.connect(host="localhost",  # your host, usually localhost
                      user="root",  # your username
                      passwd="123",  # your password
                      db="GoogleCluster")  # name of the data base

print id(db1)

def print_id(db):
    print id(db)

t = threading.Thread(target=print_id, args=(db1,))
t.start()