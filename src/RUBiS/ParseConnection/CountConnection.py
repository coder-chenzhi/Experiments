'''
@date 24, Nov, 2014
@author: Zhichen
@require: sortedcontainers
https://github.com/grantjenks/sorted_containers

'''

import os
import sys
import time
from os.path import join
from sortedcontainers.sorteddict import SortedDict

class Time(object):
    '''
    handle time of record, example:
    19:11:50
    '''

    def __init__(self, *args, **kwargs):
        '''

        @param args: tuple of anonymous arguments
        @param kwargs: dictionary of named arguments
        '''
        # if argument is one string
        if len(args) == 1 and isinstance(args[0], str):
            lst = args[0].split(':')
            self.hour = lst[0]
            self.minute = lst[1]
            self.second = lst[2]
        else:
            print 'Invalid argument format! Please input one string!'
            sys.exit()

    def __eq__(self, other):
        assert isinstance(other, type(self))
        if other.hour == self.hour and other.minute == self.minute \
                and other.second == self.second:
            return True
        else:
            return False

    def __gt__(self, other):
        if int(other.hour) != int(self.hour):
            return int(other.hour) < int(self.hour)
        elif int(other.minute) != int(self.minute):
            return (other.minute) < int(self.minute)
        else:
            return int(other.second) < int(self.second)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.hour + ':' + self.minute + ':' + self.second

    def __hash__(self):
        return hash((self.hour, self.minute, self.second))


class ConnectionRecord(object):
    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], str):
            self.portFrom = args[0].split(' ')[2].split(':')[1]
            self.portTo = args[0].split(' ')[3].split(':')[1]
            self.time = Time(args[0].split(' ')[0])
        else:
            print 'Invalid argument! Please input one string!'
            sys.exit()

    def __eq__(self, other):
        if type(other) == type(self):
            if other.portFrom == self.portFrom and other.portTo == self.portTo \
                    and other.time == self.time:
                return True
            else:
                return False
        else:
            print 'Invalid compared type!'
            sys.exit()

    def __gt__(self, other):
        if type(other) == type(self):
            if  other.portFrom != self.portFrom:
                return other.portFrom < self.portFrom
            elif other.portTo != self.portTo:
                return other.portTo < self.portTo
            else:
                return other.time < self.time
        else:
            print 'Invalid compared type!'
            sys.exit()

    def __repr__(self):
        return 'from port ' + self.portFrom + ' ' + ' to port ' \
               + self.portTo + ' at ' + str(self.time)

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return hash((self.portFrom, self.portTo, self.time))

class ConnectionKey(object):
    '''
    The key class of connection map

    '''

    def __init__(self, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], str):
            self.ipFrom = args[0].split(' ')[2].split(':')[0]
            self.ipTo = args[0].split(' ')[3].split(':')[0]
        else:
            print 'Invalid argument! Please input one string!'
            sys.exit()

    def __eq__(self, other):
        assert isinstance(other, type(self))
        if other.ipFrom == self.ipFrom and other.ipTo == self.ipTo:
            return True
        else:
            return False

    def __gt__(self, other):
        assert isinstance(other, type(self))
        if other.ipFrom != self.ipFrom:
            return other.ipFrom < self.ipFrom
        else:
            return other.ipTo < self.ipTo

    def __repr__(self):
        return 'Connection from ' + self.ipFrom + ' to ' + self.ipTo

    def __str__(self):
        return self.__repr__()

    def __hash__(self):
        return hash((self.ipFrom, self.ipTo))


class ConnectionValue(object):
    def __init__(self):
        self.value = {}
        self.count = 0

    def __contains__(self, item):
        return item in self.value.keys()

    def __getitem__(self, key):
        return self.value[key]

    def __setitem__(self, key, value):
        self.value[key] = value

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        string = ''
        for key in self.value.keys():
            string += '\t' + str(key) + ' ' + str(self[key]) + ' times\n'
        return string

    def addCount(self):
        self.count += 1

    def Count(self):
        return self.count

if __name__ == '__main__':
    # # test class time
    # a = Time('00:15:49')
    # b = Time('00:15:59')
    # print sorted([a, b])
    DATA_PATH = 'F:\Temp\Exp\\testdata'
    connect = {}
    '''
    s1 = '00:15:49 tcp 172.18.9.3:22 172.18.9.18:38683'
    s2 = '00:15:50 udp 172.18.9.3:22 172.18.9.18:38683'
    s3 = '00:15:51 udp 172.18.9.1:22 172.18.9.18:38683'
    s4 = '00:15:51 udp 172.18.9.1:22 172.18.9.18:38683'
    k = (ConnectionKey(s1), ConnectionKey(s2), ConnectionKey(s3), ConnectionKey(s4))
    r = (ConnectionRecord(s1), ConnectionRecord(s2), ConnectionRecord(s3), ConnectionRecord(s4))
    for i in range(len(k)):
        if k[i] in connect:
            if r[i] in connect[k[i]]:
                connect[k[i]][r[i]] += 1
                connect[k[i]].addCount()
            else:
                connect[k[i]][r[i]] = 1
                connect[k[i]].addCount()
        else:
            v = ConnectionValue()
            v[r[i]] = 1
            v.addCount()
            connect[k[i]] = v
    '''
    start = time.time()
    print start
    for root, dirs, files in os.walk(DATA_PATH):
        for dataFile in files:
            print 'handle file', join(root, dataFile)
            fileContent = open(join(root, dataFile))
            for line in fileContent:
                startline = time.time()
                line = line.replace('\n', '')
                k = ConnectionKey(line)
                r = ConnectionRecord(line)
                if k in connect:
                    if r in connect[k]:
                        connect[k][r] += 1
                        connect[k].addCount()
                    else:
                        connect[k][r] = 1
                        connect[k].addCount()
                else:
                    v = ConnectionValue()
                    v[r] = 1
                    v.addCount()
                    connect[k] = v
                print time.time() - startline
    end = time.time()
    print 'cost ' + str(end - start)
    for key in connect:
        print key
        #print str(connect[key]),
        print 'total connection: ' + str(connect[key].Count()) + '\n'

