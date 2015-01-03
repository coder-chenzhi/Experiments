'''
Created on 22, Nov, 2014
@author: ZhiChen

Check whether connection between different clusters exists.

'''

import os
import sys
from os.path import join
# path of data files
DATA_PATH = 'G:\Temp\Exp\data'
# map between machine's ip address and cluster it belongs to
MAP = {
    '172.18.9.136': 'ab',
    '172.18.9.144': 'ab',
    '172.18.9.145': 'ab',
    '172.18.9.147': 'ab',
    '172.18.9.148': 'ab',
    '172.18.9.149': 'ab',
    '172.18.9.150': 'ab',
    '172.18.9.151': 'ab',
    '172.18.9.152': 'ab',
    '172.18.9.154': 'ab',
    '172.18.9.155': 'ab',
    '172.18.9.174': 'ab',
    '172.18.9.175': 'ab',
    '172.18.9.176': 'ab',
    '172.18.9.177': 'ab',
    '172.18.9.178': 'ab',
    '172.18.9.189': 'ab',
    '172.18.9.190': 'ab',
    '172.18.9.191': 'ab',
    '172.18.9.192': 'ab',
    '172.18.9.193': 'ab',
    '172.18.9.201': 'ab',
    '172.18.9.202': 'ab',
    '172.18.9.203': 'ab',
    '172.18.9.204': 'ab',
    '172.18.9.205': 'ab',
    '172.18.9.206': 'ab',
    '172.18.9.207': 'ab',
    '172.18.9.214': 'ab',
    '172.18.9.137': 'ab',
    '172.18.9.9': 'Hadoop',
    '172.18.9.10': 'Hadoop',
    '172.18.9.11': 'Hadoop',
    '172.18.9.12': 'Hadoop',
    '172.18.9.13': 'Hadoop',
    '172.18.9.14': 'Hadoop',
    '172.18.9.15': 'Hadoop',
    '172.18.9.16': 'Hadoop',
    '172.18.9.17': 'Hadoop',
    '172.18.9.39': 'Hadoop',
    '172.18.9.40': 'Hadoop',
    '172.18.9.41': 'Hadoop',
    '172.18.9.42': 'Hadoop',
    '172.18.9.43': 'Hadoop',
    '172.18.9.44': 'Hadoop',
    '172.18.9.45': 'Hadoop',
    '172.18.9.46': 'Hadoop',
    '172.18.9.47': 'Hadoop',
    '172.18.9.54': 'Hadoop',
    '172.18.9.55': 'Hadoop',
    '172.18.9.56': 'Hadoop',
    '172.18.9.57': 'Hadoop',
    '172.18.9.58': 'Hadoop',
    '172.18.9.59': 'Hadoop',
    '172.18.9.69': 'Hadoop',
    '172.18.9.70': 'Hadoop',
    '172.18.9.71': 'Hadoop',
    '172.18.9.72': 'Hadoop',
    '172.18.9.73': 'Hadoop',
    '172.18.9.74': 'Hadoop',
    '172.18.9.75': 'Hadoop',
    '172.18.9.76': 'Hadoop',
    '172.18.9.77': 'Hadoop',
    '172.18.9.84': 'Hadoop',
    '172.18.9.85': 'Hadoop',
    '172.18.9.86': 'Hadoop',
    '172.18.9.87': 'Hadoop',
    '172.18.9.88': 'Hadoop',
    '172.18.9.89': 'Hadoop',
    '172.18.9.90': 'Hadoop',
    '172.18.9.91': 'Hadoop',
    '172.18.9.92': 'Hadoop',
    '172.18.9.94': 'Hadoop',
    '172.18.9.95': 'Hadoop',
    '172.18.9.114': 'Hadoop',
    '172.18.9.115': 'Hadoop',
    '172.18.9.116': 'Hadoop',
    '172.18.9.117': 'Hadoop',
    '172.18.9.118': 'Hadoop',
    '172.18.9.119': 'Hadoop',
    '172.18.9.120': 'Hadoop',
    '172.18.9.121': 'Hadoop',
    '172.18.9.122': 'Hadoop',
    '172.18.9.129': 'Hadoop',
    '172.18.9.130': 'Hadoop',
    '172.18.9.131': 'Hadoop',
    '172.18.9.132': 'Hadoop',
    '172.18.9.133': 'Hadoop',
    '172.18.9.134': 'Hadoop',
    '172.18.9.135': 'Hadoop',
    '172.18.9.3': 'RUBiS',
    '172.18.9.4': 'RUBiS',
    '172.18.9.5': 'RUBiS',
    '172.18.9.6': 'RUBiS',
    '172.18.9.7': 'RUBiS',
    '172.18.9.8': 'RUBiS',
    '172.18.9.18': 'RUBiS',
    '172.18.9.19': 'RUBiS',
    '172.18.9.20': 'RUBiS',
    '172.18.9.21': 'RUBiS',
    '172.18.9.22': 'RUBiS',
    '172.18.9.23': 'RUBiS',
    '172.18.9.24': 'RUBiS',
    '172.18.9.25': 'RUBiS',
    '172.18.9.26': 'RUBiS',
    '172.18.9.27': 'RUBiS',
    '172.18.9.28': 'RUBiS',
    '172.18.9.29': 'RUBiS',
    '172.18.9.30': 'RUBiS',
    '172.18.9.31': 'RUBiS',
    '172.18.9.32': 'RUBiS',
    '172.18.9.33': 'RUBiS',
    '172.18.9.34': 'RUBiS',
    '172.18.9.35': 'RUBiS',
    '172.18.9.36': 'RUBiS',
    '172.18.9.37': 'RUBiS',
    '172.18.9.38': 'RUBiS',
    '172.18.9.48': 'RUBiS',
    '172.18.9.49': 'RUBiS',
    '172.18.9.50': 'RUBiS',
    '172.18.9.51': 'RUBiS',
    '172.18.9.52': 'RUBiS',
    '172.18.9.53': 'RUBiS',
    '172.18.9.63': 'RUBiS',
    '172.18.9.64': 'RUBiS',
    '172.18.9.65': 'RUBiS',
    '172.18.9.66': 'RUBiS',
    '172.18.9.67': 'RUBiS',
    '172.18.9.68': 'RUBiS',
    '172.18.9.78': 'RUBiS',
    '172.18.9.79': 'RUBiS',
    '172.18.9.80': 'RUBiS',
    '172.18.9.81': 'RUBiS',
    '172.18.9.82': 'RUBiS',
    '172.18.9.83': 'RUBiS',
    '172.18.9.93': 'RUBiS',
    '172.18.9.108': 'RUBiS',
    '172.18.9.109': 'RUBiS',
    '172.18.9.110': 'RUBiS',
    '172.18.9.111': 'RUBiS',
    '172.18.9.112': 'RUBiS',
    '172.18.9.113': 'RUBiS',
    '172.18.9.123': 'RUBiS',
    '172.18.9.124': 'RUBiS',
    '172.18.9.125': 'RUBiS',
    '172.18.9.126': 'RUBiS',
    '172.18.9.127': 'RUBiS',
    '172.18.9.128': 'RUBiS',
    '172.18.9.138': 'RUBiS',
    '172.18.9.139': 'RUBiS',
    '172.18.9.140': 'RUBiS',
    '172.18.9.141': 'RUBiS',
    '172.18.9.142': 'RUBiS',
    '172.18.9.143': 'RUBiS',
    '172.18.9.153': 'RUBiS',
    '172.18.9.168': 'RUBiS',
    '172.18.9.169': 'RUBiS',
    '172.18.9.170': 'RUBiS',
    '172.18.9.171': 'RUBiS',
    '172.18.9.172': 'RUBiS',
    '172.18.9.173': 'RUBiS',
    '172.18.9.183': 'RUBiS',
    '172.18.9.184': 'RUBiS',
    '172.18.9.185': 'RUBiS',
    '172.18.9.186': 'RUBiS',
    '172.18.9.187': 'RUBiS',
    '172.18.9.188': 'RUBiS',
    '172.18.9.198': 'RUBiS',
    '172.18.9.199': 'RUBiS',
    '172.18.9.200': 'RUBiS'
}

def check_outside_connection(data_file):
    file_handle = open(data_file)
    ip_from = ''
    ip_to = ''
    for line in file_handle:
        '''
        record example:
        00:15:49 tcp 172.18.9.109:80 172.18.9.126:40682
        '''
        lst = line.split(' ')
        ip_from = lst[2].split(':')[0]
        ip_to = lst[3].split(':')[0]
        if ip_from in MAP and ip_to in MAP:
            # print ip_from, ip_to, MAP[ip_from] == MAP[ip_to]
            if MAP[ip_from] != MAP[ip_to]:
                return True
    return False


if __name__ == '__main__':
    '''
    # test split single record
    s = '18:50:31 tcp 172.18.9.136:22 192.168.2.120:50258'
    lst = s.split(' ')
    for item in lst:
        print item
    '''
    for root, dirs, files in os.walk(DATA_PATH):
        flag = False
        for data_file in files:
            if check_outside_connection(join(root, data_file)):
                flag = True
                print "Outer connection exists in file", join(root, data_file)
                break
            else:
                print "Outer connection doesn't exists in file", join(root, data_file)
        if flag:
            print 'Outer connection exists.'
            sys.exit()
    print "Outer connection doesn't exists"