__author__ = 'Chenzhi'

import os
import re
from os.path import join
runtime = {}    # A map store runtime for each client
root = "E:\KuaiPan\Lab\RUBiS\Experiments\\1 Group + No Delay + Round Robin"

print root
for root, dirs, files in os.walk(root):
    for fileName in files:
        if fileName.startswith('stat_client'):
            m = re.search('\d+', fileName)
            client = int(m.group(0))
            f = open(join(root, fileName))
            occur = 0
            time = 0
            for line in f:
                #print line
                if 'Total' in line:
                    occur += 1
                    #print occur
                if occur == 8:
                    #print line
                    m = re.findall(r'\d+', line)
                    #print m[len(m) - 1]
                    time = int(m[len(m) - 1])
                    break

            if client in runtime:
                runtime[client].append(time)
            else:
                times = [time]
                runtime[client] = times
total = 0
for key in runtime:
    total = total + sum(runtime[key]) / len(runtime[key])
print total / len(runtime.keys())