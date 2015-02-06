__author__ = 'ubuntu'

import os
import time
import threading


"""
IP_LIST = ["172.18.9.44", "172.18.9.18", "172.18.9.19", "172.18.9.20", "172.18.9.21", "172.18.9.22",
      "172.18.9.23", "172.18.9.41", "172.18.9.42", "172.18.9.66", "172.18.9.67", "172.18.9.68",
      "172.18.9.69", "172.18.9.70", "172.18.9.71", "172.18.9.72" ]
"""
IP_LIST = ["192.168.61.168"]

def get_current_time():
    localtime = time.localtime()
    time_string = time.strftime("%H:%M:%S", localtime)
    return time_string

def monitor(ip, cur_time):
    cmd = 'sshpass -p "ubuntu" ssh ubuntu@' + ip + ' "sar -r -u -d 1 5 > ~/' + ip + '-'+ cur_time + 'log"'
    print cmd
    os.system(cmd)

if __name__ == "__main__":
    thread_pool = []
    cur_time = get_current_time()
    for ip in IP_LIST:
        t = threading.Thread(target=monitor, name=ip, args=(ip, cur_time,))
        thread_pool.append(t)
    for t in thread_pool:
        t.start()
    for t in thread_pool:
        t.join()
