"""
@author: ZhiChen
@date: 11, Dec, 2014

"""

import os
from subprocess import call

PATH = '/home/ubuntu/test/'

if __name__ == '__main__':
    # call(['mkdir', 'test'])
    os.chdir(PATH)
    fileNames = ['test1', 'test2', 'test3']
    for name in fileNames:
        print PATH + name
        with open((PATH + name), 'w') as outfile:
            call(['echo', '123'], stdout=outfile)


    my_cmd = ['cat'] + [(PATH +name) for name in fileNames]
    with open('ALL', "w") as outfile:
        call(my_cmd, stdout=outfile)
