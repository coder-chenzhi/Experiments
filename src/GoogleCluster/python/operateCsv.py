"""
@author: zhichen
@date: 8, Dec, 2014

"""
import csv

INPUT_PATH = 'test.csv'
OUTPUT_PATH = 'out.csv'
inFile = open('test.csv', 'r')
outFile = open('out.csv', 'w')
reader = csv.reader(inFile, delimiter=',', quoting=csv.QUOTE_NONE)

# Attention! Different line terminator between Linux and Windows.
# CSV module use Windows-style line terminators (\r\n) rather than Unix-style (\n)
writer = csv.writer(outFile, delimiter=',', lineterminator='\n', quoting=csv.QUOTE_NONE)
for line in reader:
    print line
    writer.writerow(line)

#csv doesn't need close(), we should close file objects created by open() function
inFile.close()
outFile.close()
