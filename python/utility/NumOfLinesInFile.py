# This utility is written to validate how long it takes to read lines from a file 
# Prove whats the faster possible way when we have millions of record in a file
'''
Result - >
Python 2.6:

mapcount : 0.471799945831
simplecount : 0.634400033951
bufcount : 0.468800067902
opcount : 0.602999973297
'''
from __future__ import with_statement
import time
import mmap
import random
from collections import defaultdict

def mapcount(filename):
    f = open(filename, "r+")
    buf = mmap.mmap(f.fileno(), 0)
    lines = 0
    readline = buf.readline
    while readline():
        lines += 1
    return lines

def simplecount(filename):
    lines = 0
    for line in open(filename):
        lines += 1
    return lines

def bufcount(filename):
    f = open(filename)                  
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    return lines

def opcount(fname):
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


counts = defaultdict(list)

for i in range(5):
    for func in [mapcount, simplecount, bufcount, opcount]:
        start_time = time.time()
        assert func("big_file.txt") == 1209138
        counts[func].append(time.time() - start_time)

for key, vals in counts.items():
    print key.__name__, ":", sum(vals) / float(len(vals))
    
    
    
