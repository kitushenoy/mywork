#!/usr/bin/python

import sys

activekey=''
dict={}

def processchunk(dict):

    avgT = 0
    maxT = 0
    minT = 0
    medianT =0
    dict2 = {}
    cal = []
    for dd in dict:
	mn = min(dict[dd])
	medianT = (sorted(dict[dd])[int(round((len(dict[dd]) - 1) / 2.0))] + sorted(dict[dd])[int(round((len(dict[dd]) - 1) // 2.0))]) / 2.0
	avgT = reduce(lambda x, y: x + y, dict[dd]) / len(dict[dd])
	minT = str(min(dict[dd]))
	maxT = str(max(dict[dd]))
	ll = dd.split(",")
	ll.append(str(minT))
	ll.append(str(maxT))
	#ll+=str(maxT)
	ll.append(str(avgT))
	ll.append(str(medianT))
	dict2[dd]= ll
    return dict2
	 
def processrow(line):
	data=line.split("\t")
	keydata=data[:-1]
	key=",".join(keydata)
	if key in dict:
		dict[key].append(int(data[-1]))
	else:
		dict[key] = [int(data[-1])] 

dict = {}
import logging

for line in sys.stdin:
    
    line = line.rstrip()
    processrow(line)
dict2 = processchunk(dict)
for jj in dict2:

	sys.stderr.write("\n")
	sys.stderr.write("\t".join(dict2[jj]))
	print("\t".join(dict2[jj]))
