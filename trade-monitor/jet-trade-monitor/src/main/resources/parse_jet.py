import fileinput
import sys

total=0
count=0
min_delay=float('inf')
max_delay=0

for line in fileinput.input():
    delay=int(line.strip('()\n').split(',')[4])
    min_delay=min(min_delay, delay)
    max_delay=max(max_delay, delay)
    total+=delay
    count+=1

print "avg=%d, min=%d, max=%d" % (total/count, min_delay, max_delay)
