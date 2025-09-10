import os
import pdb
import datetime
output = os.popen('ls -lhA ~/Downloads').read().split('\n')
output.pop(0)
for line in output:
    datetime.datetime.strptime("May 29 17:35", "%b %d %H:%M")
    print(line[46:])
pdb.set_trace()