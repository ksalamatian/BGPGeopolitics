#!/usr/bin/python3

import os, sys, getopt
import subprocess

argv=sys.argv[1:]
numShards=8
basePort=6381

try:
    opts, args = getopt.getopt(argv, "n:p:", ["numshards=", "port="])
except getopt.GetoptError:
    print
    'shardedRedis -n <number of shards> -p <port number>'
    sys.exit(2)
for opt, arg in opts:
    if opt in ('-p', "--port"):
        basePort=int(arg)
    elif opt in ("-n", "--numshards"):
        numShards = int(arg)

for i in range(0,numShards):
    portNum=str(basePort+i)
    if not os.path.exists(portNum):
        os.mkdir(portNum)
    os.chdir(portNum)
#    command = "redis-server /data/BGPGeoPo1/redis.conf --port "+portNum
    command = "redis-server /Volumes/DiskSSD/kave/BGPGeoPo/redis.conf --port "+portNum
    p=subprocess.Popen(command, shell=True)
    print(p.returncode)
    os.chdir('..')
print('done')

