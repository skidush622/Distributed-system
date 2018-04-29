# Make sure you run this file at the first step for every unit tests.

from Broker import Broker

import time
import argparse   # for command line parsing

def parseCmdLineArgs ():
	# parse the command line
	parser = argparse.ArgumentParser ()
	# add optional arguments
	parser.add_argument("-i", "--ip", type=str, help='self ip address')
	parser.add_argument('-z', '--zk', type=str, help='ZK address')
	# parse the args
	args = parser.parse_args()
	return args

if __name__ == '__main__':
	args = parseCmdLineArgs()
	ip = args.ip
	zk_address = args.zk
	print('ZooKeeper Address: ' + zk_address)
	broker = Broker(zk_address, ip, '5556', '5557')

    #broker.handler()
	while True:
		pass
