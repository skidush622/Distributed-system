# Make sure you run this file at the first step for every unit tests.

from Broker import Broker

import time
import argparse   # for command line parsing
ZK_SERVER_IP = '127.0.0.1:2183'

def parseCmdLineArgs ():
	# parse the command line
	parser = argparse.ArgumentParser ()
	# add optional arguments
	parser.add_argument("-b", "--brokers", type=str, help='all brokers ip address')
	parser.add_argument("-i", "--ip", type=str, help='self ip address')
	# parse the args
	args = parser.parse_args()
	return args

if __name__ == '__main__':
	args = parseCmdLineArgs()
	brokerIPs = args.brokers
	ip = args.ip
	brokerIPs = brokerIPs.split('-')
	#broker = Broker(zk_server, my_address, '5556', '5557')
	for b_ip in brokerIPs:
		broker = Broker('10.0.0.1', b_ip, '5556', '5557')


    #broker.handler()
	while True:
		pass
