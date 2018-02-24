#!/usr/bin/env /usr/local/bin/python /usr/bin/python
# encoding: utf-8

import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT
import time
import threading

import subprocess

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor

from BusTopology import BusTopology
from StarTopology import StarTopology
from TreeTopology import TreeTopology
from RingTopology import RingTopology

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-p", "--publisher", type=int, default=3, help="Number of publishers, default 3")
    parser.add_argument ("-s", "--subscriber", type=int, default=5, help="Number of subscriber, default 5")
    parser.add_argument ("-T", "--topo", type=int, choices=[1, 2, 3], default=1, help='Topology choice options: 1. Bus Topology 2. Star Topology 3. Tree Topology default: 1. Bus Topology')
    parser.add_argument ("-t", "--test", type=int, choices=[1, 2, 3, 4, 5], default=1, help="Test case choice, default 1")

    # parse the args
    args = parser.parse_args ()

    return args

def subT_helper(broker_ip, subHosts):
     # Invoke subscribers
    for sub in subHosts:
        def sub_op():
            command = 'xterm -e python mSubscriberT.py -a ' + broker_ip
            sub.cmd(command)
        threading.Thread(target=sub_op, args=()).start()
        time.sleep(len(subHosts))

def pubT_helper(broker_ip, test, pubHosts):
    # Invoke publisher
    for pub in pubHosts:
        def pub_op():
            command = 'xterm -e python mPublisherT.py -t ' + str(test) + ' -a '+ broker_ip
            pub.cmd(command)
        threading.Thread(target=pub_op, args=()).start()
        time.sleep(len(pubHosts))

def runTestCase(pubHosts, subHosts, brokerHost, test_choice):
    try:
        broker_ip = brokerHost.IP()

        def broker_op():
            # Invoke broker
            command = 'xterm -e python BrokerT.py'
            brokerHost.cmd(command)

        threading.Thread(target=broker_op, args=()).start()

        print('Waiting for Broker ready...')
        time.sleep(5)

        pubT_helper(broker_ip, test_choice, pubHosts)
        subT_helper(broker_ip, subHosts)

        while True:
            pass
    except Exception as e:
        print(e)

def mainHelper(topo, test):
    # create the network
    print('Instantiate network')
    net = Mininet(topo=topo, link=TCLink)

    # activate the network
    print('Activate network')
    net.start ()

    # debugging purposes
    print ('Dumping host connections')
    dumpNodeConnections (net.hosts)

    # debugging purposes
    print ('Testing network connectivity')
    net.pingAll ()


    pubhosts =[]
    subhosts = []
    brokerhost = None
    for host in net.hosts:
        if 'PUB' in host.name:
            pubhosts.append(host)
        elif 'SUB' in host.name:
            subhosts.append(host)
        elif 'Broker' in host.name:
            brokerhost = host

    runTestCase(pubhosts, subhosts, brokerhost, test)

    net.stop()


#####################
# main program
######################
def main():
    print ('------------Pub/Sub with 0MQ & Mininet------------')
    print ('Topology choice options:')
    print ('1. Bus Topology')
    print ('2. Star Topology')
    print ('3. Tree Topology')
    print ('default: 1. Bus Topology\n')

    print ('Test cases options:')
    print ('1. Multiple publishers & subscribers work concurrently')
    print ('2. Subscribers receive history publications')
    print ('3. Publisher failing')
    print ('4. Ownership strength feature')
    print ('5. Publisher drop topics')
    print ('default: Test1')

    print('---------------------------------------------------')

    args = parseCmdLineArgs()

    pub_num = args.publisher
    sub_num = args.subscriber
    topo_choice = args.topo
    test = args.test

    # Bus Topology
    if topo_choice == 1:
        # instantiate our topology
        print('Instantiate topology')
        topo = BusTopology(pubnum=pub_num, subnum=sub_num)
        #print(topo)
        mainHelper(topo, test)

    # Star Topology
    elif topo_choice == 2:
        # instantiate our topology
        print('Instantiate topology')
        topo = StarTopology(pubnum=pub_num, subnum=sub_num)
        mainHelper(topo, test)

    # Tree Topology
    elif topo_choice == 3:
        # instantiate our topology
        print('Instantiate topology')
        topo = TreeTopology(pubnum=pub_num, subnum=sub_num)
        mainHelper(topo, test)

    '''
    # Ring Topology
    elif topo_choice == 4:
        # instantiate our topology
        print('Instantiate topology')
        topo = RingTopology(pubnum=pub_num, subnum=sub_num)
        mainHelper(topo, test)
    '''

if __name__ == '__main__':
    main()
    #print(os.popen('sudo mn -c', 'r').read())
