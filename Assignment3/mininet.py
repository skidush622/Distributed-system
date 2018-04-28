#!/usr/bin/env /usr/local/bin/python /usr/bin/python
# encoding: utf-8


import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT
import time
import threading
import random

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


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument("-p", "--publisher", type=int, default=3, help="Number of publishers, default 3")
    parser.add_argument("-s", "--subscriber", type=int, default=5, help="Number of subscriber, default 5")
    parser.add_argument("-T", "--topo", type=int, choices=[1, 2, 3], default=2, help='Topology choice options: 1. Bus Topology 2. Star Topology 3. Tree Topology default: 2. Star Topology')

    # parse the args
    args = parser.parse_args ()

    return args


def subT_helper(brokerIPs, subHosts, zk_host):
    # Invoke subscribers
    for sub in subHosts:
        def sub_op():
            command = 'sudo xterm -hold -e python mSubscriberT.py -a ' + brokerIPs[random.randint(0, len(brokerIPs)-1)] + ' -i ' + sub.IP() + ' -z ' + zk_host.IP()
            sub.cmd(command)
        threading.Thread(target=sub_op, args=()).start()
        time.sleep(1.5*len(subHosts))

def pubT_helper(brokerIPs, pubHosts, zk_host):
    # Invoke publisher
    for pub in pubHosts:
        def pub_op():
            command = 'sudo xterm -hold -e python mPublisherT.py' + ' -a '+ brokerIPs[random.randint(0, len(brokerIPs)-1)] + ' -i ' + pub.IP() + ' -z ' + zk_host.IP()
            pub.cmd(command)
        threading.Thread(target=pub_op, args=()).start()
        time.sleep(1.5*len(pubHosts))

def runTestCase(pubHosts, subHosts, brokerHosts, zk_host):
    try:
        def zk_op():
            start_command = 'sudo xterm -hold -e ./zookeeper-3.4.10/bin/zkServer.sh start'
            zk_host.cmd(start_command)
        threading.Thread(target=zk_op, args=()).start()
        time.sleep(3)

        brokerIPs = []
        for broker in brokerHosts:
            brokerIPs.append(broker.IP())

        command = 'sudo xterm -hold -e python BrokerT.py -b '
        for index, broker in enumerate(brokerIPs):
            if index != len(brokerIPs)-1:
                command += broker + '-'
            else:
                command += broker

        command += ' -z ' + zk_host.IP()

        for broker in brokerHosts:
            def broker_op():
                # Invoke broker
                broker.cmd('%s -i %s' % (command, broker.IP()))
            threading.Thread(target=broker_op, args=()).start()
            time.sleep(1.5*len(brokerHosts))

            print('Waiting for Broker ready...')

        time.sleep(6)
        pubT_helper(brokerIPs, pubHosts, zk_host)
        time.sleep(6)
        subT_helper(brokerIPs, subHosts, zk_host)

    except Exception as e:
        print(e)

def mainHelper(topo):
    # create the network
    print('Instantiate network')
    net = Mininet(topo=topo, link=TCLink)

    # activate the network
    print('Activate network')
    net.start()

    # debugging purposes
    print('Dumping host connections')
    dumpNodeConnections (net.hosts)

    # debugging purposes
    print('Testing network connectivity')
    net.pingAll()


    pubhosts =[]
    subhosts = []
    brokerhosts = []
    for host in net.hosts:
        if 'PUB' in host.name:
            pubhosts.append(host)
        elif 'SUB' in host.name:
            subhosts.append(host)
        elif 'Broker' in host.name:
            brokerhosts.append(host)
        else:
            zk_host = host

    runTestCase(pubhosts, subhosts, brokerhosts, zk_host)

    # net.stop()


#####################
# main program
######################
def main():
    print('------------Pub/Sub with 0MQ & Mininet------------')
    print('Topology choice options:')
    print('1. Bus Topology')
    print('2. Star Topology')
    print('3. Tree Topology')
    print('default: 1. Bus Topology\n')
    print('---------------------------------------------------')

    args = parseCmdLineArgs()
    pub_num = args.publisher
    sub_num = args.subscriber
    topo_choice = args.topo

    # Bus Topology
    if topo_choice == 1:
        # instantiate our topology
        print('Instantiate topology')
        topo = BusTopology(brokernum=3, pubnum=pub_num, subnum=sub_num)
        mainHelper(topo)

    # Star Topology
    elif topo_choice == 2:
        # instantiate our topology
        print('Instantiate topology')
        topo = StarTopology(brokernum=3, pubnum=pub_num, subnum=sub_num)
        mainHelper(topo)

    # Tree Topology
    elif topo_choice == 3:
        # instantiate our topology
        print('Instantiate topology')
        topo = TreeTopology(brokernum=3, pubnum=pub_num, subnum=sub_num)
        mainHelper(topo)

if __name__ == '__main__':
    # STOP ZK
    os.system('sudo ./zookeeper-3.4.10/bin/zkServer.sh stop')
    main()
