#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: BusTopology.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class BusTopology(Topo):
    def build(self, brokernum=9, pubnum=3, subnum=5):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHosts = []
        self.mswitches = []

        print('Topology Architecture: Bus Topology')
        print('Switches #: %d' % 3)
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)
        print('Broker Host #: %d' % brokernum)

        # Add switches for this topology architecture
        for i in range(3):
            switch = self.addSwitch('s{}'.format(i+1))
            print('Add switch %d' % (i+1))
            self.mswitches.append(switch)

            if i > 0:
                self.addLink(self.mswitches[i], self.mswitches[i-1], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
                print('Add link between switches.')


        # Add publisher hosts
        for i in range(pubnum):
            # Add publisher host
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host ' + self.pubHosts[i])

            # Add link between publisher hosts and switches
            self.addLink(self.pubHosts[i], self.mswitches[0], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between publisher host ' + self.pubHosts[i] + ' and ' + 'switch ' + self.mswitches[0])

        # Add subscriber hosts
        for i in range(subnum):
            # Add subscriber host
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            print('Add subscriber host ' + self.subHosts[i])

            # Add link between subscriber hosts and switches
            self.addLink(self.subHosts[i], self.mswitches[2], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between subscriber host ' + self.subHosts[i] + ' and ' + 'switch ' + self.mswitches[2])

        # Add broker host
        for i in range(brokernum):
            # Add broker host
            host = self.addHost('Broker%d' % (i+1))
            self.brokerHosts.append(host)
            print('Add broker host ' + self.brokerHosts[i])

            # Add link between broker hosts and switches
            self.addLink(self.brokerHosts[i], self.mswitches[1], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between broker host ' + self.brokerHosts[i] + ' and ' + 'switch ' + self.mswitches[1])

            # if i > 0:
            #    self.addLink(self.brokerHosts[i], self.brokerHosts[i-1], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)

        # self.addLink(self.brokerHosts[0], self.brokerHosts[brokernum-1], bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
