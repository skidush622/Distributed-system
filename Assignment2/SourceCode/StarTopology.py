#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: StarTopology.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class StarTopology(Topo):
    def build(self, brokernum, pubnum, subnum):
        self.brokerHosts = []
        self.pubHosts = []
        self.subHosts = []
        self.switch = None

        print('Topology Architecture: Star Topology')
        print('Switches #: %d' % 1)
        print('Broker Host #: %d' % brokernum)
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)

        # Add a switch
        self.switch = self.addSwitch('s1')
        print('Add a switch.')

        for i in range(brokernum):
            host = self.addHost('Broker%d' % (i+1))
            self.brokerHosts.append(host)
            print('Add broker host' + self.brokerHosts[i])

            self.addLink(self.brokerHosts[i], self.switch, bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between broker host ' + self.brokerHosts[i] + ' and switch ' + self.switch)

        # Add publisher host
        for i in range(pubnum):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host' + self.pubHosts[i])

            # Add link between publisher host and switch
            self.addLink(self.pubHosts[i], self.switch, bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between publisher host ' + self.pubHosts[i] + ' and switch ' + self.switch)

        # Add subscriber host
        for i in range(subnum):
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            print('Add subscriber host' + self.subHosts[i])

            # Add link between subscriber host and switch
            self.addLink(self.subHosts[i], self.switch, bandwidth=10000, max_queue_size=10000, use_htb=True, delay='0ms', loss=0)
            print('Add link between subscriber host ' + self.subHosts[i] + ' and switch ' + self.switch)
