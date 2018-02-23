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
    def buildStarTopology(self, pubnum, subnum):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHost = None
        self.switch = None

        print('Topology Architecture: Star Topology')
        print('Switches #: %d' % (pubnum + subnum + 1))
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)
        print('Broker Host #: 1 (default)')

        # Add a switch
        self.switch = self.addSwitch('s')
        print('Add a switch.')

        # Add broker host
        self.brokerHost = self.addHost('Broker')
        print('Add Broker host')

        # Add link between switch and broker host
        self.addLink(self.brokerHost, self.switch, delay='1ms')
        print('Add link between switch and broker host')

        # Add publisher host
        for i in range(pubnum):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host' + self.pubHosts[i])

            # Add link between publisher host and switch
            self.addLink(self.pubHosts[i], self.switch, delay='1ms')
            print('Add link between publisher host ' + self.pubHosts[i] + ' and switch ' + self.switch)

        # Add subscriber host
        for i in range(subnum):
            host = self.addHost('SUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add subscriber host' + self.pubHosts[i])

            # Add link between subscriber host and switch
            self.addLink(self.subHosts[i], self.switch, delay='1ms')
            print('Add link between subscriber host ' + self.subHosts[i] + ' and switch ' + self.switch)
