#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: TreeTopology.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class TreeTopology(Topo):
    def build(self, pubnum, subnum):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHost = None
        self.mswitches = []

        print('Topology Architecture: Star Topology')
        print('Switches #: 3')
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)
        print('Broker Host #: 1 (default)')

        # Add a switch
        for i in range(4):
            mswitch = self.addSwitch('s%d' % (i+1))
            self.mswitches.append(mswitch)
            print('Add a switch s%d.' % (i+1))
            if i >= 1:
                self.addLink(self.mswitches[0], self.mswitches[i])


        # Add broker host
        self.brokerHost = self.addHost('Broker')
        print('Add Broker host')

        # Add link between switch 2 and broker host
        self.addLink(self.brokerHost, self.mswitches[1])
        print('Add link between s2 and broker host')

        # Add publisher host
        for i in range(pubnum):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host' + self.pubHosts[i])

            # Add link between publisher host and switch3
            self.addLink(self.pubHosts[i], self.mswitches[2])
            print('Add link between publisher host ' + self.pubHosts[i] + ' and switch ' + self.mswitches[2])

        # Add subscriber host
        for i in range(subnum):
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            print('Add subscriber host' + self.subHosts[i])

            # Add link between subscriber host and switch
            self.addLink(self.subHosts[i], self.mswitches[3])
            print('Add link between subscriber host ' + self.subHosts[i] + ' and switch ' + self.mswitches[3])
