#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: RingTopology.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class RingTopology(Topo):
    def build(self, pubnum, subnum):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHost = None
        self.mswitches = []

        print('Topology Architecture: Star Topology')
        print('Switches #: %d' % (pubnum+subnum))
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)
        print('Broker Host #: 1 (default)')


        # Add switches
        for i in range(pubnum + subnum + 1):
            mswitch = self.addSwitch('s%d' % (i+1))
            self.mswitches.append(mswitch)
            print('Add a switch s%d.' % (i+1))

        # Add broker hosts
        self.brokerHost = self.addHost('Broker')
        print('Add Broker host')

        # Add publisher hosts
        for i in range(pubnum):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host PUB%d' %(i+1))
            if i >= 1:
                self.addLink(self.pubHosts[i-1], self.mswitches[i-1])
                print('Add link between PUB%d and s%d' % (i, i))
                self.addLink(self.pubHosts[i], self.mswitches[i-1])
                print('Add link between PUB%d and s%d' % (i+1, i))

        # Add subscriber hosts
        for i in range(subnum):
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            print('Add subscriber host SUB%d'%(i+1))
            if i >= 1:
                self.addLink(self.subHosts[i-1], self.mswitches[pubnum+i-1])
                print('Add link between SUB%d and s%d' % (i, pubnum+i))
                self.addLink(self.subHosts[i], self.mswitches[pubnum+i-1])
                print('Add link between SUB%d and s%d' % (i+1, pubnum+i))

        self.addLink(self.pubHosts[pubnum-1], self.mswitches[pubnum-1])
        print('Add link between PUB%d and s%d' % (pubnum, pubnum))
        self.addLink(self.subHosts[0], self.mswitches[pubnum-1])
        print('Add link between SUB1 and s%d' % pubnum)

        self.addLink(self.pubHosts[0], self.mswitches[pubnum+subnum-1])
        print('Add link between PUB1 and s%d' % (pubnum+subnum))
        self.addLink(self.brokerHost, self.mswitches[pubnum+subnum-1])
        print('Add link between broker and s%d' % (pubnum+subnum))

        self.addLink(self.brokerHost, self.mswitches[pubnum+subnum])
        print('Add link between broker and s%d' % (pubnum+subnum+1))
        self.addLink(self.subHosts[subnum-1], self.mswitches[pubnum+subnum])
        print('Add link between SUB%d and s%d' % (subnum, pubnum+subnum+1))
