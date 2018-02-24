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
    def build(self, pubnum=3, subnum=5):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHost = None
        self.mswitches = []

        print('Topology Architecture: Bus Topology')
        print('Switches #: %d' % (pubnum + subnum + 1))
        print('Publisher Host #: %d' % pubnum)
        print('Subscriber Host #: %d' % subnum)
        print('Broker Host #: 1 (default)')

        # Add switches for this topology architecture
        for i in range(pubnum + subnum + 1):
            switch = self.addSwitch('s{}'.format(i+1))
            self.mswitches.append(switch)

            # Add links for these Switches
            if i > 0:
                self.addLink (self.mswitches[i-1], self.mswitches[i])
                print('Add link between switches ' + self.mswitches[i-1] + ' and ' + self.mswitches[i])

        # Add publisher hosts
        for i in range(pubnum):
            # Add publisher host
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            print('Add publisher host ' + self.pubHosts[i])

            # Add link between publisher hosts and switches
            self.addLink(self.pubHosts[i], self.mswitches[i])
            print('Add link between publisher host ' + self.pubHosts[i] + ' and ' + 'switch ' + self.mswitches[i])

        # Add subscriber hosts
        for i in range(subnum):
            # Add subscriber host
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            print('Add subscriber host ' + self.subHosts[i])

            # Add link between subscriber hosts and switches
            self.addLink(self.subHosts[i], self.mswitches[i+pubnum])
            print('Add link between subscriber host ' + self.subHosts[i] + ' and ' + 'switch ' + self.mswitches[i+pubnum])

        # Add broker host
        self.brokerHost = self.addHost('Broker')
        print('Add Broker host' + self.brokerHost)

        # Add link between broker host and switch
        self.addLink(self.brokerHost, self.mswitches[pubnum+subnum], delay='1ms')
        print('Add link between broker and switch' + self.mswitches[pubnum+subnum])
