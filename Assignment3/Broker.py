#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Broker.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import random
import threading
import time

from ZMQHelper import ZMQHelper
from kazoo.client import KazooClient
from kazoo.client import KazooState

log_file = './Output/broker.log'


class Broker:
    pub_watch_path = './Publishers'
    sub_watch_path = './Subscribers'

    def __init__(self, hosts, my_address, xsub_port, xpub_port):
        self.helper = ZMQHelper()
        '''
        {$(topic): {
                'publishers': {
                    $(pubID): {
                        'publications' : [$(publication)]
                        'ownership strength': $(ownership_strength)
                    }
                },
                'subscribers': [$(subID)]
            }
        }
        '''
        self.data = {}
        self.xsubsocket, self.xpubsocket = self.helper.prepare_broker(xsub_port, xpub_port)
        self.myID = str(random.randint(1, 1000))
        self.znode_path = './Brokers/' + my_address
        print('\n************************************\n')
        print('Init Broker succeed.')
        print('\n************************************\n')
        with open(log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')
        self.isLeader = False
        self.zk = KazooClient(hosts=hosts, randomize_hosts=True)
        self.init_zk()

    def init_zk(self):
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        # Create broker znode
        self.zk.create(path=self.znode_path, ephemeral=True, sequence=True)
        while self.zk.exists(self.znode_path) is False:
            pass
        print('Create Znode in ZooKeeper.')
        self.zk.DataWatch(self.zk, self.znode_path, self.broker_watcher)
        # create publishers znode
        self.zk.create(path=self.pub_watch_path, ephemeral=False, makepath=True)
        while self.zk.exists(self.pub_watch_path) is False:
            pass
        print('Create path for publishers.')
        self.zk.ChildrenWatch(self.zk, self.pub_watch_path, self.publisher_watcher)

        # Create subscribers znode
        self.zk.create(path=self.sub_watch_path, ephemeral=False, makepath=True)
        while self.zk.exists(self.sub_watch_path) is False:
            pass
        self.zk.ChildrenWatch(self.zk, self.sub_watch_path, self.subscriber_watcher)
        print('Create path for subscribers.')

        # TODO: 新建线程
        # Start leader election
        election = self.zk.Election(self.zk, './Brokers/', identifier=self.myID)
        election.run(self.leader_function)

    def leader_function(self):
        self.isLeader = True
        print('This Broker is leader...')

    def broker_watcher(self, data):
        '''
        :param data: 1. publisher init
                     2. publishers publish publications
        :return:
        '''
        pass

    def publisher_watcher(self, children):
        # TODO: watch if publisher registered or failed
        pass

    def subscriber_watcher(self, children):
        # TODO: watch if subscriber registered or failed
        pass

    def publisher_handler(self):
        pass

    def subscriber_handler(self):
        pass

    # utility method
    def update_data(self):
        pass