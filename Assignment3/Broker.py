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
from kazoo.client import KazooState
from kazoo.client import KazooClient

class Broker:

    def __init__(self, zk_server, my_address, xsub_port, xpub_port):
        '''
        :param zk_server: IP address of ZooKeeper Server
        :param my_address: IP address of current broker
        :param xsub_port: 5556
        :param xpub_port: 5557
        '''
        self.helper = ZMQHelper()
        '''
           {$(topic): {
                   $(pubID): {
                       'publications' : [$(publication)]
                       'ownership strength': $(ownership_strength)
                   }
               }
           }
        '''
        self.data = {}
        self.xsubsocket, self.xpubsocket = self.helper.prepare_broker(xsub_port, xpub_port)
        self.myID = str(random.randint(1, 1000))
        self.log_file = './Output/' + self.myID + '.log'
        self.zk = KazooClient(zk_server)
        self.isLeader = False
        self.my_address = my_address

        print('\n************************************\n')
        print('Init MyBroker succeed.')
        print('\n************************************\n')
        with open(self.log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')
        self.init_zk()

    def init_zk(self):
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Broker connected to ZooKeeper server.')

        # Create a Znode in ZooKeeper
        znode_path = './Brokers/' + self.myID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is False:
            pass
        print('Broker created znode in ZooKeeper server.')

        # watch publishers znode
        pub_watch_path = './Publishers'

        @self.zk.ChildrenWatch(client=self.zk, path=pub_watch_path)
        def watch_publishers(children):
            self.publisher_failed(children)

        '''
        # watch subscriber znode
        sub_watch_path = './Subscribers'
        
        @self.zk.ChildrenWatch(client=self.zk, path=sub_watch_path)
        def watch_subscribers(children):
            self.subscriber_failed(children)
        '''

        # check if the leader has exists
        leader_path = './Leader'
        if self.zk.exists(leader_path):
            # If the leader znode already exists, specify this broker as follower
            self.isLeader = False
        else:
            # If the leader is not existing, create it
            self.zk.create(leader_path, value=self.my_address, ephemeral=True, makepath=True)
            while self.zk.exists(path=leader_path) is False:
                pass
            print('Broker created the Leader node.')

    def leader_monitor(self):
        # TODO: Run this method in another thread, because if leader election failed, the run() method will be blocked
        election_path = './Brokers/'
        leader_path = './Leader'

        # watch leader znode
        @self.zk.DataWatch(client=self.zk, path=leader_path)
        def watch_leader(data, state):
            if state is None:
                election = self.zk.Election(self.zk, election_path, self.myID)
                election.run(self.win_election)
            else:
                # This broker is follower
                leader_address = data
                # TODO: connect to leader using PULL socket type

                # TODO: listen sync message from leader

                # TODO: update data storage

    def publisher_failed(self, children):
        '''
        :param children: current children list under Publishers Znode
        :return:
        '''
        # TODO: Check which publisher in data storage has failed,
        # if you get one, delete the data related to this publisher
        pass

    def subscriber_failed(self, children):
        '''
        :param children: current children list under Subscribers Znode
        :return:
        '''
        # TODO: Check which subscriber in data storage has failed,
        # if you get one, delete the data related to this subscriber
        pass

    def win_election(self):
        self.isLeader = True
        # TODO: win the election, start receiving msg from publisher

    def receive_msg(self):
        '''
        Message type:
        1. publisher init
        2. publication
        *3. subscriber init
        :return:
        '''
        # TODO: Store received data into self data storage

        # TODO: Send received data to subscribers

        # TODO: Send received data to followers using PUSH socket

    def sync_data(self):
        '''
        Receive sync msg from leader if this broker is a follower
        :return:
        '''
        # TODO: Use a while loop to receive sync msg from leader

    def update_data(self, update_typ, pubID=None, subID=None, topic=None, publication=None):
        '''
        :param update_typ:
                    1. New publisher registered
                    2. Received new publication from publisher
                    3. Publisher failed
                    4. Subscriber failed
        :param pubID:
        :param topic:
        :param publication:
        :return:
        '''
        pass

