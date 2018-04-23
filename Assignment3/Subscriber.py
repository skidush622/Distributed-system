#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Subscriber.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
from kazoo.client import KazooClient
from kazoo.client import KazooState

import time
import random


class Subscriber:
    def __init__(self, hosts, address, port, topic, history_count):
        # socket type: PUB/SUB
        #              REQ/REP
        '''
        :param hosts: ZooKeeper servers
        :param address: specified broker address
        :param port: broker port number used to receive publication
        :param topic: subscribed topic
        :param history_count: the amount of requested history publication
        '''
        self.connect_address = address + ':' + port
        self.topic = topic
        self.hosts = hosts
        self.history_count = history_count
        self.helper = ZMQHelper()
        self.myID = str(random.randint(1, 1000))
        self.socket = None
        self.znode_path = './Subscribers/' + self.myID
        self.zk = KazooClient(hosts=self.hosts, timeout=20, randomize_hosts=True, read_only=False)

    def register(self):
        '''
        1. Connect to ZooKeeper server
        2. Add listener and watcher
        :return:
        '''
        self.zk.add_listener(self.my_listener)
        print('Add listener.')
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Connected to ZK.')
        self.zk.create(path=self.znode_path, value=self.myID, ephemeral=True, makepath=True)
        while self.zk.exists(self.znode_path) is False:
            pass

        print('Create Znode succeed.')

        connect_str = 'tcp://' + self.connect_address
        print('Connection info: %s' % connect_str)

        self.socket = self.helper.connect_sub2broker(connect_str)
        if self.socket is None:
            print('Connection feedback: connected xpub socket failed.')
            return False
        else:
            print('Connection feedback: connected xpub socket succeed.')
            return True

    def request_history(self):
        # TODO: request history to broker actively
        pass

    def receive_publication(self):
        '''
        Pull publication from broker whenever subscriber receives notification from watcher.
        :return:
        '''
        while self.zk.state == KazooState.CONNECTED:
            received_pub = self.helper.sub_receive_msg(self.socket)
            message = received_pub.split()
            received_topic = message[0]
            if received_topic != self.topic:
                return
            received_msg = ' '.join(message[1:])
            received_msg = received_msg.split('--')
            time_stamp = float(received_msg[1])
            received_msg = received_msg[0]

            current_time = time.time()
            print('*************************************************\n'
                  'Receipt Info:\n'
                  'Publication: %s\n'
                  'Time Interval: %f\n' % (received_msg, abs(current_time - time_stamp)))
            logfile_name = './Output/' + self.myID + '-subscriber.log'
            with open(logfile_name, 'a') as log:
                log.write('*************************************************\n')
                log.write('Receipt Info:\n')
                log.write('Receive: %s\n' % received_msg)
                log.write('Time: %f\n' % abs(current_time - time_stamp))

    def my_listener(self, state):
        '''
        Do something when state changed
        :param state:
        :return:
        '''
        if state == KazooState.LOST:
            print('Connection to ZooKeeper server lost.')
            self.register()
        elif state == KazooState.CONNECTED:
            print('Connected to ZooKeeper server.')