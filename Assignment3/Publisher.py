#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Publisher.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import random

from kazoo.client import KazooClient
from kazoo.client import KazooState


class Publisher:
    znode_path = './Publishers'

    def __init__(self, hosts, address, port, topic):
        '''
        :param hosts: ZooKeeper server hosts
        :param address: specified broker address
        :param port: broker port number used to receive publication
        :param topic: the topic this publisher held
        '''
        self.address = address
        self.port = port
        self.topic = topic
        self.helper = ZMQHelper()
        self.myID = str(random.randint(1, 1000))
        self.socket = None
        self.hosts = hosts
        self.zk = KazooClient(hosts=self.hosts, timeout=20, randomize_hosts=True)

    def register(self):
        '''
        1. Connect to broker using ZeroMQ
        2. Connect to broker Znode in ZooKeeper
        3. Create new publisher Znode in ZooKeeper
        :return:
        '''
        self.zk.add_listener(self.my_listener)
        print('Add listener.')
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Connect to ZK.')
        # create new znode: ./Publishers/pub_$self.myID
        self.zk.create(path=self.znode_path, value=self.myID, ephemeral=True, makepath=True)
        if self.zk.exists(self.znode_path):
            print('Create Znode succeed.')

        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Connection info: %s' % connect_str)
        self.socket = self.helper.connect_pub2broker(connect_str)
        if self.socket is None:
            print('Connection feedback: connected xsub socket failed.')
            return False
        else:
            print('Connection feedback: connected xsub socket succeed.')
            init_str = 'pub_init' + '#' + self.myID + '#' + self.topic + '#'
            self.helper.pub_send_msg(self.socket, init_str)
            print('Connection feedback: %s initialized with initial topic %s succeed.' % (self.myID, self.topic))
            return True

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

    # send publication to broker
    def send_pub(self, topic, msg):
        '''
        :param topic: publication topic
        :param msg: publication content
        :return:
        '''
        send_str = 'publication' + '#' + self.myID + '#' + topic + '#' + msg
        self.helper.pub_send_msg(self.socket, send_str)
        print('Publication: publishing message %s' % send_str)