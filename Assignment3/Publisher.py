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
        self.znode_path = './Publishers/' + self.myID
        self.broker_node_path = './Brokers/' + address
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
        self.zk.create(path=self.znode_path, value=self.myID, ephemeral=True, makepath=True)
        if self.zk.exists(self.znode_path):
            print('Create Znode succeed.')
        init_str = 'pub_init' + '#' + self.myID + '#' + self.topic + '#'
        lock = self.zk.Lock(self.broker_node_path, self.myID)
        with lock:
            self.zk.set(self.broker_node_path, value=init_str)

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
        lock = self.zk.Lock(self.broker_node_path, self.myID)
        with lock:
            self.zk.set(path=self.broker_node_path, value=send_str)
        print('Publication: publishing message %s' % send_str)