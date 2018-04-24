#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Publisher.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import random
import time
import sys
import threading
from kazoo.client import KazooClient
from kazoo.client import KazooState

class Publisher:
    def __init__(self, zk_server, topic):
        self.topic = topic
        self.helper = ZMQHelper()
        self.myID = str(random.randint(1, 1000))
        self.socket = None
        self.zk = KazooClient(zk_server)
        self.leader_address = None
        self.leader_alive = False 
        self.init_zk()

    def init_zk(self):
        self.zk.start()
        
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Pub %s connected to local ZooKeeper Server.' % self.myID)

        # create a Znode for this publisher
        znode_path = './Publishers/' + self.myID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is None:
            pass
        print('Pub %s created Znode in ZooKeeper server.' % self.myID)

        # register this pub with leader leader it created
        leader_path = './Leader'
        while self.zk.exists(leader_path) is None:
            pass
        data, state = self.zk.get(leader_path)
        self.leader_address = data.decode("utf-8")
        if self.register_pub():
            print('Pub %s connected with leader initially.' % self.myID)
            self.leader_alive = True
        
        # pub watch changes in the leader znode
        @self.zk.DataWatch(client=self.zk, path=leader_path)
        def watch_leader(data, state):
            print('Data in Leader Znode is: %s' % data.decode("utf-8"))
            print('%s changes happened to the Leader' % state.version)
            if state.version > 0:
                self.leader_alive = False
                self.leader_address = data.decode("utf-8")
                self.socket = None
                if self.register_pub():
                    print('pub %s re-connected with new leader', % self.myID)
                    self.leader_alive = True

    # register publisher, connect with leader
    def register_pub(self):
        connect_str = 'tcp://' + self.leader_address + ':5556'
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

    # send publication to broker
    def send_pub(self, topic, msg):
        send_str = 'publication' + '#' + self.myID + '#' + topic + '#' + msg
        print('Publication: publishing message %s' % send_str)
        self.helper.pub_send_msg(self.socket, send_str)

    def main(self, topic, input_file):
        with open(input_file, 'r') as f:
            for line in f:
                while self.leader_alive is False:
                    pass
                self.send_pub(topic, line)
                time.sleep(random.uniform(0.5, 3.0))
