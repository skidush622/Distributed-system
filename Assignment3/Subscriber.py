#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Subscriber.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import time
import random
from kazoo.client import KazooState
from kazoo.client import KazooClient


class Subscriber:
    def __init__(self, zk_server, topic, history_count):
        self.leader_address = None
        self.topic = topic
        self.history_topic = topic + '-history'
        self.history_count = history_count
        self.helper = ZMQHelper()
        self.myID = str(random.randint(1, 100))
        self.zk = KazooClient(hosts=zk_server)
        self.init_zk()
        self.isConnected = False
        self.socket = None

    def init_zk(self):
        self.zk.start()
        if self.zk.state != KazooState.CONNECTED:
            pass
        print('Subscriber connected to ZooKeeper server.')

        znode_path = './Subscribers/' + self.myID
        # Create Znode in ZooKeeper
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is False:
            pass
        print('Subscriber created Znode in ZooKeeper.')
        leader_path = './Leader'

        # watch leader broker znode in ZooKeeper
        @self.zk.DataWatch(client=self.zk, path=leader_path)
        def watch_leader(data, state):
            if state is None:
                self.isConnected = False
            else:
                self.leader_address = data
                self.isConnected = True
                self.request_history(self.history_count)
                self.register_sub()
                self.receive_publication()

    def request_history(self, history_count):
        # TODO: Connected to leader using REQ socket type, using 5558 as port number
        # TODO: Send history request message to leader
        # TODO: receive history publication from leader broker, then store these history publication into log file
        pass

    def receive_publication(self):
        while self.isConnected:
            received_pub = self.helper.sub_recieve_msg(self.socket)
            message = received_pub.split()
            received_msg = ' '.join(message[1:])
            received_msg = received_msg.split('--')
            time_stamp = float(received_msg[1])
            received_msg = received_msg[0]
            current = time.time()
            print('*************************************************\n'
                'Receipt Info:\n'
                'Publication: %s\n'
                'Time Interval: %f\n' % (received_msg, abs(current - time_stamp)))
            logfile_name = './Output/' + self.myID + '-subscriber.log'
            with open(logfile_name, 'a') as log:
                log.write('*************************************************\n')
                log.write('Receipt Info:\n')
                log.write('Receive: %s\n' % received_msg)
                log.write('Time: %f\n' % abs(current - time_stamp))

    # register subscriber
    def register_sub(self):
        connect_str = 'tcp://' + self.leader_address + ':' + '5557'
        print('Connection info: %s' % connect_str)
        current = time.time()
        while time.time() - current < 3:
            self.socket = self.helper.connect_sub2broker(connect_str)
        if self.socket is None:
            print('Connection feedback: connected xpub socket failed.')
            return False
        else:
            print('Connection feedback: connected xpub socket succeed.')
            # Add a topic for SUB socket to filter publication
            self.add_sub_topic(self.topic)
            return True

    # add a subscription topic
    def add_sub_topic(self, topic):
        print('Add topic for subscriber.')
        self.helper.subscribe_topic(self.socket, topic)
