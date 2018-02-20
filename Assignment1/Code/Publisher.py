#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MyPublisher.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import random
import time
import threading


class Publisher:
    def __init__(self, address, port, init_topic):
        self.address = address
        self.port = port
        self.init_topic = init_topic
        self.helper = ZMQHelper()
        self.heartbeat_helper = ZMQHelper()
        self.myID = str(random.randint(1, 1000))

    def register_handler(self):
        if self.register_pub():
            heartbeat_thr = threading.Thread(target = self.heartbeat, args = ())
            threading.Thread.setDaemon(heartbeat_thr, True)
            heartbeat_thr.start()
            return True
        else:
            return False

    # register publisher, connect with broker
    def register_pub(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Connection info: %s' % connect_str)
        self.socket = self.helper.connect_pub2broker(connect_str)
        if self.socket is None:
            print('Connecttion feedback: connected xsub socket failed.')
            return False
        else:
            print('Connecttion feedback: connected xsub socket succeed.')
            init_str = 'pub_init' + '#' + self.myID + '#' + self.init_topic + '#'
            current = time.time()
            while True:
                self.helper.pub_send_msg(self.socket, init_str)
                if time.time() - current > 0.01:
                    break
            print('Connecttion feedback: %s initialized with initial topic %s succeed.' % (self.myID, self.init_topic))
            return True

    # send publication to broker
    def send_pub(self, topic, msg):
        send_str = 'publication' + '#' + self.myID + '#' + topic + '#' + msg
        print('Publication: publishing message %s' % send_str)
        self.helper.pub_send_msg(self.socket, send_str)

    # drop a topic
    def drop_topic(self, topic):
        send_str = 'drop_topic' + '#' + self.myID + '#' + topic + '#'
        self.helper.pub_send_msg(self.socket, send_str)
        print('Drop topic: %s' % topic)

    # send heartbeat
    def heartbeat(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        self.heartbeat_socket = self.heartbeat_helper.connect_pub2broker(connect_str)
        print('Heartbeat connection info: %s' % connect_str)
        if self.heartbeat_socket is None:
            print('Heartbeat connection feedback: heartbeat connected xsub socket failed.')
            return False
        else:
            while True:
                send_str = 'heartbeat' + '#' + self.myID + '#'
                self.heartbeat_helper.pub_send_msg(self.heartbeat_socket, send_str)
                time.sleep(10)

    # publisher fails, disconnect with broker
    def shutoff(self):
        send_str = 'shutoff' + '#' + self.myID + '#'
        print('Shutoff')
        self.helper.pub_send_msg(self.socket, send_str)
