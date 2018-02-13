#!/usr/bin/python
# FileName: Publisher.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from Assignment1.ZMQHelper import ZMQHelper
import random


class Publisher(ZMQHelper):
    def __init__(self, address, port, init_topic):
        self.address = address
        self.port = port
        self.init_topic = init_topic
        self.helper = ZMQHelper()
        self.myID = random.randint(1, 100)

    # register publisher, connect with broker
    def register_pub(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Publisher connect to broker at %s' % connect_str)
        self.socket = self.helper.connect_pub2broker(connect_str)
        if self.socket is None:
            print('Publisher connect broker failed.')
        else:
            init_str = 'pub_init' + '#' + self.myID + '#' + self.init_topic
            self.helper.pub_send_msg(self.socket, init_str)

    # send publication to broker
    def send_pub(self, topic, msg):
        send_str = 'publication' + '#' + self.myID + '#' + topic + '#' + msg
        print('Publisher is publishing message %s' % send_str)
        self.helper.pub_send_msg(self.socket, send_str)

    # drop a topic
    def drop_topic(self, topic):
        send_str = 'drop_topic' + '#' + self.myID + '#' + topic + '#'
        self.helper.pub_send_msg(self.socket, send_str)

    # publisher fails, disconnect with broker
    def shutoff(self):
        send_str = 'shutoff' + '#' + self.myID + '#'
        self.helper.pub_send_msg(self.socket, send_str)
