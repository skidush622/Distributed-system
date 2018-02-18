#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MySubscriber.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper


class Subscriber:
    def __init__(self, address, port, topic, history_count):
        self.address = address
        self.port = port
        self.topic = topic
        self.history_count = history_count
        self.history_topic = self.topic + '-' + 'history'
        self.helper = ZMQHelper()
        self.register_sub()
        self.add_sub_topic(self.topic)

    #
    # This method should always be alive
    # handler is used to receive message from broker
    #
    # Message Type:
    # 1. history publication message
    # 2. new publication
    #
    def handler(self):
        # receive history publications
        for count in range(1, int(self.history_count)):
            received_pub = self.helper.sub_recieve_msg(self.socket)
            received_topic, received_msg = received_pub.split()
            print('History publication: %s' % received_msg)

        # receive new publication
        while True:
            received_pub = self.helper.sub_recieve_msg(self.socket)
            received_topic, received_msg = received_pub.split()
            print('Publication: %s' % received_msg)

    # register subscriber
    def register_sub(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Connection info: %s' % connect_str)
        self.socket = self.helper.connect_sub2broker(connect_str)
        if self.socket is None:
            print('Connection feedback: connected xpub socket failed.')
            return False
        else:
            print('Connection feedback: connected xpub socket succeed.')
            return True

    # add a subscription topic
    def add_sub_topic(self, topic):
        self.helper.subscribe_topic(self.socket, topic)