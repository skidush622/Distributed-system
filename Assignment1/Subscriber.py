#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MySubscriber.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import time

class Subscriber:
    def __init__(self, address, port, topic, history_count):
        self.address = address
        self.port = port
        self.topic = topic
        self.history_topic = topic + '-history'
        self.history_count = history_count
        self.helper = ZMQHelper()
        self.register_sub()
        self.add_sub_topic(self.history_topic)
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
        current_time = time.time()
        prev_time = 2e100
        # receive publications
        count = 0
        while True:
            received_pub = self.helper.sub_recieve_msg(self.socket)
            message = received_pub.split()
            received_topic = message[0]
            received_msg = ' '.join(message[1:])
            received_msg = received_msg.split('--')
            time_stamp = float(received_msg[1])
            received_msg = received_msg[0]
            if count == int(self.history_count) or prev_time <= time_stamp:
                self.helper.unsubscribe(self.socket, self.history_topic)
                count += 1e10
            if time_stamp < current_time and count < int(self.history_count) and prev_time > time_stamp:
                count += 1
                print('*************************************************')
                print('Receipt Info:')
                print('History Publication: %s' % received_msg)
                print('Time Interval: %f' % (current_time - time_stamp)) 
                prev_time = time_stamp
            if time_stamp >= current_time:
                current_time = time.time()
                print('*************************************************')
                print('Receipt Info:')
                print('Publication: %s' % received_msg)
                print('Time Interval: %f' % (current_time - time_stamp))
                

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