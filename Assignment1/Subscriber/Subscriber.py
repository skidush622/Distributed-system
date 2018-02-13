#!/usr/bin/python
# FileName: Subscriber.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from Assignment1.ZMQHelper import ZMQHelper


class Subscriber(ZMQHelper):
    def __init__(self, address, port, topic, history_count):
        self.address = address
        self.port = port
        self.topic = topic
        self.history_count = history_count
        self.history_topic = self.topic + '-' + 'history'
        self.helper = ZMQHelper()
        self.register_sub()
        self.request_history(topic, history_count)

    #
    # This method should always be alive
    # handler is used to receive message from broker
    #
    # Message Type:
    # 1. history publication message
    # 2. new publication
    #
    def handler(self):
        count = 0
        # receive history publications
        while count < self.history_count:
            received_pub = self.helper.sub_recieve_msg(self.socket)
            received_topic, received_msg = received_pub.split()
            print('Subscriber received history publication: %s' % received_msg)
            count += 1

        self.un_subscribe(self.history_topic)
        self.add_sub_topic(self.socket, self.topic)

        # receive new publication
        while True:
            received_pub = self.helper.sub_recieve_msg(self.socket)
            received_topic, received_msg = received_pub.split()
            print('Subscriber received publication: %s' % received_msg)

    # register subscriber
    def register_sub(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Subscriber connect to broker at %s' % connect_str)
        self.socket = self.helper.connect_sub2broker(connect_str)

    def un_subscribe(self, topic):
        self.helper.unsubscriber(self.socket, topic)

    # add a subscription topic
    def add_sub_topic(self, socket, topic):
        self.helper.subscribe_topic(socket, topic)

    # request last n subscription items
    # if n is not specified, the default is request all passed publications
    def request_history(self, topic, n):
        # subscribe a history topic
        self.add_sub_topic(self.socket, self.history_topic)
        request_str = 'history#' + topic + '#' + n + '#'
        print('Subscriber is requesting history publications...')
        self.helper.sub_request_history(self.socket, request_str)