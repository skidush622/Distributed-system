#!/usr/bin/python
# FileName: Subscriber.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from Assignment1.ZMQHelper import ZMQHelper


class Subscriber(ZMQHelper):
    def __init__(self, address, port, topic):
        self.address = address
        self.port = port
        self.topic = topic
        self.helper = ZMQHelper()

    #
    # This method should always be alive
    # handler is used to receive message from broker
    #
    # Message Type:
    # 1. subscription message
    #
    def handler(self):
        while True:
            pass

    # register subscriber
    def register_sub(self):
        connect_str = self.address + '-' + self.port
        print('Subscriber connect to broker at %s' % connect_str)
        self.socket = self.helper.connect_sub2broker(connect_str)

    def un_subscribe(self, topic):
        self.helper.unsubscriber(self.socket, topic)

    # add a subscription topic
    def add_sub_topic(self, socket, topic):
        self.helper.subscribe_topic(socket, topic)

    # drop a subscription topic
    def drop_sub_topic(self, socket, topic):
        self.helper.unsubscriber(socket, topic)

    # request last n subscription items
    # if n is not specified, the default is request all passed publications
    def request_history(self, n):
        pass