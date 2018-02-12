#!/usr/bin/python
# FileName: Publisher.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from Assignment1.ZMQHelper import ZMQHelper


class Publisher(ZMQHelper):
    def __init__(self, address, port):
        self.address = address
        self.port = port
        self.helper = ZMQHelper()

    # register publisher, connect with broker
    def register_pub(self):
        connect_str = 'tcp://' + self.address + ':' + self.port
        print('Publisher connect to broker at %s' % connect_str)
        self.socket = self.helper.connect_pub2broker(connect_str)

    # send msg to broker
    def send_pub(self, topic, msg):
        send_str = topic + '-' + msg
        print('Publisher is publishing message %s' % send_str)
        self.helper.pub_send_msg(self.socket, send_str)

    # publisher fails, disconnect with broker
    def shutoff(self):
        pass
