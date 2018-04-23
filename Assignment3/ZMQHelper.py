#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MyZMQHelper.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import zmq


class ZMQHelper:
    # NOTE: Broker ----> Subscriber (PUB ------ bind)
    #       Subscriber -----> Broker (SUB -------- connect)
    #       Subscriber register topic
    #       Subscriber ------> Broker (history publications) (REQ -------- connect)
    #       Broker ------> Subscriber (history publication) (REP ---------- bind)
    # REQ/REP : client.send_string()
    #           client.recv_string()
    #           server.recv_string()
    #           server.send_string()
    def __init__(self):
        pass

    def connect_pub2broker(self, connect_str):
        '''
        Connect publisher to  broker using PUB socket
        :param connect_str:
        :return:
        '''
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(connect_str)
        return socket

    def connect_sub2broker(self, connect_str):
        '''
        Connect subscriber to broker using PULL socket
        :param connect_str:
        :return:
        '''
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(connect_str)
        return socket

    def pub_send_msg(self, socket, message):
        '''
        Publisher send message to broker
        :param socket:
        :param message:
        :return:
        '''
        socket.send_string(message)

    def sub_receive_msg(self, socket):
        '''
        subscriber pull publication from broker
        :param socket:
        :return:
        '''
        return socket.recv_string()

    # bind xsub socket with a specified port
    def bind_xsub(self, port):
        context = zmq.Context()
        xsubsocket = context.socket(zmq.SUB)
        xsubsocket.bind('tcp://*:' + port)
        xsubsocket.setsockopt(zmq.SUBSCRIBE, b'')
        return xsubsocket

    def bind_xpub(self, port):
        context = zmq.Context()
        xpubsocket = context.socket(zmq.PUB)
        xpubsocket.bind('tcp://*:' + port)
        return xpubsocket

    # prepare broker
    def prepare_broker(self, xsub_port, xpub_port):
        xsub = self.bind_xsub(xsub_port)
        xpub = self.bind_xpub(xpub_port)
        return xsub, xpub

    # xpub socket sends message
    def xpub_send_msg(self, socket, string1, string2):
        socket.send_string('%s %s' % (string1, string2))
