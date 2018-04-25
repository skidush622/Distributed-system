#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: ZMQHelper.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import zmq


class ZMQHelper:

    # connect publisher with broker
    # argument: publisher connect string
    # return boolean
    def connect_pub2broker(self, connect_str):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(connect_str)
        return socket

    # connect subscriber with broker
    # argument: subscriber connect string
    # return boolean
    def connect_sub2broker(self, connect_str):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(connect_str)
        return socket

    # helper method used for publisher send message to broker
    def pub_send_msg(self, socket, message):
        socket.send_string(message)

    # publisher disconnect with broker
    def disconnect_pub2broker(self, socket, addr):
        socket.disconnect(addr)

    # helper method used for subscriber to subscribe a topic
    def subscribe_topic(self, socket, topic):
        socket.subscribe(topic)

    # helper method used for subscriber to unsubscribe a topic
    def unsubscribe(self, socket, topic):
        socket.unsubscribe(topic)

    # helper method used for subscriber receives message
    def sub_recieve_msg(self, socket):
        return socket.recv_string()

    # bind xsub socket with a specified port
    def bind_xsub(self, port):
        context = zmq.Context()
        xsubsocket = context.socket(zmq.SUB)
        xsubsocket.bind('tcp://*:' + port)
        xsubsocket.setsockopt(zmq.SUBSCRIBE, b'')
        return xsubsocket

    # bind xpub socket with a specified port
    def bind_xpub(self, port):
        context = zmq.Context()
        xpubsocket = context.socket(zmq.PUB)
        xpubsocket.bind('tcp://*:' + port)
        #xpubsocket.setsockopt(zmq.XPUB_VERBOSE, 1)
        return xpubsocket

    # prepare broker
    def prepare_broker(self, xsub_port, xpub_port):
        xsub = self.bind_xsub(xsub_port)
        xpub = self.bind_xpub(xpub_port)
        return xsub, xpub

    # xpub socket sends message
    def xpub_send_msg(self, socket, string1, string2):
        socket.send_string('%s %s' % (string1, string2))

    # client sends msg to server
    def csreq(self, address, port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect('tcp://' + address + ':' + port)
        return socket
    
    # server receives msg from client
    def csrecv(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind("tcp://*" + ':' + port)
        return socket

    # followers pull msg from Leader
    def sinkpull(self, port):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.bind("tcp://*" + ':' + port)
        return socket

    # Leader push msg to followers
    def sourcepush(self, address, port)
        context = zmq.Context()
        socket = context.socket(zmq.PUSH)
        socket.connect('tcp://' + address + ':' + port)
        return socket
