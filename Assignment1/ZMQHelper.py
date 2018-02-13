#!/usr/bin/python
# FileName: ZMQHelper.py
#
# CS6381 Assignment1
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

    # helper method used for subscriber receives message
    def sub_recieve_msg(self, socket):
        return socket.recv_string()

    # subscriber send message to xpubsocket to request history
    def sub_request_history(self, socket, msg):
        socket.send_string(msg)
        
    # helper method used for un-subscribing
    def unsubscriber(self, socket, topic):
        socket.unsubscribe(topic)

    # bind xsub socket with a specified port
    def bind_xsub(self, port):
        context = zmq.Context()
        xsubsocket = context.socket(zmq.XSUB)
        xsubsocket.bind('tcp://*:' + port)
        xsubsocket.setsockopt(zmq.SUBSCRIBE, b'')
        return xsubsocket

    # bind xpub socket with a specified port
    def bind_xpub(self, port):
        context = zmq.Context()
        xpubsocket = context.socket(zmq.XPUB)
        xpubsocket.bind('tcp://*:' + port)
        xpubsocket.setsockopt(zmq.XPUB_VERBOSE, 1)
        return xpubsocket