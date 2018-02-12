#!/usr/bin/python
# FileName: Broker.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import zmq
from Assignment1.ZMQHelper import ZMQHelper


class Broker(ZMQHelper):
    # publisher dictionary
    # dictionary format
    # $(publisher_id):{$({topic:[history]})}
    pub_dict = {}

    # publisher ownership dictionary
    # dictionary format
    # publishers are sorted by their ownership
    # $(topic):$([pubs])
    pub_ownership_dict = {}

    # subscriber dictionary
    # dictionary format:
    # $(topic):$([subscribers])
    sub_dict = {}

    def __init__(self, xsub_port, xpub_port):
        # initialize Broker class
        helper = ZMQHelper()
        self.xsubsocket = helper.bind_xsub(xsub_port)
        self.xpubsocket = helper.bind_xpub(xpub_port)

    # This method should always be alive to listen message from pubs & subs
    # Handler serves for either publisher and subscriber
    #
    # Message type for publishers:
    # 1. Registration msg
    # 2. Publication msg
    # 3. Stop publishing service msg
    #
    # #########################################
    #
    # Message type for subscribers:
    # 1. registration msg
    # 2. unsubscribe msg
    # 3. request history message
    #
    # #########################################
    # Message format:
    # $(status)-$(id)-$(msg-type)-$(msg)
    def handler(self):
        while True:
            pass

    # update publisher dictionary
    #
    # arguments: update type, publisher
    # update types:
    # 1. drop a topic
    # 2. add a publisher
    # 3. drop a publisher
    #
    # return boolean
    def update_pub_dict(self, u_type, pub):
        pass

    # update subscriber dictionary
    #
    # arguments: update type, subscriber
    # update types:
    # 1. add a subscriber
    # 2. unsubscribe a topic
    # 3. unsubscribe all topic
    # 4. add a topic for a particular subscriber
    #
    # return boolean
    def update_sub_dict(self, u_type, sub):
        pass

    # filter subscribers dictionary
    # this method is used to filter topic so that broker can know which subscribers subscribe this topic
    #
    # argument: publication topic
    # return subscriber list who subscribed this topic
    #
    def filter_topic(self, topic):
        pass

    # send message to a subscribers
    def send_msg2sub(self, msg):
        pass




