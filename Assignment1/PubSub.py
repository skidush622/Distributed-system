#!/usr/bin/python
# encoding: utf-8
# FileName: PubSub.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import sys
import logging

from Assignment1.Broker.Broker import Broker
from Assignment1.Publisher.Publisher import Publisher
from Assignment1.Subscriber.Subscriber import Subscriber

pubsub_version = '0.1'
pubsub_info = 'Dev Publisher/Subscriber system based on ZeroMQ'


def help():
    print('pubsub: ')
    print('usage: pubsub [opt] [argv]')
    print('usage: pubsub -h')

    print('usage: pub -r [address] -P [port] -t [topic] # register publisher to an address with a port number'
          'and set its initial topic')
    print('usage: pub send -t [topic] -p [publication] # send publication with topic')
    print('usage: pub -d [topic] # drop a topic')

    print('usage: broker -i [xsubsocket port] [xpubsocket port]')
    print('usage: broker -l # listening to the connections/message from publishers and subscribers')

    print('usage: sub -r [address] -P [port] -t [topic] -h [history samples count] # register subscriber to an '
          'address with a port number and set its initial topic and history samples count')
    print('usage: sub -add [topic] # add a subscription topic for a subscriber')


def parse(argv):
    pass
