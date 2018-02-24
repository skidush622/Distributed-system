#!/usr/bin/env /usr/local/bin/python /usr/bin/python
# encoding: utf-8
# FileName: PubSub.py
#
# CS6381 Assignment
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from Broker import Broker
from Subscriber import Subscriber
from Publisher import Publisher
import time

pubsub_version = '0.1'
pubsub_info = 'Dev MyPublisher/MySubscriber system based on ZeroMQ'


def help():
    print('pubsub: ')
    print('usage: pubsub [opt] [argv]')
    print('usage: pubsub -h')
    print('usage: pubsub -v')
    print('****************************')
    print('       pub -r [address] -P [port] -t [topic] # register publisher to an address with a port number'
          'and set its initial topic')
    print('       pub send -t [topic] -p [publication] # send publication with topic')
    print('       pub -d [topic] # drop a topic')
    print('       pub shutoff')
    print('****************************')
    print('       broker -l [xsubsocket port] [xpubsocket port] # listen connections at these two ports')
    print('****************************')
    print('       sub -r [address] -P [port] -t [topic] -h [history samples count] # register subscriber to an '
          'address with a port number and set its initial topic and history samples count')

    print('****************************')
    print('exit # exit program')
pub = None
broker = None
sub = None


def parse(argv):
    opt = argv[0]
    global pub
    global broker
    global sub
    if opt == 'pub':
        if argv[1] == '-r' and argv[3] == '-P' and argv[5] == '-t':
            if pub is None:
                address = argv[2]
                port = argv[4]
                topic = argv[6]
                pub = Publisher(address, port, topic)
                if pub.register_handler():
                    return True
                else:
                    return False
            else:
                print('You already registered a publisher.')
                return False
        elif argv[1] == 'send' and argv[2] == '-t' and argv[4] == '-p':
            if pub is None:
                print('Please register a publisher firstly.')
                return False
            else:
                topic = argv[3]
                publication = ' '.join(argv[5:])
                pub.send_pub(topic, publication)
        elif argv[1] == '-d':
            if pub is None:
                print('Please register a publisher firstly.')
                return False
            else:
                topic = argv[2]
                pub.drop_topic(topic)
        elif argv[1] == 'shutoff':
            if pub is None:
                print('Please register a publisher firstly.')
                return False
            else:
                pub.shutoff()
                return True
        else:
            print('Illegal command.')
            return False

    elif opt == 'broker':
        if argv[1] == '-l':
            xsubport = argv[2]
            xpubport = argv[3]
            broker = Broker(xsubport, xpubport)
            broker.handler()
        else:
            print('Illegal command.')
            return False

    elif opt == 'sub':
        if argv[1] == '-r' and argv[3] == '-P' and argv[5] == '-t' and argv[7] == '-h':
            address = argv[2]
            port = argv[4]
            topic = argv[6]
            count = argv[8]
            sub = Subscriber(address, port, topic, count)
            sub.prepare()
            sub.handler()
        else:
            print('Illegal command.')
            return False
    else:
        print('Illegal command.')
        return False


if __name__ == '__main__':

    while True:
        time.sleep(0.1)
        lcmd = raw_input('PubSub>> ')
        if lcmd == 'exit':
            break
        try:
            lcmd = lcmd.split()
            if lcmd[0] != 'pubsub':
                print('Illegal command.')
                continue
            opt = lcmd[1]

            # help info and version info
            if opt == '-h' or opt == 'help':
                help()
                continue
            elif opt == '-v' or opt == 'version':
                print('PubSub current version is: %s' % pubsub_version)
                print('PubSub info is: %s' % pubsub_info)
                continue
            ret = parse(lcmd[1:])
            if ret is False:
                print('Service failed.')
        except IndexError:
            print('Illegal command.')
