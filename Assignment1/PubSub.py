#!/usr/bin/python
# encoding: utf-8
# FileName: PubSub.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#


from Assignment1.Broker.Broker import Broker
from Assignment1.Publisher.Publisher import Publisher
from Assignment1.Subscriber.Subscriber import Subscriber

pubsub_version = '0.1'
pubsub_info = 'Dev Publisher/Subscriber system based on ZeroMQ'


def help():
    print('pubsub: ')
    print('usage: pubsub [opt] [argv]')
    print('usage: pubsub -h')
    print('usage: pubsub -v')

    print('       pub -r [address] -P [port] -t [topic] # register publisher to an address with a port number'
          'and set its initial topic')
    print('       pub send -t [topic] -p [publication] # send publication with topic')
    print('       pub -d [topic] # drop a topic')

    print('       broker -l [xsubsocket port] [xpubsocket port] # listen connections at these two ports')

    print('       sub -r [address] -P [port] -t [topic] -h [history samples count] # register subscriber to an '
          'address with a port number and set its initial topic and history samples count')


pub = None
broker = None
sub = None


def parse(argv):
    opt = argv[0]

    if opt == 'pub':
        if argv[1] == '-r' and argv[3] == '-P' and argv[5] == '-t':
            if pub is None:
                address = argv[2]
                port = argv[4]
                topic = argv[6]
                global pub
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
                publication = argv[5]
                pub.send_pub(topic, publication)
        elif argv[1] == '-d':
            if pub is None:
                print('Please register a publisher firstly.')
                return False
            else:
                topic = argv[2]
                pub.drop_topic(topic)
        else:
            print('Illegal command.')
            return False

    elif opt == 'broker':
        if argv[1] == '-l':
            xsubport = argv[2]
            xpubport = argv[3]
            global broker
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
            global sub
            sub = Subscriber(address, port, topic, count)
            sub.handler()
        else:
            print('Illegal command.')
            return False
    else:
        return False


if __name__ == '__main__':

    while True:
        cmd = input('PubSub>>')
        try:
            cmd = cmd.split()
            opt = cmd[1]

            # help info and version info
            if opt == '-h' or opt == 'help':
                help()
            elif opt == '-v' or opt == 'version':
                print('PubSub current version is: %s' % pubsub_version)
                print('PubSub info is: %s' % pubsub_info)

            ret = parse(opt[1:])
            if ret is False:
                print('Service failed.')
        except IndexError:
            print('Illegal command.')