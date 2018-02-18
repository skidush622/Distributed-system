#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MyBroker.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import random
import threading
import time
from ZMQHelper import ZMQHelper


class Broker:
    
    def __init__(self, xsub_port, xpub_port):
        # initialize MyBroker class
        self.helper = ZMQHelper()
        self.heartbeat_lock = threading.Lock()
        # publisher dictionary
        # dictionary format
        # $(pubID):{$({$topic:[$history]})}
        self.pub_dict = {}

        # publisher ownership dictionary
        # dictionary format
        # $(topic):$({$pubID: $ownership_strength})
        self.pub_ownership_dict = {}

        # publisher heartbeat dictionary
        # This dict is used to record publisher's latest heartbeat timestamp
        # $(pubID):$(time)
        self.heartbeat_dict = {}

        self.xsubsocket, self.xpubsocket = self.helper.prepare_broker(xsub_port, xpub_port)

        print('\n************************************\n')
        print('Init MyBroker succeed.')
        print('\n************************************\n')


    # This handler is used to check if any publisher has dead.
    def vice_handler(self):
        while True:
            self.heartbeat_lock.acquire()
            # check if any publisher has failed
            for pubID in self.heartbeat_dict.keys():
                if time.time() - self.heartbeat_dict[pubID] > 60:
                    print('\n************************************\n')
                    print('Publisher dead: %s has dead.' % pubID)
                    print('\n************************************\n')
                    self.update_pub_dict('shutoff', pubID, '', '')
                    self.update_pub_ownership_dict('shutoff', '', pubID)
                    del self.heartbeat_dict[pubID]
                    break
            self.heartbeat_lock.release()

    # This method should always be alive to listen message from pubs & subs
    # Handler serves for either publisher and subscriber
    #
    # Message type for publishers:
    # 1. Registration msg
    # 2. Publication msg
    # 3. Drop a topic
    # 4. Stop publishing service msg
    #
    # #########################################
    #
    # Message type for subscribers:
    # 1. request history message
    #
    def handler(self):
        thr = threading.Thread(target= self.vice_handler, args = ())
        threading.Thread.setDaemon(thr, True)
        thr.start()

        while True:
            # receive message from publisher
            msg = self.xsubsocket.recv_string(0, 'utf-8')
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'pub_init' and message[1] in self.pub_dict.keys():
                continue
            print('Publication storage info: (pubID, {topic : [history]})')
            [print(x) for x in self.pub_dict.items()]
            print('Publisher ownership info: (topic, {pubID : ownership_strength})')
            [print(x) for x in self.pub_ownership_dict.items()]
            if msg_type == 'pub_init':
                pubID = message[1]
                topic = message[2]
                print('\n************************************\n')
                print('Init msg: %s init with topic %s' % (pubID, topic))
                print('\n************************************\n')
                # publisher registration
                self.update_pub_dict('add_pub', pubID, topic, '')
                self.update_pub_ownership_dict('add_pub', topic, pubID)
                for item in self.pub_dict.items():
                    for sample in item[1].items():
                        for publication in sample[1]:
                            self.helper.xpub_send_msg(self.xpubsocket, sample[0], publication)
                
            elif msg_type == 'publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                print('\n************************************\n')
                print('Publication: %s published %s with topic %s' % (pubID, publication, topic))
                print('\n************************************\n')
                # update storage
                self.update_pub_dict('add_publication', pubID, topic, publication)

                # update publisher ownership_strength for a topic
                self.update_pub_ownership_dict('add_pub', topic, pubID)

                # filter publisher via ownership strength
                if self.filter_pub_ownership_dict(pubID, topic) is None:
                    print('     Broker filter feedback: %s doesn\'t own highest ownership strength, %s won\'t be forwarded.'
                          % (pubID, topic))
                    continue
                else:
                    # send publication to subscribers using xpubsocket
                    self.helper.xpub_send_msg(self.xpubsocket, topic, publication)

            elif msg_type == 'drop_topic':
                target_pub = message[1]
                target_topic = message[2]
                print('\n************************************\n')
                print('Drop topic: %s droped topic %s' % (target_pub, target_topic))
                print('\n************************************\n')
                self.update_pub_dict('drop_topic', target_pub, target_topic, '')
                self.update_pub_ownership_dict('drop_topic', target_topic, target_pub)

            # This shutoff is a soft shutoff, which means publisher would tell broker in advance
            elif msg_type == 'shutoff':
                target_pub = message[1]
                # update publisher dictionary
                self.update_pub_dict('shutoff', target_pub, '', '')
                # update publisher ownership dictionary
                self.update_pub_ownership_dict('shutoff', '', target_pub)
                print('\n************************************\n')
                print('Soft shutoff: %s soft shutoff' % target_pub)
                print('\n************************************\n')

            elif msg_type == 'heartbeat':
                pubID = message[1]
                print('\n************************************\n')
                self.heartbeat_lock.acquire()
                if pubID not in self.heartbeat_dict:
                    self.heartbeat_dict.update({pubID: time.time()})
                else:
                    self.heartbeat_dict[pubID] = time.time()
                self.heartbeat_lock.release()
                print('Heartbeat: %s heartbeat' % pubID)
                print('\n************************************\n')

    # update publisher dictionary
    #
    # arguments: update type, publisher
    # update types:
    # 1. drop a topic
    # 2. add a publisher
    # 3. add publication
    # 4. shutoff a publisher
    #
    def update_pub_dict(self, update_typ, pubID, topic, publication):
        if update_typ == 'add_pub':
            self.pub_dict.update({pubID: {topic:[]}})

        elif update_typ == 'add_publication':
            if topic not in self.pub_dict[pubID].keys():
                self.pub_dict[pubID].update({topic: [publication]})
            else:
                self.pub_dict[pubID][topic].append(publication)

        elif update_typ == 'drop_topic':
            if topic not in self.pub_dict[pubID].keys():
                print('     Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
            else:
                del self.pub_dict[pubID][topic]

        elif update_typ == 'shutoff':
            del self.pub_dict[pubID]

    # update publisher ownership strength dictionary
    #
    # arguments: update type, publisher
    # update types:
    # 1. add a publisher
    # 2. drop a topic
    # 3. shutoff a publisher
    #
    def update_pub_ownership_dict(self, update_type, topic, pubID):
        if update_type == 'add_pub':
            if topic not in self.pub_ownership_dict.keys():
                self.pub_ownership_dict.update({topic: {pubID: random.randint(1, 1000)}})
            else:
                if pubID not in self.pub_ownership_dict[topic].keys():
                    self.pub_ownership_dict[topic].update({pubID: random.randint(1, 1000)})
            
        elif update_type == 'drop_topic':
            if topic not in self.pub_ownership_dict.keys():
                print('     Broker filter feedback: drop topic %s for publisher %s failed, '
                      '%s not in publisher ownership dictionary.' % (topic, pubID, topic))
            else:
                if pubID not in self.pub_ownership_dict[topic].keys():
                    print('     Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
                else:
                    del self.pub_ownership_dict[topic][pubID]
                    
        elif update_type == 'shutoff':
            for key in self.pub_ownership_dict.keys():
                if pubID in self.pub_ownership_dict[key].keys():
                    del self.pub_ownership_dict[key][pubID]

    # filter publisher ownership strength dictionary
    #
    # argument: current publisher & topic
    # return publisher or None
    #
    def filter_pub_ownership_dict(self, pubID, topic):
        if self.pub_ownership_dict[topic][pubID] == max(self.pub_ownership_dict[topic].values()):
            return pubID
        else:
            return None
