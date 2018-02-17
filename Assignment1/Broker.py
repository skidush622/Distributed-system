#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: MyBroker.py
#
# CS6381 Assignment1
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import random
from time import time
from ZMQHelper import ZMQHelper


class Broker:
    
    def __init__(self, xsub_port, xpub_port):
        # initialize MyBroker class
        self.helper = ZMQHelper()

        # publisher dictionary
        # dictionary format
        # $(pubID):{$({$topic:[$history]})}
        self.pub_dict = {}

        # publisher ownership dictionary
        # dictionary format
        # publishers are sorted by their ownership
        # $(topic):$({$pubID: $ownership_strength})
        self.pub_ownership_dict = {}

        # publisher heartbeat dictionary
        # This dict is used to record publisher's latest heartbeat timestamp
        # $(pubID):$(time)
        self.heartbeat_dict = {}

        self.xsubsocket, self.xpubsocket = self.helper.prepare_broker(xsub_port, xpub_port)

        print('Init MyBroker succeed.')

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
        while True:
            # check if any publisher has failed
            for pubID in self.heartbeat_dict.keys():
                if time() - self.heartbeat_dict[pubID] > 60:
                    print('MyPublisher %s has dead.' % pubID)
                    del self.heartbeat_dict[pubID]
                    self.update_pub_dict('shutoff', pubID, '', '')
                    self.update_pub_ownership_dict('shutoff', '', pubID)

            # receive message from publisher
            msg = self.xsubsocket.recv_string(0, 'utf-8')
            print('Broker received message %s on xsubsocket.' % msg)
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'pub_init':
                pubID = message[1]
                topic = message[2]
                # publisher registration
                self.update_pub_dict('add_pub', pubID, topic, '')
                self.update_pub_ownership_dict('add_pub', topic, pubID)

            elif msg_type == 'publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                # update storage
                self.update_pub_dict('add_pub', pubID, topic, publication)

                # update publisher ownership_strength for a topic
                self.update_pub_ownership_dict('add_pub', topic, pubID)

                # filter publisher via ownership strength
                if self.filter_pub_ownership_dict(pubID, topic) is None:
                    print('MyPublisher %s doesn\'t own highest ownership strength, %s won\'t be forwarded.'
                          % (pubID, topic))
                    continue
                else:
                    # send publication to subscribers using xpubsocket
                    self.helper.xpub_send_msg(self.xpubsocket, topic, publication)

            elif msg_type == 'drop_topic':
                target_pub = message[1]
                target_topic = message[2]
                self.update_pub_dict('drop_topic', target_pub, target_topic, '')
                self.update_pub_ownership_dict('drop_topic', target_topic, target_pub)

            # This shutoff is a soft shutoff, which means publisher would tell broker in advance
            elif msg_type == 'shutoff':
                target_pub = message[1]
                # update publisher dictionary
                self.update_pub_dict('shutoff', target_pub, '', '')
                # update publisher ownership dictionary
                self.update_pub_ownership_dict('shutoff', '', target_pub)

            elif msg_type == 'heartbeat':
                pubID = message[1]
                print('MyPublisher %s heartbeat.' % pubID)
                if pubID not in self.heartbeat_dict:
                    self.heartbeat_dict.update({pubID: time()})
                else:
                    self.heartbeat_dict[pubID] = time()

            elif msg_type == 'history':
                topic = message[1]
                history_count = message[2]
                # get publisher who published this topic and has highest ownership (we have sorted the dictionary
                # , so target publisher is exactly the first key)
                try:
                    target_pub = self.pub_ownership_dict[topic].keys()[0]
                    history_publication_list = self.pub_dict[target_pub][topic]
                    topic = topic + '-' + 'history'
                    # send all history publications
                    print('MyBroker is send history publications with topic %s ...' % topic)
                    for index, history_publication in enumerate(history_publication_list):
                        if index < history_count:
                            self.helper.xpub_send_msg(self.xpubsocket, topic, history_publication)
                        else:
                            break
                    print('MyBroker has finished sending history publication with topic %s ' % topic)
                except KeyError:
                    print('Topic %s has no history.' % topic)
                    # send an empty publication list to subscriber
                    self.helper.xpub_send_msg(self.xpubsocket, topic, '')

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
            self.pub_dict.update({pubID: {}})
            print('MyPublisher storage update: Add publisher %s succeed.' % pubID)

        elif update_typ == 'add_publication':
            if topic not in self.pub_dict[pubID].keys():
                self.pub_dict[pubID].update({topic: [publication]})
            else:
                self.pub_dict[pubID][topic].add(publication)
                print('MyPublisher storage update: Add publication for %s succeed.' % pubID)

        elif update_typ == 'drop_topic':
            del self.pub_dict[pubID][topic]
            print('MyPublisher storage update: Drop topic %s for publisher %s succeed.' % (topic, pubID))

        elif update_typ == 'shutoff':
            del self.pub_dict[pubID]
            print('MyPublisher storage update: Shutoff publisher %s succeed.' % pubID)

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
                self.pub_ownership_dict.update({topic: {pubID: random.randint(1, 100)}})
            else:
                if topic not in self.pub_ownership_dict[topic].keys():
                    self.pub_ownership_dict[topic].update({pubID: random.randint(1, 100)})

            # sort the publisher ownership strength based on value
            sorted(self.pub_ownership_dict.items(), key=lambda val: val[1])
            print('MyPublisher ownership update: Update ownership for %s with topic: %s succeed.' % (pubID, topic))

        elif update_type == 'drop_topic':
            if topic not in self.pub_ownership_dict.keys():
                print('MyPublisher ownership update: drop topic %s for publisher %s failed, '
                      '%s not in publisher ownership dictionary.' % (topic, pubID, topic))
            else:
                if topic not in self.pub_ownership_dict[topic].keys():
                    print('MyPublisher ownership update: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
                else:
                    del self.pub_ownership_dict[topic][pubID]
                    print('MyPublisher ownership update: drop topic %s for publisher %s succeed.' % (topic, pubID))

        elif update_type == 'shutoff':
            count = 0
            for key in self.pub_ownership_dict.keys():
                if pubID in self.pub_ownership_dict[key].keys():
                    del self.pub_ownership_dict[key][pubID]
                    count += 1
            print('MyPublisher ownership update: Shutoff publisher %s succeed.' % pubID)

    # filter publisher ownership strength dictionary
    #
    # argument: current publisher & topic
    # return publisher or None
    #
    def filter_pub_ownership_dict(self, pub, topic):
        if self.pub_ownership_dict[topic][pub] == max(self.pub_ownership_dict[topic].values()):
            return pub
        else:
            return None




