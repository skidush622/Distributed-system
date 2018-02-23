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


log_file = './Output/broker.log'


class Broker:
    
    def __init__(self, xsub_port, xpub_port):
        self.shutoff_check = False
        self.heartbeat_lock = threading.Lock()
        self.history_lock = threading.Lock()

        # initialize MyBroker class
        self.helper = ZMQHelper()
        
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
        with open(log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')


    # This handler is used to check if any publisher has dead.
    def vice_handler(self):
        while True:
            with self.heartbeat_lock:
                # check if any publisher has failed
                for pubID in self.heartbeat_dict.keys():
                    if time.time() - self.heartbeat_dict[pubID] > 40:
                        print('\n************************************\n')
                        print('Publisher dead: %s has dead.' % pubID)
                        print('\n************************************\n')
                        with open(log_file, 'a') as logfile:
                            logfile.write('Publisher dead: %s has dead.\n' % pubID)
                        self.update_pub_dict('shutoff', pubID, '', '')
                        self.update_pub_ownership_dict('shutoff', '', pubID)
                        del self.heartbeat_dict[pubID]
                        break
            time.sleep(10)
            
    # This helper function is used to send history publications un-interruptedly
    def history_helper(self):
        while True:
            with self.history_lock:
                group = self.get_highest_strength_pubs()
                for key in list(group.keys())[::-1]:
                    for pubs in list(self.pub_dict[key][group[key]])[::-1]:
                        his_topic = group[key] + '-history'
                        print('Sending history publications: %s : %s' % (his_topic, pubs))
                        with open(log_file, 'a') as logfile:
                            logfile.write('Sending history publications: %s : %s\n' % (his_topic, pubs))
                        self.helper.xpub_send_msg(self.xpubsocket, his_topic, pubs)
            time.sleep(10)

            
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
        heartbeat_thr = threading.Thread(target= self.vice_handler, args= ())
        threading.Thread.setDaemon(heartbeat_thr, True)
        heartbeat_thr.start()

        history_thr = threading.Thread(target= self.history_helper, args= ())
        threading.Thread.setDaemon(history_thr, True)
        history_thr.start()
        while True:
            # receive message from publisher
            msg = self.xsubsocket.recv_string(0, 'utf-8')
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'pub_init' and message[1] in self.pub_dict.keys():
                continue
            print('\n************************************\n')
            print('Publication storage info: (pubID, {topic : [history]})')
            for x in self.pub_dict.keys():
                 print('PUB ID: %s' % x)
                 for y in self.pub_dict[x].keys():
                     print('     Topic: %s' % y)
                     for z in self.pub_dict[x][y]:
                         print('         Publication: %s' % z)
            print('\n************************************\n')
            print('Publisher ownership info: (topic, {pubID : ownership_strength})')
            for x in self.pub_ownership_dict.keys():
                print('Topic: %s' % x)
                for y in self.pub_ownership_dict[x].keys():
                    print('PUB ID: %s ----------> Ownership Strength: %s' % (y, self.pub_ownership_dict[x][y]))

            if msg_type == 'pub_init':
                pubID = message[1]
                topic = message[2]
                print('\n************************************\n')
                print('Init msg: %s init with topic %s' % (pubID, topic))
                print('\n************************************\n')
                with open(log_file, 'a') as logfile:
                    logfile.write('Init msg: %s init with topic %s\n' % (pubID, topic))
                # publisher registration
                self.update_pub_dict('add_pub', pubID, topic, '')
                self.update_pub_ownership_dict('add_pub', topic, pubID)
                
            elif msg_type == 'publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                print('\n************************************\n')
                print('Publication: %s published %s with topic %s' % (pubID, publication, topic))
                print('\n************************************\n')
                with open(log_file, 'a') as logfile:
                    logfile.write('Publication: %s published %s with topic %s\n' % (pubID, publication, topic))
                # update storage
                self.update_pub_dict('add_publication', pubID, topic, publication)

                # update publisher ownership_strength for a topic
                self.update_pub_ownership_dict('add_pub', topic, pubID)

                # filter publisher via ownership strength
                if self.filter_pub_ownership_dict(pubID, topic) is None:
                    print('     Broker filter feedback: %s doesn\'t own highest ownership strength, %s won\'t be forwarded.'
                          % (pubID, topic))
                    with open(log_file, 'a') as logfile:
                        logfile.write('Broker filter feedback: %s doesn\'t own highest ownership strength, %s won\'t be forwarded.\n'
                          % (pubID, topic))
                    continue
                else:
                    with self.history_lock:
                        # send publication to subscribers using xpubsocket
                        publication = publication + '--' + str(time.time())
                        self.helper.xpub_send_msg(self.xpubsocket, topic, publication)

            elif msg_type == 'drop_topic':
                target_pub = message[1]
                target_topic = message[2]
                print('\n************************************\n')
                print('Drop topic: %s droped topic %s' % (target_pub, target_topic))
                print('\n************************************\n')
                with open(log_file, 'a') as logfile:
                    logfile.write('Drop topic: %s droped topic %s\n' % (target_pub, target_topic))
                self.update_pub_dict('drop_topic', target_pub, target_topic, '')
                self.update_pub_ownership_dict('drop_topic', target_topic, target_pub)

            # This shutoff is a soft shutoff, which means publisher would tell broker in advance
            elif msg_type == 'shutoff':
                with self.heartbeat_lock:
                    target_pub = message[1]
                    # update publisher dictionary
                    self.update_pub_dict('shutoff', target_pub, '', '')
                    # update publisher ownership dictionary
                    self.update_pub_ownership_dict('shutoff', '', target_pub)
                    print('\n************************************\n')
                    print('Soft shutoff: %s soft shutoff' % target_pub)
                    print('\n************************************\n')
                    with open(log_file, 'a') as logfile:
                        logfile.write('Soft shutoff: %s soft shutoff\n' % target_pub)
                    self.shutoff_check = True

            elif msg_type == 'heartbeat':
                pubID = message[1]
                print('\n************************************\n')
                with self.heartbeat_lock:
                    if pubID not in self.heartbeat_dict:
                        self.heartbeat_dict.update({pubID: time.time()})
                    else:
                        self.heartbeat_dict[pubID] = time.time()
                
                print('Heartbeat: %s heartbeat' % pubID)
                print('\n************************************\n')
                with open(log_file, 'a') as logfile:
                    logfile.write('Heartbeat: %s heartbeat\n' % pubID)

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
        try:
            if update_typ == 'add_pub':
                self.pub_dict.update({pubID: {topic:[]}})

            elif update_typ == 'add_publication':
                stored_publication = publication + '--' + str(time.time())
                if topic not in self.pub_dict[pubID].keys():
                    self.pub_dict[pubID].update({topic: [stored_publication]})
                else:
                    self.pub_dict[pubID][topic].append(stored_publication)

            elif update_typ == 'drop_topic':
                if topic not in self.pub_dict[pubID].keys():
                    print('     Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
                    with open(log_file, 'a') as logfile:
                        logfile.write('Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.\n' % (topic, pubID, pubID))
                else:
                    del self.pub_dict[pubID][topic]

            elif update_typ == 'shutoff':
                del self.pub_dict[pubID]
        except KeyError:
            pass
        

    # update publisher ownership strength dictionary
    #
    # arguments: update type, publisher
    # update types:
    # 1. add a publisher
    # 2. drop a topic
    # 3. shutoff a publisher
    #
    def update_pub_ownership_dict(self, update_type, topic, pubID):
        try:
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
                    with open(log_file, 'a') as logfile:
                        logfile.write('Broker filter feedback: drop topic %s for publisher %s failed, '
                          '%s not in publisher ownership dictionary.\n' % (topic, pubID, topic))
                else:
                    if pubID not in self.pub_ownership_dict[topic].keys():
                        print('     Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
                        with open(log_file, 'a') as logfile:
                            logfile.write('Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.\n' % (topic, pubID, pubID))
                    else:
                        del self.pub_ownership_dict[topic][pubID]
                        
            elif update_type == 'shutoff':
                for key in self.pub_ownership_dict.keys():
                    if pubID in self.pub_ownership_dict[key].keys():
                        del self.pub_ownership_dict[key][pubID]

        except KeyError:
            pass
        
    # filter publisher ownership strength dictionary
    #
    # argument: current publisher & topic
    # return publisher or None
    #
    def filter_pub_ownership_dict(self, pubID, topic):
        try:
            if self.pub_ownership_dict[topic][pubID] == max(self.pub_ownership_dict[topic].values()):
                return pubID
            else:
                return None
        except Exception:
            return None

    # This function is used to get all publications who have highest ownership strength for all topics
    def get_highest_strength_pubs(self):
        try:
            group = {}
            for topic in self.pub_ownership_dict.keys():
                max_str = max(self.pub_ownership_dict[topic].values())
                for pub in self.pub_ownership_dict[topic].keys():
                    if self.pub_ownership_dict[topic][pub] == max_str:
                        group.update({pub:topic})
            return group
        except Exception:
            return {}
