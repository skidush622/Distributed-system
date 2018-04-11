#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Broker.py
#
# CS6381 Assignment2
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

# NOTE: Msgs received from publishers
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Publisher ---> Broker
# register    msg: 'pub_init' + '#' + pubID + '#' + init_topic + '#'
# publication msg: 'publication' + '#' + pubID + '#' + topic + '#' + msg
# drop topic  msg: 'drop_topic' + '#' + pubID + '#' + topic + '#'
# soft shutoff msg: 'shutoff' + '#' + pubID + '#'
# heartbeat   msg: 'pub_heartbeat' + '#' + pubID + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NOTE: Msgs sent to publishers
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Broker ---> Publisher
# update IP address msg: 'UpdateIP' + '#' + new_ip + '#' + topic + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NOTE: Msgs sent to subscribers
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Broker ---> Subscriber
# subscription        msg: 'publication' + '#' + publication + '--' + time
# update IP address   msg: 'UpdateIP' + '#' + new_ip + '#'
# history publication msg: 'history_publication' + '#' + [publication+'--'+time]
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NOTE: Msgs received from subscribers
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Subscriber ---> Broker
# request history publications msg: 'request_history_publication' + '#' + topic + '#' + history_count + '#'
# send IP to broker            msg: 'subscriber_IP' + '#' + ip + '#' + topic + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# NOTE: Msgs sent(received) to(from) Broker
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message fotmat: Broker ---> Broker
# heartbeat msg to precursor node : 'broker_heartbeat' + '#' + broker_id + '#'
# initialize replica data dictionary: 'init_replica' + '#' + 'this_is' + '#' + brokerID + '#' + 'mytopics' + '#' + topics
# precursor died msg: 'precursor_died' + '#' + 'this_is' + '#' + brokerID + '#' + 'my_precursor' + '#' + brokerID + '#' + 'my_precursor_data' + '#' + data + '#'
# update hop count msg: 'update_hopcount' + '#' + 'for' + '#' + brokerID + '#' + 'data' + '#' +  data
# update hashring msg: 'update_hashring' + '#' + 'broker' + '#' + brokerID + '#' + 'died'
# update replica data:
# 1. 'update_replica' + '#' + 'register' + '#' + 'src' + '#' + brokerID + '#' + 'publisher' + '#' + pubID + '#' + 'topic' + '#' + topic + '#' + 'ownership_strength' + '#' + ownership_strength
# 2. 'updata_replica' + '#' + 'new_publication' + '#' + 'src' + '#' + brokerID + '#' + 'publisher' + '#' + pubID + '#' + 'topic' + '#' + topic + '#' + 'publication' + '#' + publication
# 3. 'update_replica' + '#' + 'drop_topic' + '#' + 'src' + '#' + brokerID + '#' + 'publisher' + '#' + pubID + '#' + 'topic' + '#' + topic + '#'
# 4. 'update_replica' + '#' + 'shutoff' + '#' + 'src' + '#' + brokerID + '#' + 'publisher' + '#' + pubID + '#'
# 5. 'update_replica' + '#' + 'new_subscriber' + '#' + 'src' + '#' + brokerID + '#' + 'subscriber' + '#' + subID + '#' + 'topic' + '#' + topic
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# NOTE: Ports on broker
# 1. xsub --- socket type: PUB/SUB >> 5556
# 2. xpub --- socket type: PUB/SUB >> 5557
# 3. port1: used to receive heartbeat from successor --- socket type: Client/Server >> 5558
# 4. port2: used to receive update hash ring msg --- socket type: Client/Server >> 5559
# 5. port3: used to receive msg from precursor --- socket type: Client/Server >> 6000
# 6. port4: used to receive ip address from subscriber & used to receive history publications request --- socket type: Client/Server >> 6001
# 7. port5: used to notify publisher update IP address --- socket type: Client/Server >> 6002
# 8. port6: used to notify subscriber update IP address --- socket type: Client/Server >> 6003

from ZMQHelper import ZMQHelper
from hash_ring import HashRing

import simplejson
import random
import threading
import time


class Broker:
    # Note: Broker constructor method
    def __init__(self, brokerIPs, ip, xsub_port, xpub_port, port1, port2, port3, port4, port5, port6):
        # Call ZMQ API
        self.zmqhelper = ZMQHelper()

        # NOTE: Initialize fields
        self.id = str(ip)

        self.log_file = './Output/broker--' + str(self.id) + '.log'

        self.hash_ring = HashRing(brokerIPs)

        # Define successor table
        # format: [$(successor_broker_id)]
        self.successor_table = self.hash_ring.get_successor_table(self.id)
        self.xsubsocket, self.xpubsocket = self.zmqhelper.prepare_broker(self.id, xsub_port, xpub_port)
        self.port1 = port1
        self.port2 = port2
        self.port3 = port3
        self.port4 = port4
        self.port5 = port5
        self.port6 = port6

        self.broker_lock = threading.Lock()
        self.replica_lock = threading.Lock()
        self.pub_dict_lock = threading.Lock()
        self.successor_table_lock = threading.Lock()

        # sockets
        # formate: {$port: {publisher: $socket}}
        self.pub_sockets = {}
        # formate: {$port: {$broker: $socket}}
        self.broker_sockets = {}
        # formate: {$port: {$subscriber: $socket}}
        self.sub_sockets = {}

        # NOTE: storage initialization
        # Define data table
        # used to store replicas
        ''' Format:
        {
          $(brokerID): {
                           'hop_count': $(hop count),
                            'topics': {
                                        $(topic): {
                                                   'publishers': {
                                                                  $(pubID): {
                                                                              'ownership_strength':$(ownership_strength),
                                                                              'publications': [$(publication)]
                                                                              }
                                                                  },
                                                   'subscribers': []
                                                  }
                                      }
                     }
        }
        '''

        self.replica_data = {}
        self.replica_data.update({self.successor_table[len(self.successor_table)-1]: {'hop_count': 1, 'topics': {}}})
        self.replica_data.update({self.successor_table[len(self.successor_table) - 2]: {'hop_count': 0, 'topics': {}}})

        # publisher dictionary (including ownership strength)
        # dictionary format
        '''
        {
          $(topic): {
                       $(pubID): {
                                    'ownership_strength':$(ownership_strength),
                                    'publications': [$(publication)]
                                  }
                    }
        }
        '''

        self.pub_dict = {}

        # publisher heartbeat dictionary
        # This dict is used to record publisher's latest heartbeat timestamp
        # $(pubID):$(time)
        self.heartbeat_dict = {}

        # Subscriber table
        # format: {$(subID): $(topic)}
        self.subscriber_dict = {}

        # NOTE: Others
        self.shutoff_check = False
        self.heartbeat_lock = threading.Lock()

        print('\n************************************\n')
        print('Init MyBroker succeed.')
        print('\n************************************\n')
        with open(self.log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')

    def handler(self):
        publisher_thr = threading.Thread(target=self.publisher_handler, args=())
        threading.Thread.setDaemon(publisher_thr, True)

        broker_thr = threading.Thread(target=self.broker_handler, args=())
        threading.Thread.setDaemon(broker_thr, True)

        subscriber_thr = threading.Thread(target=self.subscriber_handler, args=())
        threading.Thread.setDaemon(subscriber_thr, True)

        broker_thr.start()
        publisher_thr.start()
        subscriber_thr.start()

    # Note: This is a handler method, which is used to process msgs received from publisher
    # In other words, all msgs in this method are transferred via xpub & xsub socket.
    def publisher_handler(self):
        # This thread is used to check all publishers' heartbeat using a while loop
        heartbeat_thr = threading.Thread(target=self.publisher_hb_monitor, args=())
        threading.Thread.setDaemon(heartbeat_thr, True)
        heartbeat_thr.start()
        while True:
            # receive message from publisher
            msg = self.xsubsocket.recv_string()
            self.xsubsocket.send_string('OK')
            print('Received msg from publisher: %s' % msg)

            message = msg.split('#')
            msg_type = message[0]

            print('\n************************************\n')
            print('Publication storage info: (pubID, {topic : [history]})')
            print(self.pub_dict)
            print('\n************************************\n')

            with open(self.log_file, 'a') as log:
                log.write('\n************************************\n')
                log.write('Publication storage info: (pubID, {topic : [history]})\n')
                log.write(simplejson.dumps(self.pub_dict))
                print('\n************************************\n')

            if msg_type == 'pub_init':
                pubID = message[1]
                topic = message[2]
                self.publisher_registration(pubID, topic)

            elif msg_type == 'publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                self.receive_publication(pubID, topic, publication)

            elif msg_type == 'drop_topic':
                target_pub = message[1]
                target_topic = message[2]
                self.receive_drop_topic_msg(target_pub, target_topic)

            elif msg_type == 'shutoff':
                with self.heartbeat_lock:
                    target_pub = message[1]
                    self.receive_soft_shutoff_msg(target_pub)

            elif msg_type == 'pub_heartbeat':
                pubID = message[1]
                self.receive_hb_from_pubs(pubID)

    # Note: This is a handler method to control all behaviors related to broker
    def broker_handler(self):
        precursor_thr = threading.Thread(target=self.listen_precursor_msg, args= ())
        threading.Thread.setDaemon(precursor_thr, True)
        precursor_thr.start()

        heartbeat_thr = threading.Thread(target=self.send_hb_2_precursor, args=())
        threading.Thread.setDaemon(heartbeat_thr, True)
        heartbeat_thr.start()

        successor_thr = threading.Thread(target=self.listen_successor_heartbeat, args=())
        threading.Thread.setDaemon(successor_thr, True)
        successor_thr.start()

        hashring_thr = threading.Thread(target=self.listen_update_hashring, args = ())
        threading.Thread.setDaemon(hashring_thr, True)
        hashring_thr.start()

    # Note: This is a handler method to control all behaviors related to subscriber
    def subscriber_handler(self):
        subscriber_thr = threading.Thread(target=self.recv_msg_from_sub, args= ())
        threading.Thread.setDaemon(subscriber_thr, True)
        subscriber_thr.start()

    # # # # # # # # # # # # # # # # # # # # # #
    # NOTE: Msgs sent to broker
    # # # # # # # # # # # # # # # # # # # # # #
    # Note: send heartbeat msg to precursor broker node
    def send_hb_2_precursor(self):
        while True:
            try:
                if self.port1 not in self.broker_sockets.keys() or self.successor_table[
                    len(self.successor_table) - 1] not in \
                        self.broker_sockets[self.port1].keys():
                    self.broker_sockets.update({self.port1: {
                        self.successor_table[len(self.successor_table) - 1]: self.zmqhelper.csreq(
                            self.successor_table[len(self.successor_table) - 1], self.port1)}})
                    self.broker_sockets[self.port1][self.successor_table[len(self.successor_table) - 1]].RCVTIMEO = 40000
                    time.sleep(random.randint(3, 5))
                # send heartbeat nsg to precursor broker node
                msg = 'broker_heartbeat' + '#' + self.id + '#'
                self.broker_sockets[self.port1][self.successor_table[len(self.successor_table)-1]].send_string(msg)
                self.broker_sockets[self.port1][self.successor_table[len(self.successor_table)-1]].recv_string()
            except Exception as ex:
                print('---------------------------------\n')
                print(ex)
                print('---------------------------------\n')
            print('\n************************************\n')
            print('Send heart beat msg to precursor broker node: %s' % self.successor_table[len(self.successor_table)-1])
            print('Heartbeat msg: %s\n' % msg)
            print('\n************************************\n')
            with open(self.log_file, 'a') as log:
                log.write('\n************************************\n')
                log.write('Send heart beat msg to precursor broker node: %s\n' % self.successor_table[len(self.successor_table)-1])
                log.write('Heartbeat msg %s\n' % msg)
                log.write('\n************************************\n')
            time.sleep(random.randint(20, 30))

    # Note: listen hash ring update message
    def listen_update_hashring(self):
        mysocket = self.zmqhelper.csrecv(self.port2)
        while True:
            msg = mysocket.recv_string()
            mysocket.send_string('OK')
            msg = msg.split('#')
            brokerID = msg[2]
            # update hash ring
            self.hash_ring.remove_node(brokerID)
            # update successor table
            print('\nReceived update hash ring msg, %s died\n' % brokerID)
            with self.successor_table_lock:
                self.successor_table.remove(brokerID)
            with open(self.log_file, 'a') as log:
                log.write('\n************************************\n')
                log.write('Update hash ring, %s died.\n' % brokerID)
                log.write('\n************************************\n')

    # # # # # # # # # # # # # # # # # # # # # #
    # NOTE: Msgs received from broker
    # # # # # # # # # # # # # # # # # # # # # #
    # Note: Define a method to receive heartbeat msg from successor node
    def listen_successor_heartbeat(self):
        # listen heartbeat msg from successor node
        mysocket = self.zmqhelper.csrecv(self.port1)
        while True:
            # set timeout for socket
            mysocket.RCVTIMEO = 45000
            msg = None
            try:
                msg = mysocket.recv_string()
                mysocket.send_string('OK')
                print('\n************************************\n')
                print('Received successor broker heartbeat: %s.\n' % msg)
                print('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Receive heartbeat msg from successor broker node: %s\n' % msg)
                    log.write('\n************************************\n')
            except Exception as ex:
                print('\n------------------------------------------\n')
                print(ex)
                print('\n------------------------------------------\n')
                print('Receive heartbeat msg from successor broker node: %s' % msg)

                print('Detected successor node died: %s' % msg)
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Detected successor node died\n')
                    log.write('\n************************************\n')

                # update hash ring
                with self.successor_table_lock:
                    self.hash_ring.remove_node(self.successor_table[0])

                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Update hash ring.\n')
                    log.write('\n************************************\n')

                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Notify other brokers to update hash ring\n')
                    log.write('\n************************************\n')

                with self.broker_lock:
                    # Note: notify other nodes to update hash ring
                    for index, node in enumerate(self.successor_table):
                        if index == 0:
                            continue
                        msg = 'update_hashring' + '#' + 'broker' + '#' + self.successor_table[0] + '#' + 'died'
                        if self.port2 not in self.broker_sockets.keys()or node not in self.broker_sockets[self.port2].keys():
                            self.broker_sockets.update({self.port2: {node: self.zmqhelper.csreq(node, self.port2)}})
                            time.sleep(random.randint(3, 5))
                        self.broker_sockets[self.port2][node].send_string(msg)
                        self.broker_sockets[self.port2][node].recv_string()
                        print('Notify %s to update hash ring.\n' % node)
                        with open(self.log_file, 'a') as log:
                            log.write('\n************************************\n')
                            log.write('Notify %s to update hash ring.\n' % node)
                            log.write('\n************************************\n')

                # successor died
                with self.replica_lock:
                    data = self.replica_data[self.successor_table[len(self.successor_table) - 1]]['topics']
                    data = simplejson.dumps(data)

                # Notify new successor node that its old precursor node died.
                try:
                    with self.broker_lock:
                        if self.port3 not in self.broker_sockets.keys() or self.successor_table[1] not in \
                                self.broker_sockets[self.port3].keys():
                            self.broker_sockets.update({self.port3: {
                                self.successor_table[1]: self.zmqhelper.csreq(self.successor_table[1],
                                                                              self.port3)}})
                            self.broker_sockets[self.port3][self.successor_table[1]].RCVTIMEO = 40000
                        time.sleep(random.randint(3, 5))
                        msg = 'precursor_died' + '#' + 'this_is' + '#' + self.id + '#' + 'my_precursor' + '#' + \
                              self.successor_table[
                                  len(self.successor_table) - 1] + '#' + 'my_precursor_data' + '#' + data + '#'
                        self.broker_sockets[self.port3][self.successor_table[1]].send_string(msg)
                        self.broker_sockets[self.port3][self.successor_table[1]].recv_string()
                except Exception as ex:
                    print('-------------------------\n')
                    print('Port3 timeout\n')
                    print(ex)
                    print('-------------------------\n')

                # update successor table
                with self.successor_table_lock:
                    self.successor_table.pop(0)
                # mysocket = self.zmqhelper.csrecv(self.port1)

    # Note: receive message from precursor when node died
    def listen_precursor_msg(self):
        # try:
        mysocket = self.zmqhelper.csrecv(self.port3)
        while True:
            msg = mysocket.recv_string()
            storted_msg = msg
            msg = msg.split('#')
            mysocket.send_string('OK')
            if msg[0] == 'precursor_died':
                print('\n************************************\n')
                print('My old precursor broker node died...\n')
                print('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('My old precursor broker node died...\n')
                    log.write('Start sending heartbeat info to new precursor broker.\n')
                    log.write('\n************************************\n')
                new_precursor = msg[2]
                new_precursor_pre = msg[4]
                new_precursor_pre_data = simplejson.loads(msg[6])
                # take over old precussor node's data
                with self.replica_lock:
                    # 将hopcount为1 的那份数据删除
                    for broker in self.replica_data.keys()[:]:
                        if self.replica_data[broker]['hop_count'] == 1:
                            precursor_data = self.replica_data[broker]
                            del self.replica_data[broker]
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update replica data...\n')
                        log.write('\n************************************\n')
                pubs = {}
                subs = []
                for topic in precursor_data['topics'].keys():
                    if 'publishers' in precursor_data['topics'][topic].keys():
                        with self.pub_dict_lock:
                            self.pub_dict.update({topic: precursor_data['topics'][topic]['publishers']})
                        pubs.update({topic: precursor_data['topics'][topic]['publishers'].keys()})
                    if 'subscribers' in precursor_data['topics'][topic].keys():
                        for subscriber in precursor_data['topics'][topic]['subscribers']:
                            self.subscriber_dict.update({subscriber: topic})
                        subs.extend(precursor_data['topics'][topic]['subscribers'])

                # TODO: notify publishers update IP
                for topic in pubs.keys():
                    msg = 'UpdateIP' + '#' + self.id + '#' + topic + '#'
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                    for pub in pubs[topic]:
                        if self.port5 not in self.pub_sockets.keys():
                            self.pub_sockets.update({self.port5: {pub: self.zmqhelper.csreq(pub, self.port5)}})
                            time.sleep(random.randint(3, 5))
                            self.pub_sockets[self.port5][pub].send_string(msg)
                        elif pub not in self.pub_sockets[self.port5].keys():
                            self.pub_sockets[self.port5].update({pub: self.zmqhelper.csreq(pub, self.port5)})
                            time.sleep(random.randint(3, 5))
                            self.pub_sockets[self.port5][pub].send_string(msg)
                        else:
                            self.pub_sockets[self.port5][pub].send_string(msg)
                        self.pub_sockets[self.port5][pub].recv_string()
                        print('Notify publisher %s to update IP address.\n' % pub)
                        with open(self.log_file, 'a') as log:
                            log.write('Notify publisher %s to update IP address.\n' % pub)

                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')

                # TODO: notify subscriber update IP
                for sub in subs:
                    msg = 'UpdateIP' + '#' + self.id + '#'
                    if self.port6 not in self.sub_sockets.keys():
                        self.sub_sockets.update({self.port6: {sub: self.zmqhelper.csreq(sub, self.port6)}})
                        time.sleep(random.randint(3, 5))
                        self.sub_sockets[self.port6][sub].send_string(msg)
                    elif sub not in self.sub_sockets[self.port6].keys():
                        self.sub_sockets[self.port6].update({sub: self.zmqhelper.csreq(sub, self.port6)})
                        time.sleep(random.randint(3, 5))
                        self.sub_sockets[self.port6][sub].send_string(msg)
                    else:
                        self.sub_sockets[self.port6][sub].send_string(msg)
                    self.sub_sockets[self.port6][sub].recv_string()
                    with open(self.log_file, 'a') as log:
                        log.write('Notify subscriber %s to update IP address.\n' % sub)
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')

                # 将hopcount为0 的那份数据hopcount++，并且传递给自己的后继节点，后继节点将存储新前驱节点的最后一份备份
                with self.replica_lock:
                    for broker in self.replica_data.keys()[:]:
                        if self.replica_data[broker]['hop_count'] == 0:
                            self.replica_data[broker]['hop_count'] += 1
                            data = simplejson.dumps(self.replica_data[broker]['topics'])
                            update_hopcount_msg = 'update_hopcount' + '#' + 'for' + '#' + self.successor_table[len(self.successor_table)-1] + '#' + 'data' + '#' + data
                try:
                    with self.broker_lock:
                        if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[
                            self.port3].keys():
                            self.broker_sockets.update({self.port3: {
                                self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                            self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                            time.sleep(random.randint(3, 5))
                            self.broker_sockets[self.port3][self.successor_table[0]].send_string(update_hopcount_msg)
                        else:
                            self.broker_sockets[self.port3][self.successor_table[0]].send_string(update_hopcount_msg)
                        self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                except Exception as ex:
                    print('-------------------------\n')
                    print('Port3 timeout\n')
                    print(ex)
                    print('-------------------------\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Notify successor node to store the third backup for my precursor node.\n')
                    log.write('\n************************************\n')

                # 将收到的这份数据存储并将hopcount设为0
                with self.replica_lock:
                    self.replica_data.update({self.successor_table[len(self.successor_table)-2]: {'hop_count': 0, 'topics': new_precursor_pre_data}})

                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Add an backup for the precursor node of my new precursor node.\n')
                    log.write('\n************************************\n')
            elif msg[0] == 'update_hopcount':
                brokerID = msg[2]
                data = simplejson.loads(msg[4])
                with self.replica_lock:
                    self.replica_data.update({brokerID: {'hop_count': 0, 'topics': data}})
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Store the backup for the precursor node of my precursor node.\n')
                    log.write('\n************************************\n')
            elif msg[0] == 'update_replica':
                print('\n************************************\n')
                print('Replica Data:')
                print(self.replica_data)
                print('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Replica Data:\n')
                    log.write(simplejson.dumps(self.replica_data))
                    log.write('\n************************************\n')
                update_type = msg[1]
                src = msg[3]
                pubID = msg[5]
                if update_type == 'register':
                    topic = msg[7]
                    ownership_strength = msg[9]
                    with self.replica_lock:
                        if topic not in self.replica_data[src]['topics'].keys():
                            self.replica_data[src]['topics'].update({topic: {'publishers': {pubID: {'ownership_strength': ownership_strength, 'publications': []}}}})
                        elif 'publishers' not in self.replica_data[src]['topics'][topic].keys():
                            self.replica_data[src]['topics'][topic].update({'publishers': {pubID: {'ownership_strength': ownership_strength, 'publications': []}}})
                        else:
                            self.replica_data[src]['topics'][topic]['publishers'].update({pubID: {'ownership_strength': ownership_strength, 'publications': []}})
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update Replica data for %s: \n' % src)
                        log.write('New Publisher %s registered.\n' % pubID)
                        log.write('\n************************************\n')
                elif update_type == 'new_publication':
                    topic = msg[7]
                    publication = msg[9]
                    with self.replica_lock:
                        self.replica_data[src]['topics'][topic]['publishers'][pubID]['publications'].append(publication)
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update Replica data for %s: \n' % src)
                        log.write('New publication from %s.\n' % pubID)
                        log.write('Content: %s\n' % publication)
                        log.write('\n************************************\n')
                elif update_type == 'drop_topic':
                    topic = msg[7]
                    with self.replica_lock:
                        del self.replica_data[src]['topics'][topic]['publishers'][pubID]
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update Replica data for %s: \n' % src)
                        log.write('Publisher %s drop topic %s\n' % (pubID, topic))
                        log.write('\n************************************\n')
                elif update_type == 'shutoff':
                    print('Received update replica: shutoff msg %s' % storted_msg)
                    with self.replica_lock:
                        for topic in self.replica_data[src]['topics'].keys():
                            if pubID in self.replica_data[src]['topics'][topic]['publishers'].keys():
                                    del self.replica_data[src]['topics'][topic]['publishers'][pubID]
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update Replica data for %s: \n' % src)
                        log.write('Publisher %s shutoff.\n' % pubID)
                        log.write('\n************************************\n')
                elif update_type == 'new_subscriber':
                    subID = pubID
                    topic = msg[7]
                    with self.replica_lock:
                        if topic not in self.replica_data[src]['topics'].keys():
                            self.replica_data[src]['topics'].update({topic: {'subscribers': []}})
                        elif 'subscribers' not in self.replica_data[src]['topics'][topic].keys():
                            self.replica_data[src]['topics'][topic].update({'subscribers': []})
                        self.replica_data[src]['topics'][topic]['subscribers'].append(subID)
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Update Replica data for %s: \n' % src)
                        log.write('New subscriber %s registered with topic %s.\n' % (subID, topic))
                        log.write('\n************************************\n')

                # current broker is the second backup
                if self.replica_data[src]['hop_count'] == 1:
                    with self.broker_lock:
                        try:
                            if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[self.port3].keys():
                                self.broker_sockets.update({self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                                self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                                time.sleep(5)
                                self.broker_sockets[self.port3][self.successor_table[0]].send_string(storted_msg)
                            else:
                                self.broker_sockets[self.port3][self.successor_table[0]].send_string(storted_msg)
                            self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                        except Exception as ex:
                            print('-------------------------\n')
                            print('Port3 timeout\n')
                            print(ex)
                            print('-------------------------\n')
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Notify my successor node update replica data.\n')
                        log.write('\n************************************\n')
            '''
            except Exception as ex:
                print('\n------------------------------------------\n')
                print('Error happened while listening precursor msg...\n')
                print(ex)
                print('\n------------------------------------------\n')

            '''

    # # # # # # # # # # # # # # # # # # # # # #
    # NOTE: Msgs received from subscriber
    # # # # # # # # # # # # # # # # # # # # # #
    def recv_msg_from_sub(self):
        mysocket = self.zmqhelper.csrecv(self.port4)
        while True:
            msg = mysocket.recv_string()
            print('Received msg from subscriber: %s' % msg)
            msg = msg.split('#')
            msg_type = msg[0]
            if msg_type == 'subscriber_IP':
                ip = msg[1]
                topic = msg[2]
                # the topic is held by current broker
                with self.pub_dict_lock:
                    keys = self.pub_dict.keys()
                if topic in keys or self.hash_ring.get_node(topic) == self.id:
                    if topic in keys:
                        mysocket.send_string('OK')
                    else:
                        mysocket.send_string('Wait')
                    # update subscriber table
                    self.subscriber_dict.update({ip: topic})

                    print('\n************************************\n')
                    print('Subscriber dictionary:')
                    print(self.subscriber_dict)
                    print('\n************************************\n')
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Recieved IP info from subscriber %s\n' % ip)
                        log.write('\n************************************\n')

                    # Note: notify successor node update replica data
                    msg = 'update_replica' + '#' + 'new_subscriber' + '#' + 'src' + '#' + self.id + '#' + 'subscriber' + '#' + ip + '#' + 'topic' + '#' + topic
                    with self.broker_lock:
                        try:
                            if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[self.port3].keys():
                                self.broker_sockets.update({self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                                self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                                time.sleep(random.randint(3, 8))
                                self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                            else:
                                self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                            self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                        except Exception as ex:
                            print('-------------------------\n')
                            print('Port3 timeout\n')
                            print(ex)
                            print('-------------------------\n')

                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Notify successor broker update replica data.\n')
                        log.write('\n************************************\n')

                    print('\n************************************\n')
                    print('Send new subscriber info to successor node...')
                    print('\n************************************\n')

                # Notify subscriber update IP
                else:
                    target = self.hash_ring.get_node(topic)
                    msg = 'UpdateIP' + '#' + target + '#'
                    mysocket.send_string(msg)
                    print('Notify subscriber update IP...')
                    with open(self.log_file, 'a') as log:
                        log.write('\n************************************\n')
                        log.write('Notify subscriber reconnect to %s.\n' % target)
                        log.write('\n************************************\n')

            elif msg_type == 'request_history_publication':
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Subscriber is requesting history publication.\n')
                    log.write('\n************************************\n')
                topic = str(msg[1])
                history_count = int(msg[2])
                # Note: send history publications to subscriber
                # get pubID who has the highest ownership strength
                for pub in self.pub_dict[topic].keys():
                    target = self.filter_pub_ownership(pub, topic)
                    if target is not None:
                        break
                data = []
                with self.pub_dict_lock:
                    i = len(self.pub_dict[topic][target]['publications'])
                    if i <= history_count:
                        data.extend(self.pub_dict[topic][target]['publications'][:])
                    else:
                        data.extend(self.pub_dict[topic][target]['publications'][-history_count:])

                msg = 'history_publication' + '#' + simplejson.dumps(data)
                mysocket.send_string(msg)
                print('\n************************************\n')
                print('Send history publications to subscriber...')
                print(msg)
                print('\n************************************\n')
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Send history publication to subscriber.\n')
                    log.write('\n************************************\n')

    # # # # # # # # # # # # # # # # # # # # # #
    # NOTE: Msgs received from publisher
    # # # # # # # # # # # # # # # # # # # # # #
    # Note: 1. Publisher registration
    def publisher_registration(self, pubID, topic):
        pubID = str(pubID)
        topic = str(topic)
        # Registered topic is held by current broker
        if self.hash_ring.get_node(topic) == self.id:
            print('\n************************************\n')
            print('Init msg: %s init with topic %s' % (pubID, topic))
            print('\n************************************\n')
            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Init msg: %s init with topic %s\n' % (pubID, topic))
                logfile.write('\n************************************\n')
            # Notify publisher start sending msg
            msg = 'start' + '#' + pubID + '#' + topic + '#'
            # publisher registration
            with self.pub_dict_lock:
                self.update_pub_dict('add_pub', pubID, topic, '')
            if self.port5 not in self.pub_sockets.keys():
                self.pub_sockets.update({self.port5: {str(pubID): self.zmqhelper.csreq(str(pubID), self.port5)}})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            elif pubID not in self.pub_sockets[self.port5].keys():
                self.pub_sockets[self.port5].update({str(pubID): self.zmqhelper.csreq(str(pubID), self.port5)})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            else:
                self.pub_sockets[self.port5][pubID].send_string(msg)
            self.pub_sockets[self.port5][pubID].recv_string()

            print('\n************************************\n')
            print('Notify publisher start sending msg: %s' % msg)
            print('\n************************************\n')

            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Notify publisher %s start sending publication.\n' % pubID)
                logfile.write('\n************************************\n')

            # initialize heartbeat dictionary
            with self.heartbeat_lock:
                self.heartbeat_dict.update({pubID: time.time()})

            # Note: notify successor broker update replica data
            msg = 'update_replica' + '#' + 'register' + '#' + 'src' + '#' + self.id + '#' + 'publisher' + '#' + str(pubID) + '#' + 'topic' + '#' + topic + '#' + 'ownership_strength' + '#' + str(self.pub_dict[topic][pubID]['ownership_strength'])
            with self.broker_lock:
                try:
                    if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[
                        self.port3].keys():
                        self.broker_sockets.update(
                            {self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                        self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                        time.sleep(random.randint(3, 5))
                        self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                    else:
                        self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                    self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                except Exception as ex:
                    print('-------------------------\n')
                    print('Port3 timeout\n')
                    print(ex)
                    print('-------------------------\n')
                print('Notify successor node update replica data: %s' % msg)
                with open(self.log_file, 'a') as logfile:
                    logfile.write('\n************************************\n')
                    logfile.write('Notify successor broker to update replica data, new publisher %s registered.\n' % pubID)
                    logfile.write('\n************************************\n')

        else:
            # TODO: DHT routing and find target broker node
            target = self.hash_ring.get_node(topic)
            # TODO: notify publisher to update IP address and reconnect to the target broker
            msg = 'UpdateIP' + '#' + target + '#' + topic + '#'
            print('\n************************************\n')
            print('Notify publisher update IP: %s' % msg)
            print('\n************************************\n')
            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Notify publisher reconnect to new IP: %s\n' % target)
                logfile.write('\n************************************\n')

            if self.port5 not in self.pub_sockets.keys():
                self.pub_sockets.update({self.port5: {str(pubID): self.zmqhelper.csreq(str(pubID), self.port5)}})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            elif pubID not in self.pub_sockets[self.port5].keys():
                self.pub_sockets[self.port5].update({str(pubID): self.zmqhelper.csreq(str(pubID), self.port5)})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            else:
                self.pub_sockets[self.port5][pubID].send_string(msg)
            self.pub_sockets[self.port5][pubID].recv_string()

    # Note: 2. Receive publication from publisher
    def receive_publication(self, pubID, topic, publication):
        topic = str(topic)
        pubID = str(pubID)
        publication = str(publication)
        # The topic is maintained by current broker
        if self.hash_ring.get_node(topic) == self.id:
            print('\n************************************\n')
            print('Publication: %s published %s with topic %s' % (pubID, publication, topic))
            print('\n************************************\n')

            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Publication: %s published %s with topic %s\n' % (pubID, publication, topic))
                logfile.write('\n************************************\n')
            # Update storage
            with self.pub_dict_lock:
                self.update_pub_dict('add_publication', pubID, topic, publication)

            # Note: notify successor broker node update replica data
            msg = 'update_replica' + '#' + 'new_publication' + '#' + 'src' + '#' + self.id + '#' + 'publisher' + '#' + pubID + '#' + 'topic' + '#' + topic + '#' + 'publication' + '#' + publication
            print('Received new publication, notify successor node to update replica data: %s' % msg)

            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Notify successor node to update replica data.\n')
                logfile.write('\n************************************\n')

            with self.broker_lock:
                try:
                    if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[
                        self.port3].keys():
                        self.broker_sockets.update(
                            {self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                        self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                        time.sleep(random.randint(3, 5))
                        self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                    else:
                        self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                    self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                except Exception as ex:
                    print('-------------------------\n')
                    print('Port3 timeout\n')
                    print(ex)
                    print('-------------------------\n')

            # Filter publisher via ownership strength
            if self.filter_pub_ownership(pubID, topic) is None:
                print('Broker filter feedback: %s doesn\'t own highest ownership strength, %s won\'t be forwarded.'
                      % (pubID, topic))
                with open(self.log_file, 'a') as logfile:
                    logfile.write('\n************************************\n')
                    logfile.write('Broker filter feedback: %s doesn\'t own highest ownership strength, %s won\'t be forwarded.\n'
                      % (pubID, topic))
                    logfile.write('\n************************************\n')
            else:
                # Send publication to subscribers using xpubsocket
                publication = publication + '--' + str(time.time())
                self.zmqhelper.xpub_send_msg(self.xpubsocket, str(topic), str(publication))
        else:
            # Note: DHT routing and find target broker node
            target = self.hash_ring.get_node(topic)
            # Note: notify publisher to update IP address and reconnect to the target broker
            msg = 'UpdateIP' + '#' + target + '#' + topic + '#'
            if self.port5 not in self.pub_sockets.keys():
                self.pub_sockets.update({self.port5: {pubID: self.zmqhelper.csreq(pubID, self.port5)}})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            elif pubID not in self.pub_sockets[self.port5].keys():
                self.pub_sockets[self.port5].update({pubID: self.zmqhelper.csreq(pubID, self.port5)})
                time.sleep(random.randint(3, 6))
                self.pub_sockets[self.port5][pubID].send_string(msg)
            else:
                self.pub_sockets[self.port5][pubID].send_string(msg)
            self.pub_sockets[self.port5][pubID].recv_string()
            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Publisher %s published new topic, which is not held by me\n.' % pubID)
                logfile.write('Notify publisher connect to %s.\n' % target)
                logfile.write('\n************************************\n')

    # Note: 3. publisher wants to drop a topic
    def receive_drop_topic_msg(self, target_pub, target_topic):
        print('\n************************************\n')
        print('Drop topic: %s droped topic %s' % (target_pub, target_topic))
        print('\n************************************\n')
        with open(self.log_file, 'a') as logfile:
            logfile.write('\n************************************\n')
            logfile.write('Drop topic: %s dropped topic %s\n' % (target_pub, target_topic))
            logfile.write('\n************************************\n')

        with self.pub_dict_lock:
            self.update_pub_dict('drop_topic', target_pub, target_topic, '')
        # Note: notify successor node drop this topic from replica data
        msg = 'update_replica' + '#' + 'drop_topic' + '#' + 'src' + '#' + self.id + '#' + 'publisher' + '#' + target_pub + '#' + 'topic' + '#' + target_topic + '#'

        with self.broker_lock:
            try:
                if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[
                    self.port3].keys():
                    self.broker_sockets.update(
                        {self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                    self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                    time.sleep(random.randint(3, 8))
                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                else:
                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                self.broker_sockets[self.port3][self.successor_table[0]].recv()
            except Exception as ex:
                print('-------------------------\n')
                print('Port3 timeout\n')
                print(ex)
                print('-------------------------\n')
        with open(self.log_file, 'a') as logfile:
            logfile.write('\n************************************\n')
            logfile.write('Notify successor broker update replica data.\n')
            logfile.write('\n************************************\n')

    # Note: 4. broker received soft shutoff msg from publisher
    def receive_soft_shutoff_msg(self, target_pub):
        # Update publisher dictionary
        with self.pub_dict_lock:
            self.update_pub_dict('shutoff', target_pub, '', '')
        print('\n************************************\n')
        print('Soft shutoff: %s soft shutoff' % target_pub)
        print('\n************************************\n')
        with open(self.log_file, 'a') as logfile:
            logfile.write('\n************************************\n')
            logfile.write('Soft shutoff: %s soft shutoff\n' % target_pub)
            logfile.write('\n************************************\n')

        self.shutoff_check = True
        # Note: notify successor node delete all data related to this publisher from replica data
        msg = 'update_replica' + '#' + 'shutoff' + '#' + 'src' + '#' + self.id + '#' + 'publisher' + '#' + target_pub + '#'

        with self.broker_lock:
            try:
                if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[
                    self.port3].keys():
                    self.broker_sockets.update(
                        {self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                    self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                    time.sleep(random.randint(3, 8))
                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                else:
                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                self.broker_sockets[self.port3][self.successor_table[0]].recv()
            except Exception as ex:
                print('-------------------------\n')
                print('Port3 timeout\n')
                print(ex)
                print('-------------------------\n')
        with open(self.log_file, 'a') as logfile:
            logfile.write('\n************************************\n')
            logfile.write('Notify successor broker update replica data.\n')
            logfile.write('\n************************************\n')

    # Note: 5. receive heartbeat msg from publisher
    def receive_hb_from_pubs(self, pubID):
        if pubID in self.heartbeat_dict.keys():
            with self.heartbeat_lock:
                self.heartbeat_dict[pubID] = time.time()
            print('\n************************************\n')
            print('Publisher heartbeat: %s heartbeat' % pubID)
            print('\n************************************\n')
            with open(self.log_file, 'a') as logfile:
                logfile.write('\n************************************\n')
                logfile.write('Heartbeat: %s heartbeat\n' % pubID)
                logfile.write('\n************************************\n')

    # # # # # # # # # # # # # # # # # # # # # #
    # NOTE: Utility methods
    # # # # # # # # # # # # # # # # # # # # # #
    # Note: Define a method to monitor publisher's heartbeat
    def publisher_hb_monitor(self):
        while True:
            with self.heartbeat_lock:
                # check if any publisher has failed
                for pubID in self.heartbeat_dict.keys():
                    if time.time() - self.heartbeat_dict[pubID] > 90:
                        print('\n************************************\n')
                        print('Publisher dead: %s has dead.' % pubID)
                        print('\n************************************\n')
                        with open(self.log_file, 'a') as logfile:
                            logfile.write('\n************************************\n')
                            logfile.write('Publisher dead: %s has dead.\n' % pubID)
                            logfile.write('\n************************************\n')
                        self.update_pub_dict('shutoff', pubID, '', '')
                        del self.heartbeat_dict[pubID]

                        # Note: notify successor node node delete all information related this publisher
                        msg = 'update_replica' + '#' + 'shutoff' + '#' + 'src' + '#' + self.id + '#' + 'publisher' + '#' + pubID + '#'
                        with self.broker_lock:
                            try:
                                if self.port3 not in self.broker_sockets.keys() or self.successor_table[0] not in self.broker_sockets[self.port3].keys():
                                    self.broker_sockets.update({self.port3: {self.successor_table[0]: self.zmqhelper.csreq(self.successor_table[0], self.port3)}})
                                    self.broker_sockets[self.port3][self.successor_table[0]].RCVTIMEO = 40000
                                    time.sleep(random.randint(3, 6))
                                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                                else:
                                    self.broker_sockets[self.port3][self.successor_table[0]].send_string(msg)
                                self.broker_sockets[self.port3][self.successor_table[0]].recv_string()
                            except Exception as ex:
                                print('-------------------------\n')
                                print('Port3 timeout\n')
                                print(ex)
                                print('-------------------------\n')
                        print('\n************************************\n')
                        print('Notify successor broker node to update replica data.')
                        print('\n************************************\n')
                        break

            time.sleep(20)

    # Note: update publisher dictionary
    def update_pub_dict(self, update_typ, pubID, topic, publication):
        try:
            if update_typ == 'add_pub':
                # Assign an ownership strength to the registered publisher
                ownership_strength = random.randint(1, 100)
                if topic not in self.pub_dict.keys():
                    self.pub_dict.update({topic: {pubID: {'ownership_strength': ownership_strength, 'publications': []}}})
                elif pubID not in self.pub_dict[topic].keys():
                    self.pub_dict[topic].update({pubID: {'ownership_strength': ownership_strength, 'publications': []}})

            elif update_typ == 'add_publication':
                stored_publication = publication + '--' + str(time.time())
                self.pub_dict[topic][pubID]['publications'].append(stored_publication)

            elif update_typ == 'drop_topic':
                if topic in self.pub_dict.keys():
                    print('Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.' % (topic, pubID, pubID))
                    with open(self.log_file, 'a') as logfile:
                        logfile.write('\n************************************\n')
                        logfile.write('Broker filter feedback: drop topic %s for publisher %s failed, %s don\'t have this topic.\n' % (topic, pubID, pubID))
                        logfile.write('\n************************************\n')
                else:
                    del self.pub_dict[topic][pubID]
            elif update_typ == 'shutoff':
                for mytopic in self.pub_dict.keys():
                    if pubID in self.pub_dict[mytopic].keys():
                        del self.pub_dict[mytopic][pubID]
        except KeyError as ex:
            print('\n----------------------------------------\n')
            print('Error happened while updating publication dictionary...')
            print(ex)
            print('\n----------------------------------------\n')

    # Note: filter publisher ownership strength dictionary
    def filter_pub_ownership(self, pubID, topic):
        try:
            max_ownership_strength = 0
            with self.pub_dict_lock:
                for val in self.pub_dict[topic].items():
                    if val[1]['ownership_strength'] > max_ownership_strength:
                        max_ownership_strength = val[1]['ownership_strength']
                if self.pub_dict[topic][pubID]['ownership_strength'] == max_ownership_strength:
                    return pubID
                else:
                    return None
        except KeyError as ex:
            print('\n----------------------------------------\n')
            print('Error happened while filtering publication dictionary...')
            print(ex)
            print('\n----------------------------------------\n')
