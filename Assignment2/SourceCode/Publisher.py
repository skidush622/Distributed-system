#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Publisher.py
#
# CS6381 Assignment2
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Publisher ---> Broker
# register    msg: 'pub_init' + '#' + pubID + '#' + init_topic + '#'
# publication msg: 'publication' + '#' + pubID + '#' + topic + '#' + msg
# drop topic  msg: 'drop_topic' + '#' + pubID + '#' + topic + '#'
# soft shutoff msg: 'shutoff' + '#' + pubID + '#'
# heartbeat   msg: 'pub_heartbeat' + '#' + pubID + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message formate: Broker ---> Publisher
# update IP address msg: 'UpdateIP' + '#' + new_ip + '#' + topic + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# NOTE: Ports on publisher
# 1. port1: used to connect to broker --- socket type: PUB/SUB >> 5556
# 2. port2: used to listen update IP msg from broker --- socket type: Client/Server >> 6002

# NOTE: Publisher may connect to multiple brokers to publish different topics

from ZMQHelper import ZMQHelper

import random
import time
import sys
import threading

class Publisher:
    # # # # # # # # # # # # # # # # # # # # # #
    # Publisher constructor method:
    # Parameters:
    # 1. address: initial broker IP address
    # 2. port1: used to connect to broker --- socket type: PUB/SUB >> 5556
    # 3. port2: used to listen update IP msg from broker  --- socket type: Client/Server >> 6002
    # 4. init_topic: initial topic of publisher
    # # # # # # # # # # # # # # # # # # # # # #
    def __init__(self, ip, init_address, port1, port2, init_topic):
        self.init_address = init_address
        self.port1 = port1
        self.init_topic = init_topic
        # use local IP address as publisher ID
        self.pubID = ip
        # Call ZeroMQ API
        self.zmqhelper = ZMQHelper()

        # 防止线程争夺socket
        self.lock = threading.Lock()

        # soft_shutoff_check is used to detect soft shutoff
        self.soft_shutoff_check = False
        # logfile name
        self.logfile_name = './Output/' + self.pubID + '-publisher.log'

        # port used to receive update IP address msg from broker
        self.port2 = port2

        self.flag = False
        self.flag_lock = threading.Lock()

        # socket dictionary
        # data formate: {$(topic): $(socket)}
        self.sockets = dict()

    # # # # # # # # # # # # # # # # # # # # # #
    # Register method
    # return True: register succeed
    # return Fale: register failed
    # # # # # # # # # # # # # # # # # # # # # #
    def register(self):
        update_addr_thr = threading.Thread(target=self.listen_update, args=())
        threading.Thread.setDaemon(update_addr_thr, False)
        send_pub_thr = threading.Thread(target=self.send_pub, args=(self.init_topic,))
        threading.Thread.setDaemon(send_pub_thr, False)

        if self.connect2broker(self.init_address, self.init_topic):
            update_addr_thr.start()
            send_pub_thr.start()

    # # # # # # # # # # # # # # # # # # # # # #
    # Connect publisher to  broker
    # return True: connection succeed
    # return Fale: connection failed
    # # # # # # # # # # # # # # # # # # # # # #
    def connect2broker(self, address, topic):
        with self.lock:
            connect_str = 'tcp://' + address + ':' + self.port1
            print('Connection info: %s' % connect_str)
            self.sockets.update({topic: self.zmqhelper.connect_pub2broker(connect_str)})
            self.sockets.get(topic).RCVTIMEO = 50000
            time.sleep(random.randint(3, 5))

            if self.sockets.get(topic) is None:
                print('Connection feedback: connected xsub socket failed.')
                with open(self.logfile_name, 'a') as log:
                    log.write('Publisher %s registered to broker failed.\n' % self.pubID)
                self.sockets.pop(topic)
                return False
            else:
                try:
                    init_str = 'pub_init' + '#' + self.pubID + '#' + topic + '#'
                    self.zmqhelper.pub_send_msg(self.sockets.get(topic), init_str)
                    self.sockets.get(topic).recv_string()
                    print('Connection feedback: connected xsub socket succeed.')
                    print('Connection feedback: %s initialized with initial topic %s succeed.' % (self.pubID, topic))

                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Register info:\n')
                        log.write('ID: %s\n' % self.pubID)
                        log.write('Initial topic: %s\n' % topic)
                        log.write('Connection Info: tcp://' + address + ':' + self.port1 + '\n')
                except Exception as ex:
                    print('\n*************************************************\n')
                    print(ex)
                    print('\n*************************************************\n')
                return True

    # # # # # # # # # # # # # # # # # # # # # #
    # Publish message
    # Parameters:
    # 1. topic
    # 2. message
    # # # # # # # # # # # # # # # # # # # # # #
    def send_pub(self, topic):
        file_path = './Input/' + topic + '.txt'
        pubs_list = self.get_publications(file_path)
        print('PUB ID:', self.pubID)
        i = 0
        while i < len(pubs_list):
            with self.flag_lock:
                if self.flag:
                    try:
                        with open(self.logfile_name, 'a') as logfile:
                            logfile.write('\n*************************************************\n')
                            logfile.write('Publish Info:\n')
                            logfile.write('Publish: %s\n' % pubs_list[i])
                            logfile.write('Time: %s\n' % str(time.time()))
                        send_str = 'publication' + '#' + self.pubID + '#' + topic + '#' + pubs_list[i]
                        with self.lock:
                            self.zmqhelper.pub_send_msg(self.sockets.get(topic), send_str)
                            self.sockets.get(topic).recv_string()
                        print('Publication: %s\n' % send_str)
                        i += 1

                        # send heartbeat msg to broker
                        send_str = 'pub_heartbeat' + '#' + self.pubID + '#'
                        with self.lock:
                            self.zmqhelper.pub_send_msg(self.sockets.get(topic), send_str)
                            self.sockets.get(topic).recv_string()
                        print('Send heartbeat to Broker\n')
                        with open(self.logfile_name, 'a') as log:
                            log.write('\n*************************************************\n')
                            log.write('Heartbeat info:\n')
                            log.write('Time: %s' % str(time.time()))
                    except Exception as ex:
                        print('\n*************************************************\n')
                        print(ex)
                        connect_str = 'tcp://' + self.init_address + ':' + self.port1
                        with self.lock:
                            self.sockets.get(topic).connect(connect_str)
                        print('\n*************************************************\n')
            time.sleep(random.randint(3, 8))

    # # # # # # # # # # # # # # # # # # # # # #
    # Drop topic: notify broker to delete all
    # publications releated to given topic
    # Parameter: topic
    # # # # # # # # # # # # # # # # # # # # # #
    def drop_topic(self, topic):
        send_str = 'drop_topic' + '#' + self.pubID + '#' + topic + '#'
        with self.lock:
            self.zmqhelper.pub_send_msg(self.sockets[topic], send_str)
            self.sockets.get(topic).recv_string()
        # delete correspond socket
        with self.lock:
            del self.sockets[topic]
        print('Drop topic: %s' % topic)
        try:
            with open(self.logfile_name, 'a') as log:
                log.write('\n*************************************************\n')
                log.write('Drop topic: %s\n' % topic)
        except IOError:
            print('Open or write file error.')

    # # # # # # # # # # # # # # # # # # # # # #
    # Soft shutoff publisher: the publisher won't send
    # publications to broker
    # # # # # # # # # # # # # # # # # # # # # #
    def softshutoff(self):
        send_str = 'shutoff' + '#' + self.pubID + '#'
        print('Shutoff')
        self.soft_shutoff_check = True
        # send soft shutoff msg to all brokers
        for s in self.sockets.values():
            with self.lock:
                self.zmqhelper.pub_send_msg(s, send_str)
                s.recv_string()
        try:
            with open(self.logfile_name, 'a') as log:
                log.write('\n*************************************************\n')
                log.write('Soft shutoff info:\n')
                log.write('Time: %s' % str(time.time()))
        except IOError:
            print('Open or write file error.')

    # Listen "Update IP address" msg from brokers
    def listen_update(self):
        mysocket = self.zmqhelper.csrecv(self.port2)
        if mysocket is None:
            print('Set up listening update socket for publisher failed')
        else:
            while True:
                msg = mysocket.recv_string()
                mysocket.send_string('OK')
                msg = msg.split('#')
                address = msg[1]
                topic = msg[2]
                # publisher don't need to update ip
                if msg[0] == 'start':
                    with self.flag_lock:
                        self.flag = True
                    print('\n---------------------------------------------------------\n')
                    print('Publisher received start sending msg...')
                    print('\n---------------------------------------------------------\n')
                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Start sending publications.\n')
                else:
                    with self.flag_lock:
                        self.flag = False
                    self.init_address = address
                    time.sleep(2)
                    print('\n---------------------------------------------------------\n')
                    print('Publisher received reconnect to new broker msg...')
                    print('\n---------------------------------------------------------\n')
                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Reconnect to new broker node.\n')
                    # connect to new broker using new IP address
                    self.connect2broker(str(address), str(topic))

    # Get publications from file
    def get_publications(self, file_path):
        try:
            with open(file_path, 'r') as file:
                pubs = file.readlines()
            for i in range(len(pubs)):
                pubs[i] = pubs[i][:-1]
            return pubs
        except IOError:
            print('Open or write file error.')
            return []
