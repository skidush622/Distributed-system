#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Subscriber.py
#
# CS6381 Assignment2
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Broker ---> Subscriber
# subscription        msg: 'publication' + '#' + publication + '--' + time
# update IP address   msg: 'UpdateIP' + '#' + new_ip + '#'
# history publication msg: 'history_publication' + '#' + [publication+'--'+time]
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# Message format: Subscriber ---> Broker
# request history publications msg: 'request_history_publication' + '#' + topic + '#' + history_count + '#'
# send IP to broker            msg: 'subscriber_IP' + '#' + ip + '#' + topic + '#'
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# NOTE: Each subscriber only can register one topic,
# which means each subscriber only connect to one broker.

from ZMQHelper import ZMQHelper
import time
import threading
import random
import socket
import simplejson

# NOTE: Subscriber needs to request history publications to broker and receive
# update IP address msg from broker, so Subscriber needs 2 CS sockets to satisfy
# demands.

class Subscriber:
    # # # # # # # # # # # # # # # # # # # # # #
    # Subscriber constructor method
    # Parameters:
    # 1. init_address: initial broker ip address when sub register to broker
    # 2. port1: broker xpub port --- socket type: PUB/SUB >> 5557
    # 3. port2: receive update IP address msg from broker --- socket type: Client/Server >> 6001
    # 4. port3: get history publication reply & send ip --- socket type: Client/Server >> 6003
    # 4. topic: initial subscribe topic
    # 5. history_count: history publications count
    # # # # # # # # # # # # # # # # # # # # # #
    def __init__(self, ip, init_address, port1, port2, port3, topic, history_count):
        self.init_address = init_address
        self.port1 = port1
        self.port2 = port2
        self.port3 = port3
        self.topic = str(topic)
        self.history_count = history_count
        # Call ZMQ API
        self.zmqhelper = ZMQHelper()

        # use local ip address as subscriber ID
        self.subID = str(ip)

        # log file name
        self.logfile_name = './Output/' + self.subID + '-subscriber.log'

        self.hisIPsocket = None

        self.flag = False
        self.flag_lock = threading.Lock()

        print('\n**************************************\n')
        print(ip + ' init with topic ' + topic)
        print('\n**************************************\n')

    # Handler method for subscriber
    def handler(self):
        # register subscriber
        self.register_sub(self.init_address)

        # listen update IP message && history publication message from broker
        listen_update_thr = threading.Thread(target=self.listen_updateIP, args = ())
        threading.Thread.setDaemon(listen_update_thr, True)
        listen_update_thr.start()

        # listen publication message from broker
        listen_pubs_thr = threading.Thread(target=self.receive_msg, args = ())
        threading.Thread.setDaemon(listen_pubs_thr, True)
        listen_pubs_thr.start()

        while True:
            pass

    # # # # # # # # # # # # # # # # # # # # # #
    # Register subscriber
    # return True: connection succeed
    # return False: connection failed
    # # # # # # # # # # # # # # # # # # # # # #
    def register_sub(self, address):
        connect_str = 'tcp://' + address + ':' + self.port1
        print('Connection info: %s' % connect_str)
        self.socket = self.zmqhelper.connect_sub2broker(connect_str)
        self.socket.RCVTIMEO = 30000
        time.sleep(random.randint(3, 5))
        # connection failed
        if self.socket is None:
            print('Connection feedback: connected xpub socket failed.')
            with open(self.logfile_name, 'w') as logfile:
                logfile.write('Subscriber %s registered to broker failed.\n' % self.subID)
            return False
        else:
            print('Connection feedback: connected xpub socket succeed.')
            with open(self.logfile_name, 'a') as log:
                log.write('\n*************************************************\n')
                log.write('Register info:\n')
                log.write('ID: %s\n' % self.subID)
                log.write('Topic: %s\n' % self.topic)
                log.write('History publications count: %s\n' % self.history_count)
            # send subscriber IP and receive history publications from broker
            self.sendIPgetHist(address)
            return True

    # Note: receive latest publications from brokers
    # # # # # # # # # # # # # # # # # # # # # # #
    def receive_msg(self):
        # self.zmqhelper.subscribe_topic(self.socket, self.topic)
        while True:
            try:
                received_pub = self.zmqhelper.sub_recieve_msg(self.socket)
                received_msg = received_pub.split('--')
                time_stamp = float(received_msg[1])
                received_msg = received_msg[0]
                if received_msg.split()[0] == self.topic:
                    print('\n*************************************************\n'
                          'Receipt Info:\n'
                          'Publication: %s\n'
                          'Time Interval: %f\n' % (received_msg, abs(time.time() - time_stamp)))
                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Receipt Info:\n')
                        log.write('Receive: %s\n' % received_msg)
                        log.write('Time: %f\n' % abs(time.time() - time_stamp))
                        log.write('\n*************************************************\n')
            except Exception as ex:
                print('\n*************************************************\n')
                print(ex)
                print('\n*************************************************\n')

    # Note: Listen Update IP address msg from broker
    def listen_updateIP(self):
        mysocket = self.zmqhelper.csrecv(self.port3)
        if socket is None:
            print('Set up listening update socket for publisher failed')
        else:
            while True:
                msg = mysocket.recv_string()
                print('Listening update IP port get new message.... %s ' % msg)
                mysocket.send_string('OK')
                # TODO: parse msg here
                msg = msg.split('#')
                new_ip = msg[1]
                # update initial IP address
                self.init_address = new_ip
                with self.flag_lock:
                    self.flag = True
                with open(self.logfile_name, 'a') as log:
                    log.write('\n*************************************************\n')
                    log.write('Reconnect to new broker...\n')
                    log.write('\n*************************************************\n')
                self.register_sub(new_ip)

    # Note: Send subscriber IP address & history request to broker
    # all history publications
    def sendIPgetHist(self, address):
        # Note: send subscriber IP to broker
        ip_msg = 'subscriber_IP' + '#' + self.subID + '#' + self.topic + '#'
        self.hisIPsocket = self.zmqhelper.csreq(address, self.port2)
        time.sleep(3)
        self.hisIPsocket.send_string(ip_msg)
        print('Send subscriber IP to broker....')
        with open(self.logfile_name, 'a') as log:
            log.write('\n*************************************************\n')
            log.write('Send subscriber IP to broker...')
            log.write('\n*************************************************\n')
        recved = str(self.hisIPsocket.recv_string())
        print('Received msg from broker: %s' % recved)
        if recved == 'OK':
            with self.flag_lock:
                if self.flag is False:
                    # TODO: send request history publications msg to broker
                    msg = 'request_history_publication' + '#' + self.topic + '#' + str(self.history_count) + '#'
                    self.hisIPsocket.send_string(msg)
                    print('Send history request to broker...')
                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Send history publications request to broker...')
                        log.write('\n*************************************************\n')
                    # Note: receive histroy publications from broker
                    # we only need to request history publication when subscriber joins the system
                    # so we only receive msg from broker one time.
                    histories = self.hisIPsocket.recv_string()
                    print('Received histories: ' + histories)
                    with open(self.logfile_name, 'a') as log:
                        log.write('\n*************************************************\n')
                        log.write('Received history publications from broker: ')
                        log.write(histories)
                        log.write('\n*************************************************\n')
                    # TODO: parse msg
                    histories = simplejson.loads(histories.split('#')[1])
                    # TODO:  write all history publications into logfile
                    for history in histories:
                        history = history.split('--')
                        print('\n*************************************************\n')
                        print('History Publication Receipt Info:\n')
                        print('Receive: %s\n' % history[0])
                        print('Time: %f\n' % abs(time.time() - float(history[1])))
                        print('\n*************************************************\n')
                        with open(self.logfile_name, 'a') as log:
                            log.write('\n*************************************************\n')
                            log.write('History Publication Receipt Info:\n')
                            log.write('Receive: %s\n' % history[0])
                            log.write('Time: %f\n' % abs(time.time() - float(history[1])))
                            log.write('\n*************************************************\n')
        # update IP address, then reconnect to new broker
        elif recved == 'Wait':
            print('Registered topic is unavailable now.........')
            with open(self.logfile_name, 'a') as log:
                log.write('\n*************************************************\n')
                log.write('Registered topic is unavailable now.........')
                log.write('\n*************************************************\n')
            return
        else:
            with open(self.logfile_name, 'a') as log:
                log.write('\n*************************************************\n')
                log.write('Reconnect to new broker: %s' % str(recved.split('#')[1]))
                log.write('\n*************************************************\n')
            self.register_sub(str(recved.split('#')[1]))
