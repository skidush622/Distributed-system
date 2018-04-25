#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Subscriber.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

from ZMQHelper import ZMQHelper
import time
import random
from kazoo.client import KazooState
from kazoo.client import KazooClient


class Subscriber:
    def __init__(self, zk_server, topic, history_count):
        self.leader_address = None
        self.topic = topic
        self.history_topic = topic + '-history'
        self.history_count = history_count if history_count > 0 else 0
        self.helper = ZMQHelper()
        self.myID = str(random.randint(1, 100))
        self.zk = KazooClient(zk_server)
        self.isConnected = False
        self.socket = None
        self.hisIPsocket = None
        self.init_zk()
    
    def init_zk(self):
        self.zk.start()
        if self.zk.state != KazooState.CONNECTED:
            pass
        print('Sub %s connected to ZooKeeper server.' % self.myID)

        # Create a Znode for this subscriber
        znode_path = './Subscribers/' + self.myID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is False:
            pass
        print('Sub %s created Znode in ZooKeeper server.' % self.myID)
        
        # register this sub with leader once leader created
        leader_path = './Leader'
        while self.zk.exists(leader_path) is None:
            pass
        data, state = self.zk.get(leader_path)
        self.leader_address = data.decode("utf-8")
        if self.history_count > 0:
            self.request_history()
        if self.register_sub():
            print('Sub %s connected with leader.' % self.myID)
            self.isConnected = True
        
        # set High-level exist watcher for leader znode
        @self.zk.DataWatch(client=self.zk, path=leader_path)
        def watch_leader(data, state):
            if state is None:
                self.isConnected = False
                print('Sub %s loses connection with old leader', % self.myID)
            elif self.isConnected is False:
                self.leader_address = data.decode("utf-8")
                self.socket = None
                if self.register_sub():
                    print('Sub %s reconnected with new leader', % self.myID)
                    self.isConnected = True
            
        print('Sub %s starts receiving publicaitons.' % self.myID)
        receive_publication()

    # only called when a sub join in
    def request_history(self):
        # Connected to new leader using REQ socket type, using 5558 as port number
        self.hisIPsocket = self.helper.csreq(self.leader_address, '5558')
        time.sleep(3)
        # Send history request message to leader
        msg = 'request_history_publication' + '#' + self.topic + '#' + str(self.history_count) + '#'
        self.hisIPsocket.send_string(msg)
        print('Send history request to broker...')
        with open(self.logfile_name, 'a') as log:
            log.write('\n*************************************************\n')
            log.write('Send history publications request to broker...')
            log.write('\n*************************************************\n')
        # receive history publication from leader broker, then store these history publication into log file
        histories = self.hisIPsocket.recv_string()
        print('Received histories: ' + histories)
        with open(self.logfile_name, 'a') as log:
        log.write('\n*************************************************\n')
        log.write('Received history publications from broker: ')
        log.write(histories)
        log.write('\n*************************************************\n')
        # parse msg
        histories = simplejson.loads(histories.split('#')[1])
        # write all history publications into logfile
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
    
    
    # register subscriber
    def register_sub(self):
        connect_str = 'tcp://' + self.leader_address + ':' + '5557'
        print('Connection info: %s' % connect_str)
        current = time.time()
        while time.time() - current < 3:
            self.socket = self.helper.connect_sub2broker(connect_str)
        if self.socket is None:
            print('Connection feedback: connected xpub socket failed.')
            self.isConnected = False
            return False
        else:
            print('Connection feedback: connected xpub socket succeed.')
            # Add a topic for SUB socket to filter publication
            self.add_sub_topic(self.topic)
            return True

    #  receive publications from leader
    def receive_publication(self):
        while True:
            if self.isConnected:
               received_pub = self.helper.sub_recieve_msg(self.socket)
               message = received_pub.split()
               received_msg = ' '.join(message[1:])
               received_msg = received_msg.split('--')
               time_stamp = float(received_msg[1])
               received_msg = received_msg[0]
               current = time.time()
               print('*************************************************\n'
                   'Receipt Info:\n'
                   'Publication: %s\n'
                   'Time Interval: %f\n' % (received_msg, abs(current - time_stamp)))
               logfile_name = './Output/' + self.myID + '-subscriber.log'
               with open(logfile_name, 'a') as log:
                   log.write('*************************************************\n')
                   log.write('Receipt Info:\n')
                   log.write('Receive: %s\n' % received_msg)
                   log.write('Time: %f\n' % abs(current - time_stamp))

    # add a subscription topic
    def add_sub_topic(self, topic):
        print('Add topic for subscriber.')
        self.helper.subscribe_topic(self.socket, topic)
