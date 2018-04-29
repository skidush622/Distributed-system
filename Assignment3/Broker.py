#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Broker.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import simplejson
import random
import threading
import time
from ZMQHelper import ZMQHelper
from kazoo.client import KazooState
from kazoo.client import KazooClient
import logging
logging.basicConfig()

class Broker:

    def __init__(self, zk_server, my_address, xsub_port, xpub_port):
        '''
        :param zk_server: IP address of ZooKeeper Server
        :param my_address: IP address of current broker
        :param xsub_port: 5556
        :param xpub_port: 5557
        :sub_history_port: 5558
        :sync_with_leader_port:5559
        '''
        self.zmqhelper = ZMQHelper()
        '''
           {$(topic): {
                   $(pubID): {
                       'publications' : [$(publication)]
                       'ownership strength': $(ownership_strength)
                   }
               }
           }
        '''
        self.data = {}
        self.xsubsocket, self.xpubsocket = self.zmqhelper.prepare_broker(xsub_port, xpub_port)
        self.syncsocket = None
        self.historysocket = self.zmqhelper.csrecv('5558')
        self.myID = str(random.randint(1, 1000))
        self.log_file = './Output/Broker' + self.myID + '.log'
        zk_server = zk_server + ':2181'
        self.zk = KazooClient(hosts=zk_server)
        self.isLeader = False
        self.my_address = my_address
        self.count = 1

        print('\n************************************\n')
        print('Init MyBroker % s succeed.' % self.my_address)
        print('\n************************************\n')
        with open(self.log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')
        self.init_zk()

    def init_zk(self):
        if self.zk.state != KazooState.CONNECTED:
            self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Broker %s connected to ZooKeeper server.' % self.myID)

        if self.zk.exists('/Brokers') is None:
            self.zk.create(path='/Brokers', value=b'', ephemeral=False, makepath=True)
        flag = False
        while self.zk.exists('/Brokers') is None:
            pass
        flag = True
        if flag:
            print('Create Znode Brokers.')

        # Create a Znode in ZooKeeper
        znode_path = '/Brokers/' + self.myID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is None:
            pass
        print('Broker %s created a znode in ZooKeeper server.' % self.myID)

        # watch publishers znode
        pub_watch_path = '/Publishers'
        if self.zk.exists(pub_watch_path) is None:
            self.zk.create(path=pub_watch_path, value=b'', ephemeral=False, makepath=True)
        flag = False
        while self.zk.exists(pub_watch_path) is None:
            pass
        flag = True
        if flag:
            print('Create Publishers znode.')

        @self.zk.ChildrenWatch(path=pub_watch_path)
        def watch_publishers(children):
            self.publisher_failed(children)

        '''
        # watch subscriber znode
        sub_watch_path = './Subscribers'

        @self.zk.ChildrenWatch(client=self.zk, path=sub_watch_path)
        def watch_subscribers(children):
            self.subscriber_failed(children)
        '''

        # check if the leader has exists
        leader_path = '/Leader'
        if self.zk.exists(leader_path):
            # If the leader znode already exists, specify this broker as follower
            self.isLeader = False
            print('Broker %s is follower.' % self.myID)
            # followers start watching leader znode for potential election
            self.leader_monitor()

            # socket for follower to receive msg from leader
            leader_address = str(self.zk.get(leader_path)[0])
            print(leader_address)
            self.syncsocket = self.zmqhelper.sinkpull(leader_address, '5559')
            if self.syncsocket != None:
                print('follower: syncsocket ok')
            # NOTE: listen sync message from leader and update data storage
            self.sync_data()
        else:
            # If the leader is not existing, create it and receive msg
            self.zk.create(leader_path, value=self.my_address, ephemeral=True, makepath=True)
            while self.zk.exists(path=leader_path) is None:
                pass
            print('Broker %s is the first leader.' % self.myID)
            self.isLeader = True
            # socket for leader to send sync request to followers
            self.syncsocket = self.zmqhelper.sourcepush('5559')
            if self.syncsocket != None:
                print('leader: syncsocket ok')
                
        subscriber_thr = threading.Thread(target=self.receive_hisreq, args= ())
        threading.Thread.setDaemon(subscriber_thr, True)
        subscriber_thr.start()

        recv_thr = threading.Thread(target=self.receive_msg, args=())
        threading.Thread.setDaemon(recv_thr, True)
        recv_thr.start()

    def leader_monitor(self):
        # Run this method in another thread, because the election.run() method will be blocked until it won
        election_path = '/Brokers/'
        leader_path = '/Leader'

        # watch leader znode
        @self.zk.DataWatch(path=leader_path)
        def watch_leader(data, state):
            if self.zk.exists(path=leader_path) is None:
                time.sleep(random.randint(0, 3))
                election = self.zk.Election(election_path, self.myID)
                election.run(self.win_election)

    def publisher_failed(self, children):
        '''
        :param children: current children list under Publishers Znode
        :return:
        '''
        # delete all entries of the pub which not in the current children list
        if self.count != 1:
           for key in self.data.keys():
               for pubID in self.data[key].keys():
                   if pubID not in children:
                       del self.data[key][pubID]
                       print('delete publisher %s from topic %s\n' % (pubID, key))
        else:
            self.count = 0


    '''
    def subscriber_failed(self, children):

        :param children: current children list under Subscribers Znode
        :return:

        # TODO: Check which subscriber in data storage has failed,
        # if you get one, delete the data related to this subscriber
        pass
    '''

    # win the election, start receiving msg from publisher
    def win_election(self):
        leader_path = '/Leader'
        if self.zk.exists(path=leader_path) is None:
            self.zk.create(leader_path, value=self.my_address, ephemeral=True, makepath=True)
        while self.zk.exists(path=leader_path) is None:
            pass

        self.isLeader = True
        print('Broker %s became new leader' % self.myID)
        # self.syncsocket = None
        self.syncsocket = self.zmqhelper.sourcepush('5559')
        if self.syncsocket != None:
            print('Broker %s started receive msg' % self.myID)

    # only leader call this method
    def receive_msg(self):
        '''
        Message type:
        1. publisher init
        2. publication
        3. subscriber request
        :return:
        '''
        while self.isLeader is False:
            pass
        while True:
            # Store received data into self data storage
            # Send received data to subscribers
            # Send received data to followers using PUSH socket

            # receive message from publisher
            print('Enter resciving msg')
            msg = self.xsubsocket.recv_string()
            print(msg)
            message = msg.split('#')
            msg_type = message[0]
            # publisher init
            if msg_type == 'pub_init':
                pubID = message[1]
                topic = message[2]
                print('\n************************************\n')
                print('Init msg: %s init with topic %s' % (pubID, topic))
                print('\n************************************\n')
                with open(self.log_file, 'a') as logfile:
                    logfile.write('Init msg: %s init with topic %s\n' % (pubID, topic))
                # update storage
                self.update_data('add_pub', pubID, topic, '')
                # send msg to followers
                #self.syncsocket.send_string('add_pub' + '#' + pubID + '#' + topic + '#')

            #  publication
            elif msg_type == 'publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                print('\n************************************\n')
                print('Publication: %s published %s with topic %s' % (pubID, publication, topic))
                print('\n************************************\n')
                with open(self.log_file, 'a') as logfile:
                    logfile.write('Publication: %s published %s with topic %s\n' % (pubID, publication, topic))
                # update storage
                self.update_data('add_pub', pubID, topic, '')
                self.update_data('add_publication', pubID, topic, publication)
                # send msg to followers
                self.syncsocket.send_string('add_publication' + '#' + pubID + '#' + topic + '#' + publication + '#')
<<<<<<< HEAD
=======
                self.syncsocket.recv_string()
                print('debug2')
>>>>>>> abbc67e2a1e84291d49aa00ffccf92eea46b4aed
                # check if this pubID has the highest ownership
                if self.filter_pub_ownership(pubID, topic) is not None:
                    # send publication to subscribers using xpubsocket
                    print('sending to sub')
                    publication = publication + '--' + str(time.time())
                    self.zmqhelper.xpub_send_msg(self.xpubsocket, topic, publication)

    def receive_hisreq(self):
        while True:
        # receive history request msg from subscribers
            if self.historysocket is None:
               print('historysocket is none')
            msg = self.historysocket.recv_string(0, 'utf-8')
            print(msg)
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'request_history_publication':
               with open(self.log_file, 'a') as log:
                  log.write('\n************************************\n')
                  log.write('Subscriber is requesting history publication.\n')
                  log.write('\n************************************\n')
                  topic = str(message[1])
                  history_count = int(message[2])
                  i = 0
                  data = []
                  # get pubID who has the highest ownership strength
                  if topic in self.data.keys():
                     for pub in self.data[topic].keys():
                         target = self.filter_pub_ownership(pub, topic)
                         if target is not None:
                             break
                     i = len(self.data[topic][target]['publications'])
                     if i <= history_count:
                         data.extend(self.data[topic][target]['publications'][:])
                     else:
                         data.extend(self.data[topic][target]['publications'][-history_count:])
                  msg = 'history_publication' + '#' + simplejson.dumps(data)
                  self.historysocket.send_string(msg)
                  print('\n************************************\n')
                  print('Send history publications to subscriber...')
                  print(msg)
                  print('\n************************************\n')
                  with open(self.log_file, 'a') as log:
                      log.write('\n************************************\n')
                      log.write('Send history publication to subscriber.\n')
                      log.write('\n************************************\n')


    def sync_data(self):
        '''
        Receive sync msg from leader if this broker is a follower
        :return:
        '''
        # Use a while loop to receive sync msg from leader
        print('start sync with leader')
        while self.isLeader is False:
            try:
                msg = self.syncsocket.recv_string()
            except Exception as e:
                print('Sync data timeout')
                continue
            print('\n************************************\n')
            print('received sync msg from leader')
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'add_pub':
                pubID = message[1]
                topic = message[2]
                self.update_data(msg_type, pubID, topic, '')
            elif msg_type == 'add_publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                self.update_data('add_pub', pubID, topic, '')
                self.update_data(msg_type, pubID, topic, publication)
                print('sync with topic %s pub %s' % (topic, pubID))
                print('\n************************************\n')

    def update_data(self, update_typ, pubID, topic, publication):
        '''
        :param update_typ:
                    1. New publisher registered
                    2. Received new publication from publisher
        :param pubID:
        :param topic:
        :param publication:
        :return:
        '''
        try:
            if update_typ == 'add_pub':
                # Assign an ownership strength to the registered publisher
                ownership_strength = random.randint(1, 100)
                if topic not in self.data.keys():
                    self.data.update({topic: {pubID: {'publications': [], 'ownership strength': ownership_strength}}})
                elif pubID not in self.data[topic].keys():
                    self.data[topic].update({pubID: {'publications': [], 'ownership strength': ownership_strength}})
            elif update_typ == 'add_publication':
                stored_publication = publication + '--' + str(time.time())
                self.data[topic][pubID]['publications'].append(stored_publication)


        except KeyError as ex:
            print('\n----------------------------------------\n')
            print('Error happened while updating publication dictionary...')
            print(ex)
            print('\n----------------------------------------\n')

    # To examine if this pubID has the highest ownership
    #
    # argument: current publisher & topic
    # return publisher ID or None
    #
    def filter_pub_ownership(self, pubID, topic):
        try:
            if self.data[topic][pubID]['ownership strength'] == max([pub['ownership strength'] for pub in self.data[topic].values()]):
                return pubID
            else:
                return None
        except Exception:
            return None
