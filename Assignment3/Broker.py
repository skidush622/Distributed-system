#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# FileName: Broker.py
#
# CS6381 Assignment3
# Group member: Peng Manyao, Li Yingqi, Zhou Minhui, Zhuangwei Kang
#

import random
import threading
import time
from ZMQHelper import ZMQHelper
from kazoo.client import KazooState
from kazoo.client import KazooClient

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
        self.log_file = './Output/' + self.myID + '.log'
        self.zk = KazooClient(zk_server)
        self.isLeader = False
        self.my_address = my_address
        
        print('\n************************************\n')
        print('Init MyBroker succeed.')
        print('\n************************************\n')
        with open(self.log_file, 'w') as logfile:
            logfile.write('Init Broker:\n')
            logfile.write('XSUB Port: ' + xsub_port + '\n')
            logfile.write('XPUB Port: ' + xpub_port + '\n')
            logfile.write('-------------------------------------------\n')
        self.init_zk()

    def init_zk(self):
        self.zk.start()
        while self.zk.state != KazooState.CONNECTED:
            pass
        print('Broker %s connected to ZooKeeper server.' % self.myID)

        # Create a Znode in ZooKeeper
        znode_path = './Brokers/' + self.myID
        self.zk.create(path=znode_path, value=b'', ephemeral=True, makepath=True)
        while self.zk.exists(znode_path) is False:
            pass
        print('Broker %s created a znode in ZooKeeper server.' % self.myID)

        # watch publishers znode
        pub_watch_path = './Publishers'

        @self.zk.ChildrenWatch(client=self.zk, path=pub_watch_path)
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
        leader_path = './Leader'
        if self.zk.exists(leader_path):
            # If the leader znode already exists, specify this broker as follower
            self.isLeader = False
            # followers start watching leader znode for potential election
            leader_monitor()
            # connect to leader using PULL socket type
            self.syncsocket = self.zmqhelper.sinkpull('5559')
            # TODO: listen sync message from leader and update data storage
            sync_data()
        
        else:
            # If the leader is not existing, create it and receive msg
            self.zk.create(leader_path, value=self.my_address, ephemeral=True, makepath=True)
            while self.zk.exists(path=leader_path) is False:
                pass
            print('Broker %s is the first leader.' % self.myID)
            self.isLeader = True
            self.syncsocket = self.zmqhelper.sourcepush(self.my_address,'5559')
            self.receive_msg()


    def leader_monitor(self):
        # Run this method in another thread, because the election.run() method will be blocked until it won
        election_path = './Brokers/'
        leader_path = './Leader'

        # watch leader znode
        @self.zk.DataWatch(client=self.zk, path=leader_path)
        def watch_leader(data, state):
            if state is None:
                election = self.zk.Election(self.zk, election_path, self.myID)
                election.run(self.win_election)


    def publisher_failed(self, children):
        '''
        :param children: current children list under Publishers Znode
        :return:
        '''
        # delete all entries of the pub which not in the current children list
        for key in self.data.keys():
            for pubID in self.data[key].keys():
                if pubID is not in children:
                    del self.data[key][pubID]
       
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
        if self.zk.exists(path=leader_path) is False:
            self.zk.create(leader_path, value=self.my_address, ephemeral=True, makepath=True)
        self.isLeader = True
        print('Broker %s became leader.' % self.myID)
        self.syncsocket = self.zmqhelper.sourcepush(self.my_address,'5559')
        self.receive_msg()
        
    # only leader call this method
    def receive_msg(self):
        '''
        Message type:
        1. publisher init
        2. publication
        3. subscriber request
        :return:
        '''
        while self.isLeader:
            # Store received data into self data storage
            # Send received data to subscribers
            # Send received data to followers using PUSH socket
            
            # receive message from publisher
            msg = self.xsubsocket.recv_string(0, 'utf-8')
            message = msg.split('#')
            msg_type = message[0]
            # publisher init
            if msg_type == 'pub_init' and message[1] not in self.data[message[2]].keys():
                pubID = message[1]
                topic = message[2]
                print('\n************************************\n')
                print('Init msg: %s init with topic %s' % (pubID, topic))
                print('\n************************************\n')
                with open(log_file, 'a') as logfile:
                    logfile.write('Init msg: %s init with topic %s\n' % (pubID, topic))
                # update storage
                self.update_data('add_pub', pubID, topic, '')
                # send msg to followers
                self.syncsocket.send_string('add_pub' + '#' + pubID + '#' + topic + '#')
        
            #  publication
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
                self.update_data('add_publication', pubID, topic, publication)
                # send msg to followers
                self.syncsocket.send_string('add_publication' + '#' + pubID + '#' + topic + '#' + publication + '#')
                # check if this pubID has the highest ownership
                if self.filter_pub_ownership(pubID, topic) is not None:
                    # send publication to subscribers using xpubsocket
                    publication = publication + '--' + str(time.time())
                    self.zmqhelper.xpub_send_msg(self.xpubsocket, topic, publication)

            # receive history request msg from subscribers
            msg = self.historysocket.recv_string(0, 'utf-8')
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'request_history_publication':
                with open(self.log_file, 'a') as log:
                    log.write('\n************************************\n')
                    log.write('Subscriber is requesting history publication.\n')
                    log.write('\n************************************\n')
                topic = message[1]
                history_count = int(message[2])
                # get pubID who has the highest ownership strength
                for pub in self.data[topic].keys():
                    target = self.filter_pub_ownership(pub, topic)
                    if target is not None:
                        break
                data = []
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
        while not self.isLeader:
            msg = self.syncsocket.recv_string()
            print('received msg from leader: %s' % msg)
            message = msg.split('#')
            msg_type = message[0]
            if msg_type == 'add_pub':
                pubID = message[1]
                topic = message[2]
                update_data(msg_type, pubID, topic)
            elif msg_type == 'add_publication':
                pubID = message[1]
                topic = message[2]
                publication = message[3]
                update_data(msg_type, pubID, topic, publication)
                    

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
            if self.data[topic][pubID]['ownership strength'] == max([pub['ownership strength'] for pub in data[topic].values()]):
                return pubID
            else:
                return None
        except Exception:
            return None
