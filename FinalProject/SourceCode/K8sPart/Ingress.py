import zmq
import random
import threading
import simplejson
from kazoo.client import KazooClient
from kazoo.client import KazooState


class Ingress:
    def __init__(self, spout, my_address, zk_address):
        # Buffer format:
        '''
        {
            'state': [
                {
                    'state': $state,
                    'data': $data,
                    'time': $time,
                    'status': $status
                }
            ]
        }
        '''
        self.buffer = {}
        self.spout = spout
        self.zk_address = zk_address
        self.id = str(random.randint(1, 1000))
        self.my_address = my_address
        self.zk = None
        self.up_stream_socket = None
        self.down_stream_socket = []
        self.operators = []

        self.parent_path = '/' + spout + '/Ingress_operators/'
        self.leader_path = self.parent_path + '/leader'
        self.znode_path = self.parent_path + '/' + self.my_address
        self.operator_path = '/' + self.spout + '/operators/'

        self.init_zk()

    def init_zk(self):
        self.zk = KazooClient(self.zk_address)
        self.zk.start()
        while self.zk.state == KazooState.CONNECTED is False:
            pass
        print('Connected to ZooKeeper server.')

        if self.zk.exists(path=self.parent_path) is False:
            self.zk.create(path=self.parent_path, value=b'', ephemeral=False, makepath=True)
            while self.zk.exists(path=self.parent_path) is False:
                pass
            print('Create parent znode path for ingress operator znode.')

        self.zk.create(path=self.znode_path, value=b'', ephemeral=True)
        while self.zk.exists(self.znode_path) is False:
            pass
        print('Create zndoe for Ingress operator.')

        if self.zk.exists(self.leader_path) is False:
            self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True)
            while self.zk.exists(self.leader_path) is False:
                pass
            print('Create leader znode.')

        @self.zk.DataWatch(self.leader_path)
        def watch_leader(data, stat):
            if stat == KazooState.LOST:
                election = self.zk.Election(self.parent_path, self.id)
                election.run(win_election)

        @self.zk.ChildrenWatch(self.zk, self.operator_path)
        def watch_operators(children):
            for child in children:
                if child not in self.operators:
                    path = self.spout + '/operators/' + child
                    address = self.operator_path + self.zk.get(path=path)
                    threading.Thread(target=init_REQ, args=(address,)).start()

        def init_REQ(address):
            context = zmq.Context()
            socket = context.socket(zmq.REQ)
            socket.connect('tcp://' + address + ':2341')
            self.down_stream_socket.append(socket)

        def win_election():
            print('Yeah, I won the election.')
            self.zk.set(path=self.leader_path, value=self.my_address)

    def init_upstream_socket(self):
        context = zmq.Context()
        self.up_stream_socket = context.socket(zmq.REP)
        self.up_stream_socket.bind('tcp://*:2341')

    def recv_data(self):
        while True:
            msg = self.up_stream_socket.recv_string()
            self.up_stream_socket.send_sting('OK')
            print('Receive msg %s from data source.' % msg)
            msg = simplejson.loads(msg)
            msg.update({'status': 'recv'})
            state = msg['state']
            if state in self.buffer.keys():
                self.buffer[state].append(msg)
            else:
                self.buffer.update({state: msg})

    # TODO: 如何将数据并行发送，并且将buffer状态同步


