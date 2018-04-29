import zmq
import random
import threading
import simplejson
from kazoo.client import KazooClient
from kazoo.client import KazooState
from MySQLOP import MysqlOperations as mysqlop


class Ingress:
    def __init__(self, spout, my_address, zk_address, db_address, db_user, db_pwd):
        # Connect to MySQL server
        self.tb_name = 'IngressOperator'
        self.db_name = 'Spout--' + spout
        self.columns = ['Time', 'State', 'Data', 'Status']
        self.columns_type = ['char(50)', 'char(20)', 'char(100)', 'char(10)']
        self.db_connection, self.db_handler = self.init_db(db_address, db_user, db_pwd)
        
        self.spout = spout
        self.zk_address = zk_address
        self.id = str(random.randint(1, 1000))
        self.my_address = my_address
        self.zk = None
        self.up_stream_socket = None
        self.down_stream_sockets = []
        self.operators = []

        self.parent_path = '/' + spout + '/Ingress_operators/'
        self.leader_path = self.parent_path + '/leader'
        self.znode_path = self.parent_path + '/' + self.my_address
        self.operator_path = '/' + self.spout + '/operators/'

        self.init_zk()

    def init_db(self, db_address, db_user, db_pwd):
        # Connect to Mysql server
        db_connection, db_handler = mysqlop.connectMysql(db_address, db_user, db_pwd)
        # Create DB
        mysqlop.createDB(db_handler, db_name)
        # Create Table
        mysqlop.createTable(db_handler, self.db_name, self.tb_name, self.columns, self.columns_type)
        # Add primary key for the table
        mysqlop.add_primary_key(db_handler, db_connection, self.db_name, self.tb_name, self.columns[0])
        return db_connection, db_handler

    def init_zk(self):
        self.zk = KazooClient(hosts=self.zk_address)
        self.zk.start()
        while (self.zk.state == KazooState.CONNECTED) is False:
            pass
        print('Connected to ZooKeeper server.')

        # Create parent path in zookeeper
        if self.zk.exists(path=self.parent_path) is None:
            self.zk.create(path=self.parent_path, value=b'', ephemeral=False, makepath=True)
            while self.zk.exists(path=self.parent_path) is None:
                pass
            print('Create parent znode path for ingress operator znode.')

        self.zk.create(path=self.znode_path, value=b'', ephemeral=True)
        while self.zk.exists(self.znode_path) is None:
            pass
        print('Create zndoe for Ingress operator.')

        if self.zk.exists(self.leader_path) is None:
            self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True)
            while self.zk.exists(self.leader_path) is None:
                pass
            print('Create leader znode.')

        @self.zk.DataWatch(self.leader_path)
        def watch_leader(data, stat):
            if stat is None:
                election = self.zk.Election(self.parent_path, self.id)
                election.run(win_election)

        # Check if operators path exists
        flag = False
        if self.zk.exists(self.operator_path) is None:
            self.zk.create(path=self.operator_path, value=b'', ephemeral=False, makepath=True)
        while self.zk.exists(self.operator_path) is None:
            flag = True
        if flag:
            print('Create Znode Operators in ZK.')

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
            # Create leader znode

            if self.zk.exists(self.leader_path) is None:
            self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True)
            while self.zk.exists(self.leader_path) is None:
                pass
            print('Create leader znode.')

            self.init_upstream_socket()
            self.zk.set(path=self.leader_path, value=self.my_address)
            # Start receiving data from data source
            threading.Thread(target=self.recv_sourcedata, args=()).start()
            time.sleep(2)
            threading.Thread(target=self.distribute_data, args=()).start()

    def init_upstream_socket(self):
        context = zmq.Context()
        self.up_stream_socket = context.socket(zmq.REP)
        self.up_stream_socket.bind('tcp://*:2341')

    def recv_sourcedata(self):
        while True:
            msg = self.up_stream_socket.recv_string()
            self.up_stream_socket.send_sting('OK')
            print('Receive msg %s from data source.' % msg)
            msg = simplejson.loads(msg)
            msg.update({'Status': 'Recv'})
            # Store data into DB
            vals = msg.values()
            vals = [str(val) for val in vals]
            mysqlop.insert_data(self.db_connection, self.db_handler, self.db_name, self.tb_name, vals)

    def distribute_data(self):
        while True:
            data = mysqlop.get_first_row(self.db_handler, self.db_name, self.tb_name)
            if data not None and data[2] == 'Recv':
                time = data[0]
                data = data[1]
                op_num = len(self.down_stream_sockets)
                socket = self.down_stream_sockets[random.randint(0, op_num-1)]
                socket.send_sting(data)
                # Update data status in DB
                mysqlop.update_tb(self.db_handler, self.connection, self.db_name, self.tb_name, 'Status', 'Sending', 'Time', time)
                msg = socket.recv_string()
                # Remove this line from DB
                mysqlop.delete_row(self.db_handler, self.db_connection, self.db_name, self.tb_name, 'Time', time)
            