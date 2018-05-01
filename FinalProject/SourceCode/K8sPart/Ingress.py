#!/usr/bin/python
# -*- coding: utf-8 -*-


import zmq
import random
import threading
import simplejson
import time
from kazoo.client import KazooClient
from kazoo.client import KazooState
from MySQLOP import MysqlOperations as mysqlop


class Ingress:
	def __init__(self, spout, my_address, zk_address, db_address, db_user, db_pwd):
		# Connect to MySQL server
		self.tb_name = 'IngressOperator'
		self.db_name = 'Spout_' + str(spout)
		self.columns = ['Time', 'State', 'Data', 'Status']
		self.columns_type = ['char(50)', 'char(20)', 'char(100)', 'char(10)']
		self.db_connection, self.db_handler = self.init_db(db_address, db_user, db_pwd)

		self.spout = spout
		self.zk_address = zk_address
		self.id = str(random.randint(1, 1000))
		self.my_address = my_address
		self.zk = KazooClient(hosts=self.zk_address)
		self.up_stream_socket = None
		self.down_stream_sockets = []
		self.operators = []

		self.parent_path = '/Spout--' + str(spout) + '/Ingress_operators'
		self.leader_path = '/Spout--' + str(spout) + '/Ingress_leader'
		self.znode_path = self.parent_path + '/Ingress--' + self.id
		self.operator_path = '/Spout--' + str(self.spout) + '/Operators'

		self.isLeader = False
		self.lock = threading.Lock()
		self.flag = 0

		self.init_zk()

	def init_db(self, db_address, db_user, db_pwd):
		# Connect to Mysql server
		db_connection, db_handler = mysqlop.connectMysql(db_address, db_user, db_pwd)
		# Create DB
		mysqlop.createDB(db_handler, self.db_name)
		# Create Table
		mysqlop.createTable(db_handler, self.db_name, self.tb_name, self.columns, self.columns_type)
		# Add primary key for the table
		mysqlop.add_primary_key(db_handler, db_connection, self.db_name, self.tb_name, self.columns[0])
		return db_connection, db_handler

	def init_zk(self):
		self.zk.start()
		while (self.zk.state == KazooState.CONNECTED) is False:
			pass
		print('Connected to ZooKeeper server.')

		# Create parent path in zookeeper
		if self.zk.exists(path=self.parent_path) is None:
			self.zk.create(path=self.parent_path, value=b'', ephemeral=False, makepath=True)
		self.zk.create(path=self.znode_path, value=b'', ephemeral=False, makepath=True)

		def win_election():
			print('Yeah, I won the election.')
			# Create leader znode
			self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True, makepath=True)
			self.init_upstream_socket()
			self.isLeader = True
			# Start receiving data from data source
			threading.Thread(target=self.recv_sourcedata, args=()).start()
			time.sleep(0.3)
			threading.Thread(target=self.distribute_data, args=()).start()

		@self.zk.DataWatch(self.leader_path)
		def watch_leader(data, stat):
			if stat is None:
				election = self.zk.Election(self.parent_path, self.id)
				election.run(win_election)

		def init_REQ(address):
			context = zmq.Context()
			socket = context.socket(zmq.REQ)
			socket.connect('tcp://' + address + ':2341')
			# socket.setsockopt(zmq.RCVTIMEO, 30000)
			self.down_stream_sockets.append(socket)

		@self.zk.ChildrenWatch(self.operator_path)
		def watch_operators(children):
			for i, op in enumerate(self.operators[:]):
				if op not in children:
					del self.operators[i]
			for child in children:
				if child not in self.operators:
					self.operators.append(child)
					print(child)
					path = '/Spout--' + str(self.spout) + '/Operators/' + child
					address = self.zk.get(path=path)[0]
					print('Address is: ' + address)
					init_REQ(address)

		while True:
			pass

	def init_upstream_socket(self):
		context = zmq.Context()
		self.up_stream_socket = context.socket(zmq.REP)
		self.up_stream_socket.bind('tcp://*:2341')

	def recv_sourcedata(self):
		while True:
			msg = self.up_stream_socket.recv_string()
			# print('Receive msg %s from data source.' % msg)
			msg = simplejson.loads(msg)
			temp = []
			temp.extend(msg)
			temp.append('Recv')
			print(temp)
			# Store data into DB
			self.lock.acquire()
			mysqlop.insert_data(self.db_connection, self.db_handler, self.db_name, self.tb_name, temp)
			self.flag += 1
			self.lock.release()
			self.up_stream_socket.send_string('Ack--OK')

	def distribute_data(self):
		while True:
			if self.flag == 10:
				self.flag = 0
				# 读取前100/row_count 行数据
				self.lock.acquire()
				data = mysqlop.query_first_N(self.db_handler, self.db_name, self.tb_name, 10)
				self.lock.release()
				print(data)
				temp_data = []
				for item in data:
					temp_data.append({'Time': item[0], 'State': item[1], 'Data': item[2]})
				data = temp_data

				# 开始并行发送
				def send_data(socket, my_data):
					for __data in my_data:
						__data = simplejson.dumps(__data)
						socket.send_string(__data)
						ack = socket.recv_string()
						# Ack msg format: 'ack--' + $time
						ack_time = ack.split('--')[1]
						print(ack)
						# Update DB
						self.lock.acquire()
						mysqlop.delete_row(self.db_handler, self.db_connection, self.db_name, self.tb_name, 'Time', ack_time)
						self.lock.release()

				socket_count = len(self.down_stream_sockets)
				while socket_count == 0:
					print('no socket')
				each_count = 10 / socket_count
				for i in range(socket_count):
					if i != socket_count - 1:
						threading.Thread(target=send_data, args=(
						self.down_stream_sockets[i], data[i * each_count:(i + 1) * each_count], )).start()
					else:
						threading.Thread(target=send_data,
										 args=(self.down_stream_sockets[i], data[i * each_count:],)).start()


if __name__ == '__main__':
	ingress = Ingress(1, '172.17.0.5', '172.17.0.3', '172.17.0.2', 'root', 'kzw')