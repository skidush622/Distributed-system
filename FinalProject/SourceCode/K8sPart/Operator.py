#!/usr/bin/python
# -*- coding: utf-8 -*-


import random
import zmq
import simplejson
import threading
import numpy as np
from kazoo.client import KazooClient
from kazoo.client import KazooState
from MySQLOP import MysqlOperations as mysqlop


class Operator:
	def __init__(self, zk_address, my_address, spout, operator_id, db_address, db_user, db_pwd):
		'''
		:param zk_address: ZooKeeper server address
		:param my_address: Address of current operator
		:param spout: ID of spout
		:param operator_id: Global ID of the operator
		'''
		self.db_address = db_address
		self.db_user = db_user
		self.db_pwd = db_pwd
		self.db_name = 'Spout_' + str(spout)
		self.tb_name = str('Operator_' + str(operator_id))
		self.columns = ['ID', 'State', 'Status', 'Sum', 'Mean', 'Max', 'Min']
		self.columns_type = ['INT(11)', 'CHAR(20)', 'CHAR(30)', 'CHAR(30)', 'CHAR(30)', 'CHAR(30)', 'CHAR(30)']
		self.db_connection, self.db_handler = self.init_db()

		self.id = 'op' + str(random.randint(1, 1000))
		self.my_address = my_address
		self.operator_id = operator_id
		self.parent_path = '/Spout--' + str(spout) + '/Operator--' + str(operator_id)
		self.znode_path = self.parent_path + '/' + str(self.id)
		self.leader_path = '/Spout--' + str(spout) + '/Operators/Leader--' + str(operator_id)
		self.egress_leader = '/Spout--' + str(spout) + '/Egress_leader'
		zk_address = zk_address + ':2181'
		self.zk = KazooClient(hosts=zk_address)
		self.isLeader = False
		self.egress_available = False

		self.up_stream_socket = None
		self.down_stream_socket = None
		self.lock = threading.Lock()
		self.flag = 0

		self.init_zk()

	def init_db(self):
		# Connect to Mysql server
		db_connection, db_handler = mysqlop.connectMysql(self.db_address, self.db_user, self.db_pwd, db=self.db_name)
		# Create Table
		mysqlop.createTableAutoInc(db_handler, self.db_name, self.tb_name, self.columns, self.columns_type)
		# Add primary key for the table
		# mysqlop.add_primary_key(db_handler, db_connection, self.db_name, self.tb_name, self.columns[0])
		return db_connection, db_handler

	def init_zk(self):
		self.zk.start()
		while (self.zk.state == KazooState.CONNECTED) is False:
			pass
		print('Connected to ZK server.')

		# Ensure parent path exists
		if self.zk.exists(path=self.parent_path) is None:
			self.zk.create(path=self.parent_path, value=b'', ephemeral=False, makepath=True)

		# Create Znode for this operator in ZK
		self.zk.create(path=self.znode_path, value=b'', ephemeral=True, makepath=True)
		self.zk.ensure_path(path=self.znode_path)

		# Watch the leader of Egress operator
		@self.zk.DataWatch(self.egress_leader)
		def watch_egress_leader(data, stat):
			if stat is not None:
				egress_leader_address = str(data)
				self.down_stream_socket = self.build_egress_socket(egress_leader_address)
				self.egress_available = True
			else:
				self.egress_available = False

		def win_election():
			# create leader node
			print('Yeah, I won the election.')
			self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True, makepath=True)
			self.up_stream_socket = self.build_socket()
			if self.up_stream_socket is not None:
				self.isLeader = True
				threading.Thread(target=self.recv_data, args=()).start()
				threading.Thread(target=self.distribute_data, args=()).start()

		# Watch leader node
		@self.zk.DataWatch(self.leader_path)
		def watch_leader(data, stat):
			if stat is None:
				# Old leader failed or no leader was initialized
				election = self.zk.Election(self.parent_path)
				election.run(win_election)

		while True:
			pass

	def build_socket(self):
		context = zmq.Context()
		socket = context.socket(zmq.REP)
		socket.bind('tcp://*:2341')
		return socket

	def build_egress_socket(self, address):
		context = zmq.Context()
		socket = context.socket(zmq.REQ)
		socket.connect('tcp://' + address + ':2341')
		return socket

	def recv_data(self):
		data_set = []
		while True:
			data = self.up_stream_socket.recv_string()
			print('Recv data :' + data)
			data = simplejson.loads(data)
			time = data['Time']
			self.up_stream_socket.send_string('Ack--' + time)
			state = data['State']
			data = int(data['Data'])
			data_set.append(data)
			if len(data_set) == 100:
				result = self.calculating(data_set)
				# 将数据存入数据库
				values = [state, 'Recv']
				values.extend(result.values())
				self.lock.acquire()
				mysqlop.insert_data(self.db_connection, self.db_handler, self.db_name, self.tb_name, values)
				data_set = []
				self.flag += 1
				self.lock.release()

	def distribute_data(self):
		while True:
			if self.flag > 20:
				self.lock.acquire()
				self.flag = 0
				# 读取前20 行数据
				data = mysqlop.query_first_N(self.db_handler, self.db_name, self.tb_name, 20)
				self.lock.release()
				temp_data = []
				for item in data:
					temp_data.append({'ID': item[0], 'State': item[1], 'Sum': item[3], 'Mean': item[4], 'Max': item[5], 'Min': item[6]})
				data = temp_data

				# 开始发送
				for __data in data:
					while self.egress_available is False:
						pass
					__data = simplejson.dumps(__data)
					self.down_stream_socket.send_string(__data)
					ack = self.down_stream_socket.recv_string()
					# Ack msg format: 'ack--' + $ID
					ack_id = ack.split('--')[1]
					self.lock.acquire()
					mysqlop.delete_row(self.db_handler, self.db_connection, self.db_name, self.tb_name, 'ID', ack_id)
					self.lock.release()

	def calculating(self, data_set):
		data_sum = str(np.sum(data_set))
		data_mean = str(np.mean(data_set))
		data_max = str(np.max(data_set))
		data_min = str(np.min(data_set))
		return {'sum': data_sum, 'mean': data_mean, 'max': data_max, 'min': data_min}

if __name__ == '__main__':
	op = Operator('172.17.0.3', '172.17.0.6', 1, 1, '172.17.0.2', 'root', 'kzw')