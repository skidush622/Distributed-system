#!/usr/bin/python
# -*- coding: utf-8 -*-


import random
import zmq
import simplejson
import threading
import argparse
import numpy as np
import time as tm
from kazoo.client import KazooClient
from kazoo.client import KazooState
from MySQLOP import MysqlOperations as mysqlop


class Egress:
	def __init__(self, zk_address, my_address, output_address, spout, db_address, db_user, db_pwd):
		'''
		:param zk_address: ZooKeeper server address
		:param my_address: Egress operator address
		:param output_address: End server address
		:param spout: Spout number
		:param db_address: Mysql server address
		:param db_user: Mysql user
		:param db_pwd: Mysql password
		'''
		self.db_address = db_address
		self.db_user = db_user
		self.db_pwd = db_pwd
		self.db_name = 'Spout_' + str(spout)
		self.tb_name = 'EgressOperator'
		self.columns = ['ID', 'State', 'Status', 'Sum', 'Mean', 'Max', 'Min']
		self.columns_type = ['INT(11)', 'CHAR(20)', 'CHAR(20)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)']

		self.db_connection, self.db_handler = self.init_db()
		self.output_address = output_address

		self.spout = spout
		self.zk_address = zk_address
		self.id = str(random.randint(1, 1000))
		self.my_address = my_address
		self.zk = KazooClient(hosts=zk_address)
		self.root = 'Spout--' + str(spout)
		self.parent_path = '/Spout--' + str(spout) + '/Egress_operators'
		self.leader_path = '/Spout--' + str(spout) + '/Egress_leader'
		self.znode_path = self.parent_path + '/Egress--' + self.id

		self.isLeader = False

		self.up_stream_socket = None
		self.down_stream_socket = None
		self.lock = threading.Lock()
		self.flag = 0

		self.init_zk()

	def init_db(self):
		# Connect to Mysql server
		db_connection, db_handler = mysqlop.connectMysql(self.db_address, self.db_user, self.db_pwd)
		# Create Table
		mysqlop.createTableAutoInc(db_handler, self.db_name, self.tb_name, self.columns, self.columns_type)
		# Add primary key for the table
		mysqlop.add_primary_key(db_handler, db_connection, self.db_name, self.tb_name, self.columns[0])
		return db_connection, db_handler

	def init_zk(self):
		self.zk.start()
		while (self.zk.state == KazooState.CONNECTED) is False:
			pass
		print('Connected to ZK server.')

		# Ensure root path exists
		if self.zk.exists(path=self.root) is None:
			self.zk.create(path=self.root, value=b'', ephemeral=False, makepath=True)

		# Ensure parent path exists
		if self.zk.exists(path=self.parent_path) is None:
			self.zk.create(path=self.parent_path, value=b'', ephemeral=False, makepath=True)

		# Create Znode for this operator in ZK
		self.zk.create(path=self.znode_path, value=b'', ephemeral=True, makepath=True)
		self.zk.ensure_path(path=self.znode_path)

		def win_election():
			print('Yeah, I won the election.')
			self.isLeader = True
			if self.zk.exists(path=self.leader_path) is None:
				self.zk.create(path=self.leader_path, value=self.my_address, ephemeral=True, makepath=True)
			else:
				self.zk.set(path=self.leader_path, value=self.my_address)
			self.up_stream_socket = self.build_upstream_socket()
			self.down_stream_socket = self.build_downstream_socket(str(self.output_address))
			threading.Thread(target=self.recv_data, args=()).start()
			threading.Thread(target=self.send_data, args=()).start()

		# Watch Leader node
		@self.zk.DataWatch(self.leader_path)
		def watch_egress_leader(data, state):
			if state is None:
				print('Suggest election..')
				election = self.zk.Election(self.parent_path)
				election.run(win_election)

		while True:
			pass

	def build_upstream_socket(self):
		context = zmq.Context()
		socket = context.socket(zmq.REP)
		socket.bind('tcp://*:2341')
		return socket

	def build_downstream_socket(self, address):
		context = zmq.Context()
		socket = context.socket(zmq.REQ)
		socket.connect('tcp://' + address + ':2341')
		return socket

	def recv_data(self):
		sum_set = []
		mean_set = []
		max_set = []
		min_set = []
		myid = 0
		while True:
			data = self.up_stream_socket.recv_string()
			data = simplejson.loads(data)
			# print(data)
			id = data['ID']
			self.up_stream_socket.send_string('Ack--' + str(id))
			state = data['State']
			sum_set.append(data['Sum'])
			print(sum_set)
			mean_set.append(data['Mean'])
			max_set.append(data['Max'])
			min_set.append(data['Min'])
			if len(sum_set) == 3:
				myid += 1
				data_sum = np.sum(sum_set)
				data_max = np.max(max_set)
				data_min = np.min(min_set)
				data_mean = np.mean(mean_set)
				# 将数据存入数据库
				values = [id, state, 'Recv']
				values.extend([data_sum, data_mean, data_max, data_min])
				# print(values)
				self.lock.acquire()
				mysqlop.insert_data_operator(self.db_connection, self.db_handler, self.db_name, self.tb_name, values)
				sum_set = []
				mean_set = []
				max_set = []
				min_set = []
				self.flag += 1
				self.lock.release()
			tm.sleep(0.01)

	def send_data(self):
		while True:
			if self.flag == 2:
				self.lock.acquire()
				self.flag = 0
				# 读取前5/row_count 行数据
				data = mysqlop.query_first_N(self.db_handler, self.db_name, self.tb_name, 2)
				self.lock.release()
				temp_data = []
				for item in data:
					temp_data.append({'ID': item[0], 'State': item[1], 'Sum': item[3], 'Mean': item[4], 'Max': item[5], 'Min': item[6]})
				data = temp_data

				# 开始发送
				for __data in data:
					__data = simplejson.dumps(__data)
					self.down_stream_socket.send_string(__data)
					ack = self.down_stream_socket.recv_string()
					# Ack msg format: 'ack--' + $ID
					ack_id = ack.split('--')[1]
					self.lock.acquire()
					mysqlop.delete_row(self.db_handler, self.db_connection, self.db_name, self.tb_name, 'ID', ack_id)
					self.lock.release()


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-z', '--zk_address', type=str, help='ZooKeeper server address')
	parser.add_argument('-a', '--address', type=str, help='IP address of this operator')
	parser.add_argument('-s', '--spout', type=int, default=1, help='Spout number')
	parser.add_argument('-o', '--output_address', type=str, help='IP address of output operator')
	parser.add_argument('-d', '--db_address', type=str, help='IP address of mysql server')
	parser.add_argument('-u', '--user', type=str, default='root', help='User name of mysql server')
	parser.add_argument('-p', '--pwd', type=str, default='kzw', help='Password of mysql server')
	args = parser.parse_args()
	zk_address = args.zk_address
	address = args.address
	spout = args.spout
	output_address = args.output_address
	db_address = args.db_address
	user = args.user
	pwd = args.pwd
	Egress(zk_address, address, output_address, spout, db_address, user, pwd)