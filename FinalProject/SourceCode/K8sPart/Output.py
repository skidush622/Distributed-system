#!/usr/bin/python
# -*- coding: utf-8 -*-

import zmq
import simplejson
import argparse
from MySQLOP import MysqlOperations as mysqlop

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('-d', '--db_address', help='Database address')
	parser.add_argument('-n', '--db_name', default='result', help='Database name')
	parser.add_argument('-u', '--user', default='root', help='Mysql user name')
	parser.add_argument('-p', '--pwd', default='kzw', help='Mysql server password')
	args = parser.parse_args()
	db_address = args.db_address
	db_name = args.db_name
	db_user = args.user
	db_pwd = args.pwd

	# connect to mysql
	db_connection, db_handler = mysqlop.connectMysql(db_address, db_user, db_pwd)
	# create database
	mysqlop.createDB(db_handler, 'result')
	# table columns
	columns = ['ID', 'State', 'Sum', 'Mean', 'Max', 'Min']
	columns_type = ['INT(11)', 'CHAR(20)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)']

	def build_socket():
		context = zmq.Context()
		socket = context.socket(zmq.REP)
		socket.bind('tcp://*:2341')
		return socket
	exists_table = []
	output_socket = build_socket()
	while True:
		recv = output_socket.recv_string()
		recv = simplejson.loads(recv)
		id = recv['ID']
		output_socket.send_string('Ack--' + str(id))
		state = recv['State']
		data_sum = recv['Sum']
		data_mean = recv['Mean']
		data_max = recv['Max']
		data_min = recv['Min']
		tb_name = state
		if state not in exists_table:
			exists_table.append(state)
			# create table if not exists
			mysqlop.createTableAutoInc(db_handler, db_name, tb_name, columns, columns_type)
			# Add primary key
			mysqlop.add_primary_key(db_handler, db_connection, db_name, tb_name, columns[0])
		# store data into database
		values = [id, state, data_sum, data_mean, data_max, data_min]
		mysqlop.insert_data_output(db_connection, db_handler, db_name, tb_name, values)
		# Display data
		print('----------------------' + state + '----------------------')
		print('Sum: ' + data_sum)
		print('Mean: ' + data_mean)
		print('Max: ' + data_max)
		print('Min: ' + data_min)
		print('---------------------------------------------------------\n')

