#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import zmq
import simplejson
import argparse
import time
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
	columns = ['ID', 'State', 'Sum', 'Mean', 'Max', 'Min', 'Total_Sum', 'Total_Mean', 'Total_Max', 'Total_Min']
	columns_type = ['INT(11)', 'CHAR(20)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)', 'DOUBLE(30,4)']

	def build_socket():
		context = zmq.Context()
		socket = context.socket(zmq.REP)
		socket.bind('tcp://*:2341')
		return socket
	exists_table = []
	output_socket = build_socket()
	total_sum = 0
	total_mean = 0
	total_max = -sys.maxsize-1
	total_min = sys.maxsize
	my_id = 0
	while True:
		recv = output_socket.recv_string()
		recv = simplejson.loads(recv)
		my_id += 1
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
		total_sum += data_sum
		total_mean = (total_mean + data_mean) / 2
		total_max = max(total_max, data_max)
		total_min = min(total_min, data_min)
		# store data into database
		values = [my_id, state, data_sum, data_mean, data_max, data_min, total_sum, total_mean, total_max, total_min]
		mysqlop.insert_data_output(db_connection, db_handler, db_name, tb_name, values)
		# Display data
		print('\n----------------------' + state + '----------------------')
		print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
		print('Sum: ' + str(data_sum))
		print('Mean: ' + str(data_mean))
		print('Max: ' + str(data_max))
		print('Min: ' + str(data_min))
		print('Total sum:' + str(total_sum))
		print('Total mean:' + str(total_mean))
		print('Total max:' + str(total_max))
		print('Total min:' + str(total_min))


