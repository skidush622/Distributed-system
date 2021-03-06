#!/usr/bin/python3
# -*- coding: utf-8 -*-


import pymysql


def connectMysql(mysql_host, mysql_user, mysql_pwd, db=None):
	try:
		connection = pymysql.connect(host=mysql_host, port=3306, user=mysql_user, passwd=mysql_pwd)
		db_handler = connection.cursor()
		return connection, db_handler
	except Exception as e:
		print(e)


def closeConnection(connection):
	connection.close()


def createDB(db_handler, db_name):
	create_db = 'CREATE DATABASE IF NOT EXISTS ' + db_name
	db_handler.execute(create_db)


def useDB(db_handler, db_name):
	use_db = 'USE ' + db_name
	db_handler.execute(use_db)


def createTable(db_handler, db_name, table_name, columns, columns_type):
	useDB(db_handler, db_name)
	create_tb = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('
	for i in range(len(columns)):
		if i != len(columns) - 1:
			create_tb += columns[i] + ' ' + columns_type[i] + ','
		else:
			create_tb += columns[i] + ' ' + columns_type[i] + ')'
	db_handler.execute(create_tb)


def createTableAutoInc(db_handler, db_name, table_name, columns, columns_type):
	useDB(db_handler, db_name)
	create_tb = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' ('
	for i in range(len(columns)):
		if i != len(columns) - 1:
			if i == 0:
				create_tb += columns[i] + ' ' + columns_type[i] + ' NOT NULL PRIMARY KEY AUTO_INCREMENT,'
			else:
				create_tb += columns[i] + ' ' + columns_type[i] + ','
		else:
			create_tb += columns[i] + ' ' + columns_type[i] + ')'
	db_handler.execute(create_tb)


def add_primary_key(db_handler, db_connection, db_name, tb_name, pkey):
	useDB(db_handler, db_name)
	cmd = 'ALTER TABLE ' + tb_name + ' ADD PRIMARY KEY (' + pkey + ')'
	try:
		db_handler.execute(cmd)
		db_connection.commit()
	except:
		db_connection.rollback()


def insert_data(db_connection, db_handler, db_name, tb_name, values):
	useDB(db_handler, db_name)
	insert_db = 'INSERT INTO ' + tb_name + ' VALUES ' + '('
	for i in range(len(values)):
		if i != len(values) - 1:
			insert_db += '\'' + values[i] + '\','
		else:
			insert_db += '\'' + values[i] + '\')'
	db_handler.execute(insert_db)
	db_connection.commit()


def insert_data_operator(db_connection, db_handler, db_name, tb_name, values):
	useDB(db_handler, db_name)
	insert_db = 'INSERT INTO ' + tb_name + ' VALUES ' + '(' + str(values[0]) + ',' + '\'' + values[1] + '\',' + '\'' + values[2] + '\',' + str(values[3]) + ',' + str(values[4]) + ',' + str(values[5]) + ',' + str(values[6]) + ')'
	db_handler.execute(insert_db)
	db_connection.commit()


def insert_data_output(db_connection, db_handler, db_name, tb_name, values):
	useDB(db_handler, db_name)
	insert_db = 'INSERT INTO ' + tb_name + ' VALUES ' + '(' + str(values[0]) + ',' + '\'' + values[1] + '\',' + str(values[2]) + ',' + str(values[3]) + ',' + str(values[4]) + ',' + str(values[5]) + ',' + str(values[6]) + ',' + str(values[7]) + ',' + str(values[8]) + ',' + str(values[9]) + ')'
	db_handler.execute(insert_db)
	db_connection.commit()

def update_tb(db_handler, db_connection, db_name, tb_name, column_name, new_val, key_column_name, key_column_val):
	useDB(db_handler, db_name)
	update_tb = 'UPDATE ' + tb_name + ' SET ' + column_name + ' = \'' + new_val + '\' WHERE ' + key_column_name + ' = \'' + key_column_val + '\''

	db_handler.execute(update_tb)
	db_connection.commit()

def delete_row(db_handler, db_connection, db_name, tb_name, key_column_name, key_column_val):
	useDB(db_handler, db_name)
	delete = 'DELETE FROM ' + tb_name + ' WHERE ' + key_column_name + ' = ' + key_column_val
	try:
		db_handler.execute(delete)
		db_connection.commit()
	except:
		db_connection.rollback()


def get_first_row(db_handler, db_name, tb_name):
	useDB(db_handler, db_name)
	get_first = 'SELECT * FROM ' + tb_name + ' LIMIT 1'
	db_handler.execute(get_first)
	return db_handler.fetchall()


def count_rows(db_handler, db_name, tb_name, column):
	useDB(db_handler, db_name)
	count_cmd = 'SELECT COUNT(' + column + ') FROM ' + tb_name
	db_handler.execute(count_cmd)
	return db_handler.fetchall()[0][0]


def count_spec_rows(db_handler, db_name, tb_name, spec_column, expect_val):
	# useDB(db_handler, db_name)
	count_spec = 'SELECT COUNT(' + spec_column + ') FROM ' + tb_name + ' WHERE ' + spec_column + '=' + '\'' + expect_val + '\''
	db_handler.execute(count_spec)
	result = db_handler.fetchall()
	if result is None:
		return 0
	return result[0][0]

def update_rows(db_handler, db_connection, db_name, tb_name, spec_column, new_value, row_num):
	useDB(db_handler, db_name)
	update_cmd = 'UPDATE ' + tb_name + ' SET ' + spec_column + '=' + '\'' + new_value + '\'' + ' LIMIT ' + str(row_num)
	try:
		db_handler.execute(update_cmd)
		db_connection.commit()
	except:
		db_connection.rollback()


def query_first_N(db_handler, db_name, tb_name, n):
	useDB(db_handler, db_name)
	query_cmd = 'SELECT * FROM ' + tb_name + ' LIMIT ' + str(n)
	db_handler.execute(query_cmd)
	result = db_handler.fetchall()
	# print(result)
	if result is None:
		return None
	return result