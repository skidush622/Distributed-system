import time
import sys
import threading
from Publisher import Publisher

# Register publisher
def register(address, port, init_topic):
	publisher = Publisher(address, port, init_topic)
	publisher.register_handler()
	logfile_name = './Output/' + publisher.myID + '-publisher.log'
	with open(logfile_name, 'w') as log:
		log.write('ID: %s \n' % publisher.myID)
		log.write('Init Topic: %s\n' % publisher.init_topic)
		log.write('Connection Info: tcp://' + address + ':' + port + '\n')
	return publisher

# Send publications
def publish(publisher, topic, pubs_list, logfile_name):
	try:
		with open(logfile_name, 'a') as logfile:
			for pub in pubs_list:
				logfile.write('*************************************************\n')
				logfile.write('Publish Info:\n')
				logfile.write('Publish: %s\n' % pub)
				logfile.write('Time: %s\n' % str(time.time()))
				publisher.send_pub(topic, pub)
				time.sleep(1)
	except IOError:
		print('Open or write file error.')

# Drop a topic
def drop_topic(publisher, topic, logfile_name):
	publisher.drop_topic(topic)
	try:
		with open(logfile_name, 'a') as logfile:
			logfile.write('*************************************************\n')
			logfile.write('Drop topic: %s\n' % topic)
	except IOError:
		print('Open or write file error.')

# Shutoff publisher
def shutoff(publisher, logfile_name):
	publisher.shutoff()
	try:
		with open(logfile_name, 'a') as logfile:
			logfile.write('*************************************************\n')
			logfile.write('Shutoff: %s' % publisher.myID)
		return None
	except IOError:
		print('Open or write file error.')

# Get publications from file
def get_publications(file_path):
	try:
		with open(file_path, 'r') as file:
			pubs = file.readlines()
		for i in range(len(pubs)):
			pubs[i] = pubs[i][:-1]
		return pubs
	except IOError:
		print('Open or write file error.')
		return []
