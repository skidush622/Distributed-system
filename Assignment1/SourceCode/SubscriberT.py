from Subscriber import Subscriber
import time
import sys
import threading

print('--------Unit Test:')
print('# Here is the subscriber side.')

def sub1_op():
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5557'
	sub1 = Subscriber(address, port, 'foods', '20')
	sub1_logfile = './Output/' + sub1.myID + '-subscriber.log'
	with open(sub1_logfile, 'w') as log:
		log.write('ID: ' + sub1.myID + '\n')
		log.write('Topic: ' + sub1.topic + '\n')
		log.write('History publications: %s\n' % sub1.history_count)
		log.write('Connection: tcp://' + address + ':' + port + '\n')
	sub1.prepare()
	sub1.handler()


def sub2_op():
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5557'
	sub2 = Subscriber(address, port, 'animals', '5')
	sub2_logfile = './Output/' + sub2.myID + '-subscriber.log'
	with open(sub2_logfile, 'w') as log:
		log.write('ID: ' + sub2.myID + '\n')
		log.write('Topic: ' + sub2.topic + '\n')
		log.write('History publications: %s\n' % sub2.history_count)
		log.write('Connection: tcp://' + address + ':' + port + '\n')
	sub2.prepare()
	sub2.handler()

def sub3_op():
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5557'
	sub3 = Subscriber(address, port, 'laptops', '10')
	sub3_logfile = './Output/' + sub3.myID + '-subscriber.log'
	with open(sub3_logfile, 'w') as log:
		log.write('ID: ' + sub3.myID + '\n')
		log.write('Topic: ' + sub3.topic + '\n')
		log.write('History publications: %s\n' % sub3.history_count)
		log.write('Connection: tcp://' + address + ':' + port + '\n')
	sub3.prepare()
	sub3.handler()

def sub4_op():
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5557'
	sub4 = Subscriber(address, port, 'animals', '10')
	sub4_logfile = './Output/' + sub4.myID + '-subscriber.log'
	with open(sub4_logfile, 'w') as log:
		log.write('ID: ' + sub4.myID + '\n')
		log.write('Topic: ' + sub4.topic + '\n')
		log.write('History publications: %s\n' % sub4.history_count)
		log.write('Connection: tcp://' + address + ':' + port + '\n')
	sub4.prepare()
	sub4.handler()

def sub5_op():
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5557'
	sub5 = Subscriber(address, port, 'foods', '10')
	sub5_logfile = './Output/' + sub5.myID + '-subscriber.log'
	with open(sub5_logfile, 'w') as log:
		log.write('ID: ' + sub5.myID + '\n')
		log.write('Topic: ' + sub5.topic + '\n')
		log.write('History publications: %s\n' % sub5.history_count)
		log.write('Connection: tcp://' + address + ':' + port + '\n')
	sub5.prepare()
	sub5.handler()

if __name__ == '__main__':
	print('Note: Press Ctr+C, if you want to exit.')
	
	sub1_thr = threading.Thread(target= sub1_op, args=())
	threading.Thread.setDaemon(sub1_thr, True)
	sub2_thr = threading.Thread(target= sub2_op, args=())
	threading.Thread.setDaemon(sub2_thr, True)
	sub3_thr = threading.Thread(target= sub3_op, args=())
	threading.Thread.setDaemon(sub3_thr, True)
	sub4_thr = threading.Thread(target= sub4_op, args=())
	threading.Thread.setDaemon(sub4_thr, True)
	sub5_thr = threading.Thread(target= sub5_op, args=())
	threading.Thread.setDaemon(sub5_thr, True)

	sub1_thr.start()
	time.sleep(3)
	sub2_thr.start()
	time.sleep(3)
	sub3_thr.start()
	time.sleep(3)
	sub4_thr.start()
	time.sleep(3)
	sub5_thr.start()

	while True:
		pass
		
