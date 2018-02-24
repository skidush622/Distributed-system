import time
import sys
import threading
from PubFuncs import *

def common_func(address, port):
	def pub1_op():
		pub1 = register(address, port, 'foods')
		time.sleep(3)
		foods = get_publications('./Input/foods.txt')
		pub1_log = './Output/' + pub1.myID + '-publisher.log'
		publish(pub1, 'foods', foods, pub1_log)

	def pub2_op():
		pub2 = register(address, port, 'animals')
		time.sleep(3)
		animals = get_publications('./Input/animals.txt')
		pub2_log = './Output/' + pub2.myID + '-publisher.log'
		publish(pub2, 'animals', animals, pub2_log)

	def pub3_op():
		pub3 = register(address, port, 'laptops')
		time.sleep(3)
		laptops = get_publications('./Input/laptops.txt')
		pub3_log = './Output/' + pub3.myID + '-publisher.log'
		publish(pub3, 'laptops', laptops, pub3_log)

	pub1_thr = threading.Thread(target= pub1_op, args= ())
	threading.Thread.setDaemon(pub1_thr, True)
	pub2_thr = threading.Thread(target= pub2_op, args= ())
	threading.Thread.setDaemon(pub2_thr, True)
	pub3_thr = threading.Thread(target= pub3_op, args= ())
	threading.Thread.setDaemon(pub3_thr, True)

	pub1_thr.start()
	time.sleep(3)
	pub2_thr.start()
	time.sleep(3)
	pub3_thr.start()

def unitTest1():
	print('--------Unit Test1:')
	print('# This case will test if the system supports multiple publishers and subscribers working concurrently.')
	print('# Here is the publisher side.')
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5556'
	common_func(address, port)

def unitTest2():
	print('--------Unit Test2:')
	print('# This case will test if subscribers can receive history publications.')
	print('# Here is the publisher side.')
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5556'
	common_func(address, port)

def unitTest3():
	print('--------Unit Test3:')
	print('# This case will test if broker can detect publisher failing.')
	print('# Here is the publisher side.')
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5556'

	def pub1_op():
		pub1 = register(address, port, 'foods')
		foods = get_publications('./Input/foods.txt')
		pub1_log = './Output/' + pub1.myID + '-publisher.log'
		publish(pub1, 'foods', foods, pub1_log)
		shutoff(pub1, pub1_log)
		sys.exit()

	def pub2_op():
		pub2 = register(address, port, 'animals')
		animals = get_publications('./Input/animals.txt')
		pub2_log = './Output/' + pub2.myID + '-publisher.log'
		publish(pub2, 'animals', animals, pub2_log)
		shutoff(pub2, pub2_log)
		sys.exit()

	def pub3_op():
		pub3 = register(address, port, 'laptops')
		laptops = get_publications('./Input/laptops.txt')
		pub3_log = './Output/' + pub3.myID + '-publisher.log'
		publish(pub3, 'laptops', laptops, pub3_log)
		shutoff(pub3, pub3_log)
		sys.exit()

	pub1_thr = threading.Thread(target= pub1_op, args= ())
	threading.Thread.setDaemon(pub1_thr, True)
	pub2_thr = threading.Thread(target= pub2_op, args= ())
	threading.Thread.setDaemon(pub2_thr, True)
	pub3_thr = threading.Thread(target= pub3_op, args= ())
	threading.Thread.setDaemon(pub3_thr, True)

	pub1_thr.start()
	time.sleep(3)
	pub2_thr.start()
	time.sleep(3)
	pub3_thr.start()

def unitTest4():
	print('--------Unit Test4:')
	print('# This case will test if ownership strength feature works.')
	print('# Here is the publisher side.')
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5556'

	def pub_op():
		pub = register(address, port, 'foods')
		foods = get_publications('./Input/foods.txt')
		pub_log = './Output/' + pub.myID + '-publisher.log'
		publish(pub, 'foods', foods, pub_log)
		time.sleep(10)
		publish(pub, 'foods', foods, pub_log)

	pub1_thr = threading.Thread(target= pub_op, args= ())
	threading.Thread.setDaemon(pub1_thr, True)
	pub2_thr = threading.Thread(target= pub_op, args= ())
	threading.Thread.setDaemon(pub2_thr, True)
	pub3_thr = threading.Thread(target= pub_op, args= ())
	threading.Thread.setDaemon(pub3_thr, True)
	pub4_thr = threading.Thread(target= pub_op, args= ())
	threading.Thread.setDaemon(pub4_thr, True)
	pub5_thr = threading.Thread(target= pub_op, args= ())
	threading.Thread.setDaemon(pub5_thr, True)


	pub1_thr.start()
	pub2_thr.start()
	pub3_thr.start()
	pub4_thr.start()
	pub5_thr.start()

def unitTest5():
	print('--------Unit Test5:')
	print('# This case will test drop topic feature for publisher.')
	print('# Here is the publisher side.')
	address = sys.argv[1] if len(sys.argv) > 1 else "localhost"
	port = '5556'

	def pub1_op():
		pub1 = register(address, port, 'foods')
		foods = get_publications('./Input/foods.txt')
		animals = get_publications('./Input/animals.txt')
		laptops = get_publications('./Input/laptops.txt')
		pub1_log = './Output/' + pub1.myID + '-publisher.log'
		publish(pub1, 'foods', foods, pub1_log)
		drop_topic(pub1, 'foods', pub1_log)
		publish(pub1, 'animals', animals, pub1_log)
		drop_topic(pub1, 'animals', pub1_log)
		publish(pub1, 'laptops', laptops, pub1_log)
		drop_topic(pub1, 'laptops', pub1_log)


	pub1_thr = threading.Thread(target= pub1_op, args= ())
	threading.Thread.setDaemon(pub1_thr, True)

	pub1_thr.start()


def userInterface():
	print('------------------------------Pub/Sub System Unit Tests------------------------------')
	print('1. Multiple publishers & subscribers work concurrently')
	print('2. Subscribers receive history publications')
	print('3. Publisher failing')
	print('4. Ownership strength feature')
	print('5. Publisher drop topics')
	print('-------------------------------------------------------------------------------------')
	print('Press Ctr+C if you want to exit.')
	while True:
		try:
			choice = int(input('Please select an unit test option: '))
			if choice < 1 or choice > 5:
				print('Invalid choice.....Try again')
				continue
			else:
				if choice == 1:
					unitTest1()
				elif choice == 2:
					unitTest2()
				elif choice == 3:
					unitTest3()
				elif choice == 4:
					unitTest4()
				elif choice == 5:
					unitTest5()
			break
		except ValueError:
			print('Invalid choice.....Try again')
	while True:
		pass

if __name__ == '__main__':
	userInterface()
