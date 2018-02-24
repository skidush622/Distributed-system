#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing

from PubFuncs import *
import random
import time

def testHelper(address, topic):
    pub = register(address, '5556', topic)
    print('Waiting for Publisher ready...')
    time.sleep(5)
    file_path = './Input/'+ topic + '.txt'
    pubs_list = get_publications(file_path)
    print('PUB ID:', pub.myID)
    log_file = './Output/' + pub.myID + '-publisher.log'
    publish(pub, topic, pubs_list, log_file)
    return pub, log_file

def unitTest12(address):
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'countries', 5:'phones', 6:'universities', 7: 'phones', 8: 'laptops', 9: 'foods', 10:'laptops', 11:'laptops', 12:'animals'}
    topic = topics[random.randint(1, 12)]
    testHelper(address, topic)
    wait()

def test3Helper(address, topic):
    t = testHelper(address, topic)
    shutoff(t[0], t[1])
    wait()

def unitTest3(address):
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'countries', 5:'phones', 6:'universities', 7: 'phones', 8: 'laptops', 9: 'foods', 10:'laptops', 11:'laptops', 12:'animals'}
    topic = topics[random.randint(1, 12)]
    test3Helper(address, topic)
    wait()

def unitTest4(address):
    testHelper(address, 'animals')
    wait()

def unitTest5(address):
    topics = ['animals', 'countries', 'foods', 'laptops', 'phones', 'universities']
    pub = None
    log_file = ''
    for index, topic in enumerate(topics):
        if index == 0:
            pub = register(address, '5556', topics[0])
            log_file = './Output/' + pub.myID + '-publisher.log'
        file_path = './Input/'+ topic + '.txt'
        pubs_list = get_publications(file_path)
        publish(pub, topic, pubs_list, log_file)
        drop_topic(pub, topic, log_file)
    wait()

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-t", "--test", type=int, choices=[1, 2, 3, 4, 5], help="Please enter your test case choice.")
    parser.add_argument('-a', '--address', type=str, help='Please enter ip address of broker.')
    # parse the args
    args = parser.parse_args ()

    return args

def wait():
    while True:
        pass

if __name__ == '__main__':
    args = parseCmdLineArgs()
    test_choice = args.test
    address = args.address
    if test_choice == 1 or test_choice == 2:
        unitTest12(address)
    elif test_choice == 3:
        unitTest3(address)
    elif test_choice == 4:
        unitTest4(address)
    elif test_choice == 5:
        unitTest5(address)
