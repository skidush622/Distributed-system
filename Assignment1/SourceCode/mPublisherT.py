#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing

import PubFuncs
import random

def testHelper(address, topic):
    pub = register(address, '5556', topic)
    file_path = './Input/'+ topic + '.txt'
    pubs_list = get_publications(file_path)
    log_file = './Output/' + pub.myID + '-publisher.log'
    publish(pub, topic, pubs_list, log_file)
    return pub, log_file

def unitTest12(address):
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    topic = topics[random.randint(1, 6)]
    testHelper(address, topic)

def test3Helper(address, topic):
    t = testHelper(address, topic)
    shutoff(t[0], t[1])

def unitTest3(address):
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    topic = topics[random.randint(1, 6)]
    test3Helper(address, topic)

def unitTest4(address):
    testHelper(address, 'animals')

def unitTest5(address):
    topics = ['animals', 'countries', 'foods', 'laptops', 'phones', 'universities']
    pub = register(address, '5556', topic)
    file_path = './Input/'+ topic + '.txt'
    log_file = './Output/' + pub.myID + '-publisher.log'
    for topic in topics:
        pubs_list = get_publications(file_path)
        publish(pub, topic, pubs_list, log_file)
        drop_topic(pub, topic, log_file)

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
