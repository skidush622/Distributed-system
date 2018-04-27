#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing
import random
import time
from Publisher import Publisher


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

def unitTest12(ip, address):
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'countries', 5:'phones', 6:'universities', 7: 'phones', 8: 'laptops', 9: 'foods', 10:'laptops', 11:'laptops', 12:'animals'}
    topic = topics[random.randint(1, 12)]
    publisher = Publisher(zk_server, topic)
    publisher.register()
    # wait()

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-a', '--address', type=str, help='Please enter ip address of broker.')
    parser.add_argument('-i', '--ip', type=str, help='current publisher ip address')
    parser.add_argument("-z", '--zk_ip', type=str, help='zookeeper ip address')
    # parse the args
    args = parser.parse_args ()

    return args

def wait():
    while True:
        pass

if __name__ == '__main__':
    args = parseCmdLineArgs()
    address = args.address
    ip = args.ip
    unitTest12(ip, address)
