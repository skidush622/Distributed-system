#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing
from Subscriber import Subscriber
import random


##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-a', '--address', type=str, help='Please enter ip address of broker.')
    parser.add_argument('-i', '--ip', type=str, help='Self ip address')
    parser.add_argument('-z', '--zk', type=str, help='ZK address')
    # parse the args
    args = parser.parse_args ()

    return args

if __name__ == '__main__':
    args = parseCmdLineArgs()
    address = args.address
    ip = args.ip
    port = '5557'
    zk_address = args.zk
    topics = {1:'animals', 2:'countries', 3:'animals', 4:'laptops', 5:'phones', 6:'laptops', 7:'animals', 8:'animals', 9:'foods', 10:'phones', 11:'phones', 12:'foods',
              13: 'animals', 14: 'countries', 15: 'laptops', 16: 'laptops', 17: 'animals', 18: 'phones', 19: 'foods', 20: 'phones'}
    topic = topics[random.randint(1, 20)]
    hist = random.randint(0, 20)
    sub = Subscriber(zk_address, topic, hist)
    sub.receive_publication()
    sub_logfile = './Output/' + sub.subID + '-subscriber.log'
    with open(sub_logfile, 'w') as log:
        log.write('ID: ' + sub.subID + '\n')
        log.write('Topic: ' + sub.topic + '\n')
        log.write('History publications: %s\n' % sub.history_count)
        log.write('Connection: tcp://%s:%s\n' % (address,port))

    exit_opt = input('Would you like to stop subscriber? (y/n) ')
    if exit_opt != 'y':
        print('Your subscriber will continue running...')
        print('You can press Ctr+C to exit program...')
        while True:
            pass
