#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing
from Subscriber import Subscriber

##################################
# Command line parsing
##################################
def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-a', '--address', type=str, help='Please enter ip address of broker.')
    # parse the args
    args = parser.parse_args ()

    return args

if __name__ == '__main__':
    address = parseCmdLineArgs()
    port = '5557'
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    topic = topics[random.randint(1, 6)]
    hist = random.randint(0, 20)
    sub = Subscriber(address, port, topic, hist)
    sub_logfile = './Output/' + sub.myID + '-subscriber.log'
    with open(sub_logfile, 'w') as log:
        log.write('ID: ' + sub.myID + '\n')
        log.write('Topic: ' + sub.topic + '\n')
        log.write('History publications: %s\n' % sub.history_count)
        log.write('Connection: tcp://' + address + ':' + port + '\n')
    sub.prepare()
    sub.handler()
