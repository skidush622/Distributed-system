#!/usr/bin/python

import glob
import argparse
import zmq
import time
import simplejson
from kazoo.client import KazooState
from kazoo.client import KazooClient

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--spout', type=int, choices=[1, 2, 3, 4, 5], default=1, help='The spout number.')
    parser.add_argument('-z', '--zk_address', type=str, default='127.0.0.1', help='The ip address of ZooKeeper server.')
    args = parser.parse_args()
    spout = args.spout
    socket = None
    ingress_alive = False

    zk_address = args.zk_address + ':2181'
    # connect to ZK server
    zk = KazooClient(hosts=zk_address)
    zk.start()
    ingress_path = '/' + spout + '/' + '/Ingress_operators/leader'
    # Watch the change in Leader node

    @zk.DataWatch(ingress_path)
    def watch_ingress(data, stat):
        global socket
        global ingress_alive
        if stat == KazooState.CONNECTED:
            socket = connect_2_k8s(data)
            ingress_alive = True
        else:
            ingress_alive = False

    def connect_2_k8s(address):
        connect_str = 'tcp://' + address
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(connect_str)
        return socket

    data_files = glob.glob('home/DataSource/*.txt')
    data_files = data_files[spout-1]

    print(data_files)

    def read_file(file_path):
        with open(file_path, 'r') as f:
            for line in f:
                while ingress_alive is False:
                    pass
                state = file_path.split('/')[2]
                state = state.split('.')[0]
                current = time.time()
                event = {
                    'Time': current
                    'State': state,
                    'Data': line,
                }
                socket.send_string(simplejson.dumps(event))
                print(socket.recv_string())

    read_file(data_files)






