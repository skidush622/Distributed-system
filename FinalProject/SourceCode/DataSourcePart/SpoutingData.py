#!/usr/bin/python
# -*- coding: utf-8 -*-

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
    ingress_path = '/Spout--' + str(spout) + '/Ingress_leader'
    # Watch the change in Leader node

    def connect_2_k8s(address):
        connect_str = 'tcp://' + address
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(connect_str)
        return socket

    if zk.exists(ingress_path) is not None:
        address = zk.get(ingress_path)
        if address is not None:
            ingress_alive = True
            address = address[0] + ':2341'
            socket = connect_2_k8s(address)

    @zk.DataWatch(ingress_path)
    def watch_ingress(data, stat):
        global socket
        global ingress_alive
        if stat is not None:
            socket = connect_2_k8s(str(data) + ':2341')
            ingress_alive = True
        else:
            ingress_alive = False

    data_files = glob.glob('/home/CS6381/FinalProject/SourceCode/DataSourcePart/DataSource/*.txt')

    data_files = data_files[spout-1]

    print(data_files)

    def read_file(file_path):
        with open(file_path, 'r') as f:
            for line in f:
                while ingress_alive is False:
                    pass
                state = file_path.split('/')[7]
                state = state.split('.')[0]
                current = str(time.time())
                event = [current, state, line]
                socket.send_string(simplejson.dumps(event))
                print(socket.recv_string())
                time.sleep(0.02)

    read_file(data_files)






