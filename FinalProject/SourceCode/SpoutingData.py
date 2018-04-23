import glob
import argparse
import zmq
import random
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--spout', type=int, choices=[1, 2, 3, 4, 5], default=1, help='The spout number.')
    parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='The address of destination.')
    parser.add_argument('-p', '--port', type=str, default='2341', help='The port number of destination server.')
    args = parser.parse_args()
    spout = args.spout
    address = args.address
    port = args.port

    def connect_2_k8s(__address__='127.0.0.1', __port__='2341'):
        connect_str = 'tcp://' + __address__ + ':' + __port__
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(connect_str)
        return socket

    socket = connect_2_k8s(address, port)

    data_files = glob.glob('/home/DataSource/*.txt')
    if spout != 5:
        data_files = data_files[(spout-1)*10:spout*10]
    else:
        data_files = data_files[(spout-1)*10:]

    def read_file(file_path):
        with open(file_path, 'r') as f:
            for line in f:
                socket.send_string(line)
                time.sleep(random.uniform(0.0, 0.3))






