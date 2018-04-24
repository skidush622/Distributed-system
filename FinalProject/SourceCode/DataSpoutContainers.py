import docker
import argparse


class DataSpoutContainers:
    def __init__(self):
        self.client = docker.from_env()
        self.dockerfile_path = '/home/zhuangwei/Documents/CS6381/FinalProject/SourceCode/DockerFiles/'
        self.image_tag = 'dataspout'
        self.image = None

    def build_image(self):
        self.image = self.client.images.build(path=self.dockerfile_path, tag=self.image_tag)

    def run_container(self, name, hostname, command):
        container = self.client.containers.run(image=self.image,
                                               detach=True,
                                               name=name,
                                               hostname=hostname,
                                               command=command)
        return container

    def main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--address', type=str, default='127.0.0.1', help='The address of destination.')
        parser.add_argument('-p', '--port', type=str, default='2341', help='The port number of destination.')
        args = parser.parse_args()
        address = args.address
        port = args.port
        for i in range(5):
            container_name = 'DataSpout' + str(i+1)
            command = 'python SpoutingData.py -s ' + str(i+1) + ' -a ' + address + ' -p ' + port
            self.run_container(container_name, container_name, command)


if __name__ == '__main__':
    spout = DataSpoutContainers()
    spout.build_image()
    spout.main()








