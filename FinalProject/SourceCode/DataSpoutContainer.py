import docker

client = docker.from_env()
client.images.build(path='./DockerFiles/DataSpoutDockerFile', tag='DataSpout')
