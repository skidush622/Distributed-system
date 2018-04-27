FROM ubuntu:16.04
MAINTAINER Zhuangwei Kang

RUN apt-get update
RUN apt-get install -y python
RUN apt-get install -y python-pip

# Install app dependencies
RUN pip install --upgrade pip
RUN pip install pyzmq
RUN apt-get install -y nano

RUN pip install kazoo

EXPOSE 2341