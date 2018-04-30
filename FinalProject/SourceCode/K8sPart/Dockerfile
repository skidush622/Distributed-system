FROM ubuntu:16.04
MAINTAINER Zhuangwei Kang

RUN apt-get update
RUN apt-get install -y python python3
RUN apt-get install -y python-pip python-dev build-essential
# Install app dependencies
RUN pip install --upgrade pip
RUN pip install pyzmq
RUN apt-get install -y nano
RUN pip install kazoo
RUN pip install simplejson
RUN apt-get install -y libmysqlclient-dev
RUN pip install MySQL-python

COPY MySQLOP /home/K8sPart/MySQLOP/
COPY ./*.py /home/K8sPart/

EXPOSE 2341