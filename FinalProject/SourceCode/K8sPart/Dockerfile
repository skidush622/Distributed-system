FROM ubuntu:16.04
MAINTAINER Zhuangwei Kang

# Install python
RUN apt-get update
RUN apt-get install -y python

# Install pip
RUN apt-get install -y python-pip python-dev build-essential
RUN apt-get install -y libmysqlclient-dev
RUN pip install --upgrade pip
RUN pip install --upgrade virtualenv

# Install app dependencies
RUN pip install pyzmq
RUN apt-get install -y nano git
RUN pip install kazoo
RUN pip install simplejson
RUN pip install PyMySQL
RUN pip install numpy

RUN git clone https://github.com/ZhuangweiKang/CS6381.git /home/

EXPOSE 2341