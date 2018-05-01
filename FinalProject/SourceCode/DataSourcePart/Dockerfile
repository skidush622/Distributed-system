FROM ubuntu:16.04
MAINTAINER Zhuangwei Kang

RUN apt-get update
# Install Python
RUN apt-get install -y python


# Install pip
RUN apt-get install -y python-pip python-dev build-essential
RUN pip install --upgrade pip
RUN pip install --upgrade virtualenv


# Install app dependencies
RUN pip install pyzmq
RUN apt-get install -y nano
RUN pip install kazoo
RUN pip install simplejson

COPY DataSource /home/DataSource/
COPY SpoutingData.py /home/

EXPOSE 2341