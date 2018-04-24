FROM ubuntu:16.04
MAINTAINER Zhuangwei Kang

RUN apt-get update
RUN apt-get install -y python
RUN apt-get install -y python-pip

# Install app dependencies
RUN pip install --upgrade pip
RUN pip install pyzmq

COPY DataSource /home/
COPY SpoutingData.py /home/

RUN cd /home/
RUN chmod +x *

EXPOSE 5556 5557 5558 5559 6000