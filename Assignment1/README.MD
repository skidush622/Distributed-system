# Programming Assignment 1: Publish-Subscribe using ZMQ and Mininet

Team members:

  - Manyao Peng
  - Yingqi Li
  - Minhui Zhou
  - Zhuangwei Kang

### Environment

  - Python3
  - Pyzmq
  - Mininet

##### Tips:
  - You can spawn multiple publishers and subscribers in this system.
  - Broker will assign a random ownership strength value for
#### How to run our codes

```sh
#######################Step into System#######################
$ python3 PubSub.py

#######################System info#######################
# check pubsub help info
pubsub>> pubsub -h
# check pubsub version
pubsub>> pubsub -v

#######################Publisher#######################
# register publisher to an address with a port number and set its initial topic
pubsub>> pubsub pub -r [address] -P [port] -t [topic]

# send publication with topic
pubsub>> pubsub pub send -t [topic] -p [publication]

# drop a topic for a publisher
pubsub>> pubsub pub -d [topic]

# soft shutoff a publisher
pubsub>> pubsub pub shutoff

#######################Broker#######################
# initialize broker and listen connections at these two ports
pubsub>> pubsub broker -l [xsubsocket port] [xpubsocket port]

#######################Subscriber#######################
# register subscriber to an address with a port number and set its initial topic and history samples count')
pubsub>> pubsub sub -r [address] -P [port] -t [topic] -h [history samples count] 
```
