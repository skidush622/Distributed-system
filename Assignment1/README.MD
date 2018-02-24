# Programming Assignment 1: Publish-Subscribe using ZMQ and Mininet

Team members:

  - Manyao Peng
  - Yingqi Li
  - Minhui Zhou
  - Zhuangwei Kang

### Environment
  - Ubuntu 16.04
  - Python
  - Pyzmq
  - Mininet

#### Tips:
  - You can spawn multiple publishers and subscribers in this system.
  - Broker will assign a random ownership strength value for each publisher when it publishes a new topic.
  - Broker sends all valid publications to all subscribers every 10 seconds.
  - Publishers send heartbeat signal evey 10 seconds.
  - If a publisher did't send heartbeat signal to broker within 60 seconds, broker would announce the publisher has died and all data related to this publisher would be deleted.
  - Publications storage format: {$pubID : {$topic : [$publication]}}
  - Publisher ownership strength storage format: {$topic : {$pubID : $ownership_strength}}
#### File Description (/SourceCode/):
  - Broker.py : Broker class that defines  Broker's behavior
  - Publisher.py : Publisher class that defines Publishers' behavior
  - Subscriber.py : Subscriber class that defines Subscribers' behavior
  - BrokerT.py : Broker test entrance at localhost
  - lBrokerT.py : Broker test entrance at localhost
  - lPublisherT.py : Publisher test file at localhost
  - lSubscriberT.py : Subscriber test file at localhost
  - mPublisherT.py : Publisher test file using mininet
  - mSubscriberT.py : Subscriber test file using mininet
  - PubFuncs.py : Contains some helper methods for publisher
  - PubSub.py : Entrance file for manually test at localhost
  - mininet.py : Entrance file for system test using mininet
  - ZMQHelper.py : Custom API that encapsulates Pyzmq API
  - BusTopology.py : Used to build up bus topology
  - StarTopology.py : Used to build up star topology
  - TreeTopology.py : Used to build up tree topology
  - RingTopology.py : Used to build up ring topology(Not Used)
#### Test Methods:
  - Atomatically test using Mininet
  - Atomatically test at localhost
  - Manually test
#### Topology Type:
  1. Bus Topology
  2. Star Topology
  3. Tree Topology 
#### Test cases:
  1. Multiple publishers & subscribers work concurrently
	2. Subscribers receive history publications
	3. Publisher failing
	4. Ownership strength update
	5. Publisher drop topics
##### Atomatically Test using Mininet
```sh
# Arguements liminitions:
# publisher_num : type = int
# subscriber_num : type = int
# topology_type : choices = [1, 2, 3] (correspond to: Bust topology, Star topology, Tree topology)
# testcase_choice : choices = [1, 2, 3, 4, 5] 
$ sudo python mininet.py -p [publisher_num] -s [subscriber_num] -T [topology_type] -t [testcase_choice]

# note: 
# 1. After run this command, please wait until all nodes launched up, you would see sorts of Xterm CLIs. Press Ctr+c if you want to exit program.
# 2. Topics for publishers and subscribers are assigned radomlly, provided topics include: animals, foods, laptops, phones, universities and countries. If you find no subscriber received publications, please check if any publisher is publishing the expected topic.
# 3. You can check log files under /Output directory. I suggest creating a folder to store all log files after you run a test case. 
# 4. !!!Please run 'sudo mn -c' to clean all nodes before you run a new test.
```
##### Atomatically Test at Localhost
This test method will spawn 3 publishers and 5 subscribers with some redom topics.
Broker will bind 5556 and 5557 ports automatically
```sh
# At broker side:
$ sudo python lBrokerT.py
# Launch a new window (Publisher side)
$ sudo python lPublisherT.py
# Launch a new window (Subscriber side)
$ sudo python lSubscriberT.py
```
##### Manually Test

```sh
# Step into System
$ sudo python PubSub.py

# Get System info
# check pubsub help info
pubsub>> pubsub -h
# check pubsub version
pubsub>> pubsub -v

#Publisher
# register publisher to an address with a port number and set its initial topic
pubsub>> pubsub pub -r [broker address] -P [port] -t [topic]

# send publication with topic
pubsub>> pubsub pub send -t [topic] -p [publication]

# drop a topic for a publisher
pubsub>> pubsub pub -d [topic]

# soft shutoff a publisher
pubsub>> pubsub pub shutoff

# Broker
# initialize broker and listen connections at these two ports
pubsub>> pubsub broker -l [xsubsocket port] [xpubsocket port]

# Subscriber
# register subscriber to an address with a port number and set its initial topic and history samples count')
pubsub>> pubsub sub -r [broker address] -P [port] -t [topic] -h [history samples count] 

# Exit
pubsub>> exit
```
