# Assignment 2: DHT Register/Lookup in Publish-Subscribe using ZMQ and Mininet

### Team members

 - Zhuangwei Kang
 - Manyao Peng
 - Minhui Zhou
 - Yingqi Li

### Environment
  - Ubuntu 16.04
  - Python
  - Pyzmq
  - Mininet
  
### File Description (/SourceCode/):
  - Broker.py : Broker class that defines  Broker's behavior
  - Publisher.py : Publisher class that defines Publishers' behavior
  - Subscriber.py : Subscriber class that defines Subscribers' behavior
  - BrokerT.py : Broker test file
  - mPublisherT.py : Publisher test file using mininet
  - mSubscriberT.py : Subscriber test file using mininet
  - PubFuncs.py : Contains some helper methods for publisher
  - ZMQHelper.py : Custom API that encapsulates Pyzmq API
  - hash_ring.py : Custom API that encapsulates hash ring API
  - mininet.py : Entrance file for system test using mininet
  - BusTopology.py : Used to build up bus topology
  - StarTopology.py : Used to build up star topology
  - TreeTopology.py : Used to build up tree topology
    
### Broker Logic
#### Related to publisher:  
 - Receive registered message. First, check out whether the registered topic is ruled by current broker. If yes, register directly. Otherwise, begin DHT routing and record the topic and publisher in the corresponding broker. Meanwhile, the broker which rules the topic sends message to the publisher and tells the publisher to update the broker IP, so that later the broker will communicate directly with the publisher without DHT routing.
 - Receive published message. First, check out whether the received topic is ruled by itself. If yes, store the message directly. Otherwise, it means that the publisher publishes a new topic which needs DHT routing and finds the broker which rules the topic. Then the publisher will be told to send the message about the topic directly to the target broker. And the target broker should store the message about the pulication and publisher. If the corresponding broker is not found after one circle, it means the message published can not found the corresponding topic in hash ring. A new node is needed to insert into the hash ring. To simplify our design, we will not insert new node in the hash ring, since inserting new nodes in the hash ring will cause lots of trouble. We only hash the topic to get a node ID, and process DHT routing again, find the range ruled by the broker including this ID. The broker will be told that a new topic should be added to its ruling range. The broker will be notified of the published message, the topic and the corresponding publisher. So since then, publication about the topic will be sent directly to the new target broker. Meanwhile, the successor of the target broker will backup the new topic and received the content of the message. And the successor will pass the message to make sure that every backup node of the broker will add a new topic backup.
 - Receive drop topic message. Update publisher table and ownership strength table.
 - Receive heartbeat message. If the broker does not receive the heartbeat in the corresponding time, the publisher is assumed to die, and all the messages about the corresponding publisher are deleted.
 
#### Related to subscriber:
 - After receiving the registered message of the subscriber, the broker checks out whether the to be registered topic is ruled by itself. If yes, the broker will directly send the message to the subscriber. Otherwise, begin DHT routing, find the broker which rules the topic. The subscriber will update the registered IP, reconnect to the target broker. Since then, the target broker will send message to the subscriber. 
 
#### Related to DHT:
 - Every broker has a view about the whole DHT itself.
 - Publishers and subscribers can connnect to arbitrary broker.
 - Every broker has two successor brokers to backup for it. We use hop count to represent whether it is the original data or the backup data. If hop count == 2, the broker is the primary broker which stores the original data. If hop count == 1, the broker stores the first backup of its predecessor. If hop count == 0, the broker stores the second (last) backup of its predecessor of predecessor. 
 - Every broker will send its heartbeat to its predecessor. After receiving the heartbeat message the predecessor checks out whether the heartbeat message is sent by its known successor. If not, it indicates that the previous node has already died.
 - When a broker died, the successor of it inherits all its stored messages.
 
 - Here is an instance to illustrate what will happen when a broker dies. B1, B2 and B3 are all brokers. The predecessor of B2 is B1 and the successor of B2 is B3. So B2 sends heartbeat to B1 and B3 sends heartbeat to B2. B1 passes data to B2 through port3 and B2 passes data to B3 similarly. When B2 dies, the following things will happen:
 
   1. B1 tells all brokers to update the view of the whole DHT.
   
   2. B1 tells B3 its predecessor died and asks B3 to inherit all data from B2.
   
   3. B3 will do the following tasks:
   
     - Inherit all the backup which B2 manages.
     - Notify all publishers and subscribers which were connected to B2 to reconnect to the new broker.
     - Notify its successor to increase its hop count (hop count ++). It will pass the message of increasing hop count until hop count == 0. For example, when B2 died, the hop count of B3 is changed from 0 to 1, and B3 tells B4 to update its hop count to 0, then B4 will be the last copy of B1. 
 
#### Test Methods:
  - Atomatically test using Mininet
  
#### Topology Type:
  - Bus Topology
  - Star Topology
  - Tree Topology 
  
#### Test cases:
- Multiple publishers & subscribers work concurrently
- Subscribers receive history publications
- Publisher failing
- Ownership strength update
- Publisher drop topics
##### Atomatically Test using Mininet
- Prerequisite:
```sh
 $ sudo pip install simplejson
```
- Command Line:
```sh
 $ sudo python mininet.py -b [broker_number] -p [publisher_number] -s [subscriber_num] -T [topology_type]
```
- Tips:
```sh
# Arguements liminitions:
# broker_num : type = int, default = 9
# publisher_num : type = int, default = 3
# subscriber_num : type = int, default = 5
# topology_type : choices = [1, 2, 3], (correspond to: 1: Bus topology, 2: Star topology, 3: Tree topology), default: 1: Bus topology

# note: 
# 1. After run this command, please wait until all nodes launched up, you would see sorts of Xterm CLIs. Press Ctr+c if you want to exit program.
# 2. Topics for publishers and subscribers are assigned radomlly, provided topics include: animals, foods, laptops, phones, universities and countries. If you find no subscriber received publications, please check if any publisher is publishing the expected topic.
# 3. You can check log files under /Output directory. I suggest creating a folder to store all log files after you run a test case. 
# 4. !!!Please run 'sudo mn -c' to clean all nodes before you run a new test.
```
 
 
