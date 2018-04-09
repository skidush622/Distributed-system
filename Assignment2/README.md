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
  
### Broker Logic
#### Related to publisher:  
 - Receive registered message. First, check out whether the registered topic is ruled by current broker. If yes, register directly. Otherwise, begin DHT routing and record the topic and publisher in the corresponding broker. Meanwhile, the broker which rules the topic sends message to the publisher and tells the publisher to update the broker IP, so that later the broker will communicate directly with the publisher without DHT routing.
 - Receive published message. First, check out whether the received topic is ruled by itself. If yes, store the message directly. Otherwise, it means that the publisher publishes a new topic which needs DHT routing and finds the broker which rules the topic. Then the publisher will be told to send the message about the topic directly to the target broker. And the target broker should store the message about the pulication and publisher. If the corresponding broker is not found after one circle, it means the message published can not found the corresponding topic in hash ring. A new node is needed to insert into the hash ring. To simplify our design, we will not insert new node in the hash ring, since inserting new nodes in the hash ring will cause lots of trouble. We only hash the topic to get a node ID, and process DHT routing again, find the range ruled by the broker including this ID. The broker will be told that a new topic should be added to its ruling range. The broker will be notified of the published message, the topic and the corresponding publisher. So since then, publication about the topic will be sent directly to the new target broker. Meanwhile, the successor node of the target broker will backup the new topic and received the content of the message. And the successor node will pass the message to make sure that every backup node of the broker will add a new topic backup.
 - Receive drop topic message. Update publisher table and ownership strength table.
 - Receive heartbeat message. If the broker does not receive the heartbeat in the corresponding time, the publisher is assumed to die, and all the messages about the corresponding publisher are deleted.
 
#### Related to subscriber:
 - After receiving the registered message of the subscriber, the broker checks out whether the to be registered topic is ruled by itself. If yes, the broker will directly send the message to the subscriber. Otherwise, begin DHT routing, find the broker which rules the topic. The subscriber will update the registered IP, reconnect to the target broker. Since then, the target broker will send message to the subscriber. 
 
#### Related to DHT:
 - Every broker will send its heartbeat to its successor. After receiving the heartbeat message the successor checks out whether the heartbeat message is sent by its known predecessor. If not, it indicates that the previous node has already died, the precursor table needs to be updated.
 
