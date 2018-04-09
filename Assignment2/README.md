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
