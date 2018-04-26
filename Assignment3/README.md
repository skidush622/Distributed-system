# Assignment 3: ZooKeeper Register/Lookup in Publish-Subscribe using ZMQ and Mininet

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
  - Kazoo
  
### File Description (/SourceCode/):
  - Broker.py : Broker class that defines  Broker's behavior
  - Publisher.py : Publisher class that defines Publishers' behavior
  - Subscriber.py : Subscriber class that defines Subscribers' behavior
  - BrokerT.py : Broker test file
  - mPublisherT.py : Publisher test file using mininet
  - mSubscriberT.py : Subscriber test file using mininet
  - ZMQHelper.py : Custom API that encapsulates Pyzmq API
  - mininet.py : Entrance file for system test using mininet
  - BusTopology.py : Used to build up bus topology
  - StarTopology.py : Used to build up star topology
  - TreeTopology.py : Used to build up tree topology
  
### Main Logic
  - The whole system has 3 brokers, n publishers, n subscribers, 1 zookeeper server, all of which are mininet host.
  - In ZooKeeper server, there are permanent nodes like Brokers, Publishers and Subscribers and ephemeral node like Leader.
  Brokers contain three ephemeral znodes Broker1, Broker2 and Broker3. Publishers contain ephemeral znodes, n concrete  publishers. Subscribers contain ephemeral znodes, n concrete subscribers.
  

 
