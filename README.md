# ChordProtocol
A simple Python implementation of the Chord p2p protocol.

### Usage:
To launch a node one must pass it an ip address and port number as arguments

```
python node.py <ip> <port>
```

A node can join a chord ring by selecting 'join' in the menu prompt and passing the ip address and the port number of a node in that node ring.

Todo:
* implement leave (nodes can leave but not by menu input)
* implement key/value pairs (adding data to nodes)
* implement some error handling 

Done:
* start
* stabalize
* fix fingers
* notify 
* join
* find successor
* fnid predecessor
* closest proceding node
* check predecessor
