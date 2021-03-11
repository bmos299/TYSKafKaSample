# TYSKafKaSample

Starting the NodeJS Application
This article is built around the node-rdkafka npm library. This is a third party library provided for securely connecting to Kafka streams.
```
mkdir ibm-es-nodejs 
npm init 
npm i --save node-rdkafka 
```
The two programs to produce and consume are:
kafkaconsumer.js 	
kafkaproducer.js

The endpoint, cert, and topic are hardcoded.  We can make these variables moving forward.

