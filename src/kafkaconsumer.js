var Kafka = require('node-rdkafka');
var driver_options = {
    //'debug': 'all',
    'metadata.broker.list': 'aapeventstreams-kafka-bootstrap-eventstreams.cp4auto-1e3af63cfd19e855098d645120e18baf-0000.us-south.containers.appdomain.cloud:443',
    'security.protocol': 'sasl_ssl',
    'ssl.ca.location': './es-cert.pem',
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': 'scram-user',
    'sasl.password': 'JrkOtw5fTkgj',
    'broker.version.fallback': '0.10.0',
    'log.connection.close' : false,
    'client.id': 'Es-NodeJS-101',
    'group.id': 'node-rdkafka-consumer-tys-example',
    'enable.auto.commit': false
};

var consumer = new Kafka.KafkaConsumer(driver_options);
  
var topicName = 'TYS';
  
//logging debug messages, if debug is enabled
consumer.on('event.log', function(log) {
 console.log(log);
});
  
//logging all errors
consumer.on('event.error', function(err) {
  console.error('Error from consumer');
  console.error(err);
});
  
//counter to commit offsets every numMessages are received
var counter = 0;
var numMessages = 5;
  
consumer.on('ready', function(arg) {
    console.log('consumer ready.' + JSON.stringify(arg));
  
    consumer.subscribe([topicName]);
    //start consuming messages
    consumer.consume();

});
    
consumer.on('data', function(m) {
    counter++;
  
    //committing offsets every numMessages
    if (counter % numMessages === 0) {
      console.log('calling commit');
      consumer.commit(m);
    }
  
    // Output the actual message contents
    console.log(JSON.stringify(m));
    console.log(m.value.toString());
  
});
  
consumer.on('disconnected', function(arg) {
    console.log('consumer disconnected. ' + JSON.stringify(arg));
});
  
//starting the consumer
consumer.connect();
  
//stopping this example after 30s
setTimeout(function() {
    consumer.disconnect();
  }, 30000);