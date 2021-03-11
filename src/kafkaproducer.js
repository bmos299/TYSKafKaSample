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
    'client.id': 'Es-NodeJS-101'
};
var topicName = "TYS"


var producerTopicOpts = {
    'request.required.acks': -1,
    'produce.offset.report': true
};
//producer = new Kafka.Producer(producer_opts, producerTopicOpts);
producer = new Kafka.Producer(driver_options);
producer.setPollInterval(100);
// Register listener for debug information; only invoked if debug option set in driver_options
producer.on('event.log', function(log) {
    console.log(log);
});
// Register error listener
producer.on('event.error', function(err) {
    console.error('Error from producer:' + JSON.stringify(err));
});

// Register delivery report listener
producer.on('delivery-report', function(err, dr) {
    if (err) {
        console.error('Delivery report: Failed sending message ' + dr.value);
        console.error(err);
        // We could retry sending the message
    } else {
        console.log('Message produced, partition: ' + dr.partition + ' offset: ' + dr.offset);
    }
});


// Register callback invoked when producer has connected
producer.on('ready', function() {
    console.log('The producer has connected.');

    // request metadata for all topics
    producer.getMetadata({
        timeout: 10000
    },
    function(err, metadata) {
        if (err) {
            console.error('Error getting metadata: ' + JSON.stringify(err));
            shutdown(-1);
        } else {
            console.log('Producer obtained metadata: ' + JSON.stringify(metadata));
            var topicsByName = metadata.topics.filter(function(t) {
                return t.name === topicName;
            });
            if (topicsByName.length === 0) {
                console.error('ERROR - Topic ' + topicName + ' does not exist. Exiting');
                shutdown(-1);
            }
        }
    });
    var counter = 0;
});

producer.connect()

setTimeout(produceMessage, 3000);

function produceMessage ()
{
  var message = new Buffer('Message I want to send Barry');
  var key = 'Key';
  var partition = 0
  // Short sleep for flow control in this sample app
  // to make the output easily understandable
  try {
      producer.produce(topicName, partition, message, key);
  } catch (err) {
      console.error('Failed sending message ' + message);
        console.error(err);
  }
}