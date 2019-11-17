var kafka = require('kafka-node');
var HighLevelProducer = kafka.HighLevelProducer;
var Client = kafka.KafkaClient;
var client = new Client();

var topic = 'nearest.request';
var count = 10;
var rets = 0;
var producer = new HighLevelProducer(client);

producer.on('ready', function() {
    setInterval(send, 1000);
});

producer.on('error', function(err) {
    console.log('error', err);
});

function send() {
    var message = '{ "teste": "teste" }';
    producer.send([{ topic: topic, messages: [message] }], function(err, data) {
        if (err) console.log(err);
        else console.log('send %d messages', ++rets);
        if (rets === count) process.exit();
    });
}