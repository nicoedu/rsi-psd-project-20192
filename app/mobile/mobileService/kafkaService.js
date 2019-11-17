var express = require('express');
var bodyParser = require('body-parser');
var logger = require('morgan');
var methodOverride = require('method-override')
var cors = require('cors');
const kafkaLogging = require('kafka-node/logging');


function consoleLoggerProvider(name) {
    // do something with the name
    return {
        debug: console.debug.bind(console),
        info: console.info.bind(console),
        warn: console.warn.bind(console),
        error: console.error.bind(console)
    };
}

kafkaLogging.setLoggerProvider(consoleLoggerProvider);

var kafka = require('kafka-node')

var app = express();
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(methodOverride());
app.use(cors());


Producer = kafka.HighLevelProducer;
clientProducer = new kafka.KafkaClient();
producer = new Producer(clientProducer, { requireAcks: 1 });

Consumer = kafka.Consumer
clientConsumer = new kafka.KafkaClient()
clientConsumer.loadMetadataForTopics(["interpolation.reply"], (err, resp) => {
    console.log(JSON.stringify(resp))
});

consumer = new Consumer(
    clientConsumer, [
        { topic: 'nearest.request' }
    ], {
        autoCommit: false
    }
);
producer.on('ready', function() {


    app.post('/', function(req, res) {
        const buffer = new Buffer.from(JSON.stringify(req.body));
        payloads = [
            { topic: 'nearest.request', messages: buffer }
        ];
        producer.send(payloads, function(err, data) {
            if (err) {
                res.status(500).send(JSON.stringify({
                    "messages": "Error while sending messages.",
                }));
            } else {
                res.status(201).send(JSON.stringify({
                    "messages": "Message sent."
                }));
            }

        });

    });
    producer.on('error', function(err) {
        console.log(err)
    })

});
app.listen(process.env.PORT || 3000);