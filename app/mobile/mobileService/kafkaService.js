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

const topicNearestReply = 'nearest.reply'
const topicNearestRequest = 'nearest.request'
const topicInterpolationReply = 'interpolation.reply'
const topicInterpolationRequest = 'interpolation.request'

Producer = kafka.HighLevelProducer;
clientProducer = new kafka.KafkaClient();
producer = new Producer(clientProducer, { requireAcks: 1 });

Consumer = kafka.Consumer
clientConsumer = new kafka.KafkaClient()
clientConsumer.loadMetadataForTopics([topicNearestReply, topicInterpolationReply], (err, resp) => {
    console.log(JSON.stringify(resp))
});

consumer = new Consumer(
    clientConsumer, [
        { topic: topicNearestReply, fromOffset: 'latest'  }, { topic: topicInterpolationReply,fromOffset: 'latest'  }
    ], {
        autoCommit: true
    }
);


producer.on('ready', function() {


    app.post('/', function(req, res) {

        payloadsNearest = [
            { topic: topicNearestRequest, messages: JSON.stringify(req.body) }
        ];

        producer.send(payloadsNearest, function(err, data) {
            if (err) {
                res.status(500).send(JSON.stringify({
                    "messages": "Error while sending messages.",
                }));
            }

        });
        consumer.on('message', function(message) {
            if (message.topic == topicNearestReply) {
                console.log(message)
                payloadsInterpolation = [
                    { topic: topicInterpolationRequest, messages: message.value }
                ];
                producer.send(payloadsInterpolation, function(err, data) {
                    if (err) {
                        res.status(500).send(JSON.stringify({
                            "messages": "Error while sending messages.",
                        }));
                    }
                });
            } else if (message.topic == topicInterpolationReply) {
                res.status(200).send(message.value)
                
                
            }

        });
        

    });
   

});

producer.on('error', function(err) {
    console.log(err)
})

app.listen(process.env.PORT || 3000);