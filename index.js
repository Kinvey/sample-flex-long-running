const {Kafka, logLevel} = require('kafkajs');
const sdk = require('kinvey-flex-sdk');

//Kafka connection configuration, for more info see https://kafka.js.org/docs/getting-started
const kafkaBrokers = ['{kafka host:port here}'];
const kafkaTopicName = '{kafka topic name here}';

var kafkaProducer;
var kafkaConsumer;
var messageIndex = 1;
var processQueue = 0;

async function init() {
	
	try {
		await initKafka();
		await initFlex();
	} catch (e) {
		console.log('Error occured while initializing. Shutting down.');
		shutdownFlexService();
	}
	
    //Handle garaceful shutdown
    process.removeAllListeners('SIGINT');
    process.removeAllListeners('SIGTERM');
    process.on('SIGINT', shutdownFlexService);
    process.on('SIGTERM', shutdownFlexService);
}

async function initKafka() {
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: kafkaBrokers,
		logLevel: logLevel.ERROR
    });
	
    await initKafkaConsumer(kafka);
	
    await initKafkaProducer(kafka);
}

async function initKafkaConsumer(kafka) {
    kafkaConsumer = kafka.consumer({
        groupId: 'test-group'
    })

        await kafkaConsumer.connect()
        console.log('Consumer connected.');

    await kafkaConsumer.subscribe({
        topic: kafkaTopicName,
        fromBeginning: false
    })
    console.log('Subscribed for topic.');

    await kafkaConsumer.run({
        eachMessage: processKafkaMessage
    });
}

async function initKafkaProducer(kafka) {
    kafkaProducer = kafka.producer()

	await kafkaProducer.connect()
	console.log('Producer connected.');

	//Send a new message every 1 second
    setInterval(sendKafkaMessage, 1000);
}

async function processKafkaMessage({topic, partition, message}) {
    processQueue++;
    console.log('Message received: ' + message.value.toString());
	
	//Simulating processing that takes some time
	const timeForProcessing = Math.floor(Math.random() * 2000) + 500;
    setTimeout(
        function () {
			console.log('Message processed: ' + message.value.toString());
			processQueue--;
		},
        timeForProcessing
	);
}

async function sendKafkaMessage() {
	if (kafkaProducer.connectionStatus) {}
	console.log(JSON.stringify(kafkaProducer, 0, 2));
	
    await kafkaProducer.send({
        topic: kafkaTopicName,
        messages: [{
                value: 'Message from a Flex Service: ' + messageIndex
            }
        ],
    });
    messageIndex++;
}

async function shutdownFlexService() {
    console.log('Shutting down gracefully...');

    //Stop consuming and producing messages
	if (kafkaConsumer) await kafkaConsumer.disconnect();
	if (kafkaProducer) await kafkaProducer.disconnect();

    //Wait for all messages to be processed and then exit
	setInterval(
        () => {
			if (processQueue > 0) {
				console.log('Waiting on messages to be processed: ' + processQueue);
			} else {
				//Exit process
				console.log('All done, bye!');
				process.exit(0);
			}
		},
        500
	);

}

async function initFlex() {
    sdk.service(
		(err, flex) => {
			//Work with Flex API here
		}
	);
}

init();