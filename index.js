const { Kafka, logLevel } = require('kafkajs');
const sdk = require('kinvey-flex-sdk');

// Kafka connection configuration, for more info see https://kafka.js.org/docs/getting-started
const kafkaBrokers = ['{kafka host:port here}'];
const kafkaTopicName = '{kafka topic name here}';

let kafkaProducer;
let kafkaConsumer;
let messageIndex = 1;
let processQueue = 0;
let msgProducerIntervalId;
let isShuttingDown = false;

const handlersByEventName = {};

async function init() {
  try {
    await initKafka();
    await initFlex();

    attachGracefulShutdownHandlers();
  } catch (error) {
    console.log(`Error occured while initializing. Shutting down. Error: ${error.stack}`);
    shutDownGracefully({ cause: error });
  }
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
  });

  await kafkaConsumer.connect();
  console.log('Consumer connected.');

  await kafkaConsumer.subscribe({
    topic: kafkaTopicName,
    fromBeginning: false
  });

  console.log('Subscribed to topic.');

  await kafkaConsumer.run({
    eachMessage: processKafkaMessage
  });
}

async function initKafkaProducer(kafka) {
  kafkaProducer = kafka.producer()

  await kafkaProducer.connect()
  console.log('Producer connected.');

  // Send a new message every 1 second
  msgProducerIntervalId = setInterval(sendKafkaMessage, 1000);
}

async function processKafkaMessage({ message }) {
  processQueue++;
  console.log(`Message received: ${message.value}`);

  // Simulating processing that takes some time
  const timeForProcessing = Math.round(Math.random() * 7000) + 500;
  setTimeout(
    () => {
      console.log(`Message processed: ${message.value}`);
      processQueue--;
    },
    timeForProcessing
  );
}

async function sendKafkaMessage() {
  await kafkaProducer.send({
    topic: kafkaTopicName,
    messages: [
      { value: `Message from a Flex Service: ${messageIndex}` }
    ],
  });

  messageIndex++;
}

// Stop consuming and producing messages
async function uninitializeKafka() {
  if (kafkaConsumer) await kafkaConsumer.disconnect();
  if (kafkaProducer) await kafkaProducer.disconnect();
  if (msgProducerIntervalId) clearInterval(msgProducerIntervalId);
}

// Wait for all messages to be processed
async function awaitQueueDrain() {
  console.log(`Awaiting processing queue to drain. Messages in queue: ${processQueue}`);

  return new Promise((resolve) => {
    const intervalId = setInterval(() => {
      if (processQueue > 0) {
        console.log(`Messages in queue: ${processQueue}`);
        return;
      }
      console.log('All messages processed!');
      clearInterval(intervalId);
      resolve();
    }, 1000);
  });
}

async function shutDownGracefully({ eventName, cause }) {
  if (isShuttingDown) {
    console.log('Already shutting down. Awaiting uninitialization logic to complete...');
    return;
  }

  isShuttingDown = true;
  console.log('Shutting down gracefully...');

  await uninitializeKafka();
  await awaitQueueDrain();

  if (handlersByEventName[eventName]) {
    handlersByEventName[eventName].forEach(handler => handler(cause));
  }
}

/**
 * Flex SDK attaches graceful shutdown handlers which we want to run, so the Flex service itself uninitializes
 * and stops accepting incoming requests, but we want to uninitialize our long-running (Kafka) processing logic before that,
 * so that Flex SDK doesn't make the process exit before we are done.
 * It's important to stop accepting new processing tasks at the point of long-running task uninitialization.
 * We have planned improvements on the graceful shutdown front for long-running tasks, but this is the current state.
 */
function attachGracefulShutdownHandlers() {
  ['SIGTERM', 'SIGINT', 'unhandledRejection', 'uncaughtException'].forEach((eventName) => {
    handlersByEventName[eventName] = process.listeners(eventName);
    process.removeAllListeners(eventName);
    process.on(eventName, async eventData => {
      const isAnUnhandledError = !eventName.startsWith('SIG');
      if (isAnUnhandledError) {
        console.log(`An ${eventName} occurred: ${eventData}`);
      }
      await shutDownGracefully({ eventName, cause: eventData });
      if (isAnUnhandledError) { // trigger the Flex SDK graceful shutdown manually
        handlersByEventName.SIGINT.forEach(handler => handler('SIGINT'));
      }
    });
  });
}

async function initFlex() {
  return new Promise((resolve, reject) => {
    sdk.service((err, flex) => {
      if (err) return reject(err);
      // Work with Flex API here
      resolve();
    });
  });
}

init();
