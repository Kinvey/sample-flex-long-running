# Long-running Kinvey Flex Service

## Basics
This repository contains a sample implementation of a long-running Kinvey Flex Serivce.

In most cases Flex Services are used to handle a call to a custom endpoint or a collection hook. However, it is also possible to have a long-running Flex Service that does some processing on its own, unrelated to an outside request.

Please check out the documentation for more information about [long-running Flex Services](https://devcenter.kinvey.com/nativescript/guides/flex-services#long-running-scripts).

## Flex Service Idling
By default, the Kinvey runtime stops Flex Services when they become idle (there have been no requests to them for a certain time period). This does not work well with long-running services, as you want them to run indefinitely and do whatever processing they are doing without being interrupted.

You need to contact Kinvey Support to disable idling for Flex Services that you plan to use as long-running.

## Graceful Shutdown
Even if you have idling disabled, your Flex Service will sometimes be stopped and then started again. This happens because of various runtime updates or internal rebalancing of resources. It does not happen often, but you still need to handle it.

This sample Flex Service incorporates code for graceful shutdown.

## Implementation
This Flex Service uses the kafkajs module to consume and produce Kafka messages. It subscribes to a topic and then sends messages to this same topic every 1 second. The service also intercepts shutdown signals and exits gracefully, waiting for all messages that are already being processed to finish processing. A 500-2000 ms delay is introduced on purpose in the processing of the messages so that graceful shutdown can be easily understood and tested.

## Prerequisites
This Flex service requires a Kafka server to be accessible and a Kafka topic to be created.

## Testing
You can test this Flex Service locally, without deploying it to Kinvey. To do that you just start the node.js application:
> node .

Please check out the documentation for more information on [local testing](https://devcenter.kinvey.com/nativescript/guides/flexservice-runtime#testing-locally).
