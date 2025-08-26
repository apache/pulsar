# PIP-37: Large message size handling in Pulsar

- Status: Implemented
- Author: Rajan Dhabalia
- Discussion Thread:
- Release: 2.6.0
- PR: #4400

## Motivation

We have multiple asks from users who want to publish large size messages in the data-pipeline. Most of those usecases are mainly streaming where they need message ordering in their streaming data pipeline. For example: sending large database-records which needs to be processed in order,  streaming pipeline on grid which consume raw input data from a topic, aggregate, transform and write to new topics for further processing, etc. TCP already handles such problem by splitting large packets , transferring in order and joining at the other end. So, we would like to propose a similar approach for publishing large messages in pulsar. It can be also done by transactions in pulsar but it might not be the right alternative due to multiple reasons which we will discuss here.

## Approach

Large message payloads can be split into multiple smaller chunks that can be accepted by brokers. The chunks can be stored at broker in the same way as ordinary messages are stored in the managed-ledger. The only difference is that the consumer would need to buffer the chunks and combine them into the real message when all chunks have been collected.
The chunks in the managed-ledger can be interwoven with ordinary messages.

For example,
### Usecase 1: Single producer with ordered consumer
Topic has one producer which publishes large message payload in chunks along with regular non-chunked messages. Producer first published message M1 in three chunks M1-C1, M1-C2 and M1-C3. Broker stores all 3 chunked-messages in managed-ledger and dispatches to the ordered (exclusive/fail-over) consumer in the same order. Consumer buffers all  the chunked messages in memory until it receives all the chunks,  combines them to one real message and hand over original large M1 message to the application.

![image](https://user-images.githubusercontent.com/2898254/57895169-230e0d00-77ff-11e9-808d-a04c3ef14679.png)

                              [Fig 1: One producer with ordered  (Exclusive/Failover) consumer]

### Usecase 2: Multiple producers with ordered consumer
Sometimes, data-pipeline can have multiple publishers which publish chunked messages into the single topic. In this case, broker stores all the chunked messages coming from the different publishers in the same ledger. So, all chunked of the specific message will be still in the order but might not be consecutive in the ledger.
This usecase can be still served by ordered consumer but it will create  little memory pressure at consumer because now, consumer has to keep separate buffer for each large-message to aggregate all chunked of the large message and combine them to one real message.
So, one of the drawbacks is that consumer has to maintain multiple buffers into memory but number of buffers are same as number of publishers.

If we compare chunked messages  with transaction in pulsar then chunked message has limited life-cycle where at a time only one transaction can exist for a producer. So, we know that consumer has to deal with only one transaction coming from each producer at anytime so, if consumer is handling transaction-aggregation instead of broker (as per PIP-31) then consumer doesn’t have to much worry about the memory-pressure because there are not many concurrent transactions happening on the topic.

The main difference between this approach and PIP-31(Txn) is assembling of messages happen at consumer side instead at broker without much worrying about memory-pressure at consumer.


![image](https://user-images.githubusercontent.com/2898254/57895200-4cc73400-77ff-11e9-9edf-a4e1c202cddf.png)

                            [Fig 2: Multiple producer with ordered  (Exclusive/Failover) consumer]


### Usecase 3: Multiple producers with shared consumers

We discussed how message chunking works without any broker changes when there is a single ordered consumer consumes messages published by  single/multiple publishers. In this section we will discuss how it works with shared consumers.


#### Option 1: Broker caches mapping of message-UUID and consumerId

Message chunking/split and joins requires all chunks related to one message must be delivered to one consumer. So, in the case of shared consumers we need broker change where broker reads message metadata before dispatching the messages. Broker keeps a sorted list of shared consumer based on consumer connected time and based on message-id hash , broker selects a consumer to send all chunks attached to that message-id. Broker also keeps track of hash-id for on-fly messages which will avoid conflict when new consumer connects to broker and joins the party. 

Broker can also use [PersistentStickyKeyDispatcherMultipleConsumers](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/persistent/PersistentStickyKeyDispatcherMultipleConsumers.java) to dispatch chunked messages of specific original message by considering message-uuid as Key.

**Cons:**
This approach will not work very well in case of redelivery. In case of redelivery, broker keeps redelivered messages into unsorted map and it will be difficult and expensive for broker to figure out all chunked messages of the original message and sort them into order.



![image](https://user-images.githubusercontent.com/2898254/57895228-741e0100-77ff-11e9-8feb-334ebec83f4a.png)

                                   [Fig 3: Multiple producer with shared  consumer]



#### Option 2: Producer publish marker message after publishing all chunked messages.

One of the main issues in Option 1 was to manage dispatching of redelivered messages which are stored into unsorted map. 
To solve this problem, producer will publish a marker-message with all list of chunked messages's messageIds(ledgerId,entryId) once all the chunked messages are published successfully. There will not be any change at broker while persisting these messages and they will be persisted into the same ledger with other messages.

However, while dispatching messages, broker will read metadata of the message and skip the message if it is chunked message. Broker will deliver these chunked messages when it is dispatching appropriate marker-message attached to those chunked messages. When broker is trying to dispatch maker-message it will deserialize payload of that marker message and retrieve list of chunked message-ids present into the payload. Broker reads those list of chunked messages and dispatch them together to one specific consumer in order. Consumer will stitch these messages together, process the original message and ack all the chunked messages along with the marker message.

Eg:
1. Producer publish large message M1 into two chunks M1-C1 and M1-C2.

2. After successfully publishing M1-C1 and M1-C2, producer will publish marker message M1-Marker1 with payload will have M1-C1 and M1-C2 messages with their messageIds assigned by broker(ledgerId and entryId).

3. Broker will read M1-C1 and M2-C2 messages while dispatching and broker will skip them and not deliver immediately to any consumer.

4. Now, when Broker will dispatch marker message M1-Marker1, broker will read the payload and retrieve list of chunked message-ids of M1-C1 and M1-C2. Broker will choose one of the available consumers and dispatch all chunked messages of M1 along with Marker message.

5. Consumer handles chunked and marker messages, stitch chunked messages and create original message and place into receiver queue.

6. Once the original message is processed successfully by consumer, consumer will ack all the chunked messages along with marker messages.


![image](https://user-images.githubusercontent.com/2898254/58521849-c4d41900-8172-11e9-963e-1868d845ef8a.png)



## Chunking on large-message Vs Txn with large-message

1. Txn approach requires lot of new enhancement at broker which requires extra service for txn-coordinator, extra CPU to read over txn-buffer for each txn-msg, extra memory to maintain txn-buffer, extra metadata for new txn-partition. And it might not be convenient to deploy txn-service for any specialized system which serves 1M topics with large traffic.

2. Chunking only requires minimal changes at client side and doesn’t create any cost at broker side. Consumer is the only module which has to pay the cost in terms of memory while building buffer but that is also limited by number of publishers on the topic.

3. Chunking is the alternative of transactions?
- No. chunking is one of the useacse of transaction. chunking is a short-lived transaction where number of concurrent chunking-txn is limited by number of publishers. But chunking can’t replace all txn-usecases with large session-time and large number of concurrent txns on the topic because client can’t afford memory to handle such usecase.



## Client and Broker changes for Non-shared subscription:

### Client changes:
There will not be any change require into producer and consumer api. 

Producer will split the original message into chunks and publish them with chunked metadata. Producer will have configuration `chunkingEnabled` to enable chunking if message payload size is larger than broker can support. If we want to support non-shared subscription then producer have to publish marker message along with chunked messages as we have discussed in the previous section.

Consumer consumes the chunked messages and buffer them until it receives all the chunks of a message and finally consumer stitch them together and places into receiver-queue so, application can consume message from there. Once, consumer consumes entire large message and acks it, consumer internally sends acknowledgement all the chunk messages associated to that large message.

Consumer will have configuration: `maxPendingChuckedMessage` to allow only configured number of chunked buffers into memory to avoid memory pressure in client application. Once, consumer reaches this threshold, it discards oldest buffer from the memory and marks them for redelivery to consume them later.
Protocol changes:

*SEND command:*
will have new fields under metadata to pass chunking metadata from producer to consumer
```
optional string uuid; // Original message uuid that will be same for all the chunks
optional int32 num_chunks_from_msg;
optional int32 total_chunk_msg_size; 
optional int32 chunk_id;
```

### Broker changes:

#### Non-Shared consumer
Broker doesn’t require any changes to support chunking for non-shared subscription. Broker only records chunked message rate on the topic for monitoring purpose.

#### Shared consumer
Broker requires changes when it has to support chunking for non-shared subscription. In that case, broker skips chunked message delivery immediately and dispatch those messages when broker reads marker message associated to those chunked messages.
