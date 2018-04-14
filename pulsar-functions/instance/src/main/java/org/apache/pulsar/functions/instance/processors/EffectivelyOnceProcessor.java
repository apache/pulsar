/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.functions.instance.processors;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.instance.InputMessage;
import org.apache.pulsar.functions.instance.producers.MultiConsumersOneOuputTopicProducers;
import org.apache.pulsar.functions.instance.producers.Producers;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Utils;

/**
 * A message processor that process messages effectively-once.
 */
@Slf4j
class EffectivelyOnceProcessor extends MessageProcessorBase implements ConsumerEventListener {

    private LinkedList<String> inputTopicsToResubscribe = null;

    @Getter(AccessLevel.PACKAGE)
    protected Producers outputProducer;

    EffectivelyOnceProcessor(PulsarClient client,
                             FunctionDetails functionDetails,
                             LinkedBlockingDeque<InputMessage> processQueue) {
        super(client, functionDetails, SubscriptionType.Failover, processQueue);
    }

    /**
     * An effectively-once processor can only use `Failover` subscription.
     */
    @Override
    protected SubscriptionType getSubscriptionType() {
        return SubscriptionType.Failover;
    }

    @Override
    protected ConsumerConfiguration createConsumerConfiguration(String topicName) {
        ConsumerConfiguration conf = super.createConsumerConfiguration(topicName);
        // for effectively-once processor, register a consumer event listener to react to active consumer changes.
        conf.setConsumerEventListener(this);
        return conf;
    }

    @Override
    public void becameActive(Consumer<?> consumer, int partitionId) {
        // if the instance becomes active for a given topic partition,
        // open a producer for the results computed from this topic partition.
        if (null != outputProducer) {
            try {
                this.outputProducer.getProducer(consumer.getTopic(), partitionId);
            } catch (PulsarClientException e) {
                // this can be ignored, because producer can be lazily created when accessing it.
                log.warn("Fail to create a producer for results computed from messages of topic: {}, partition: {}",
                    consumer.getTopic(), partitionId);
            }
        }
    }

    @Override
    public void becameInactive(Consumer<?> consumer, int partitionId) {
        if (null != outputProducer) {
            // if I lost the ownership of a partition, close its corresponding topic partition.
            // this is to allow the new active consumer be able to produce to the result topic.
            this.outputProducer.closeProducer(consumer.getTopic(), partitionId);
        }
    }

    @Override
    protected void initializeOutputProducer(String outputTopic) throws Exception {
        outputProducer = new MultiConsumersOneOuputTopicProducers(client, outputTopic);
        outputProducer.initialize();
    }

    //
    // Methods to process messages
    //

    @Override
    public void prepareDequeueMessageFromProcessQueue() {
        super.prepareDequeueMessageFromProcessQueue();
        // some src topics might be put into resubscribe list because of processing failure
        // so this is the chance to resubscribe to those topics.
        resubscribeTopicsIfNeeded();
    }

    @Override
    public boolean prepareProcessMessage(InputMessage msg) throws InterruptedException {
        boolean prepared = super.prepareProcessMessage(msg);
        if (prepared) {
            // if the messages are received from old consumers, we discard it since new consumer was
            // re-created for the correctness of effectively-once
            if (msg.getConsumer() != inputConsumers.get(msg.getTopicName())) {
                return false;
            }

            if (null != outputProducer) {
                // before processing the message, we have a producer connection setup for producing results.
                Producer producer = null;
                while (null == producer) {
                    try {
                        producer = outputProducer.getProducer(msg.getTopicName(), msg.getTopicPartition());
                    } catch (PulsarClientException e) {
                        // `ProducerBusy` is thrown when an producer with same name is still connected.
                        // This can happen when a active consumer is changed for a given input topic partition
                        // so we need to wait until the old active consumer release the produce connection.
                        if (!(e instanceof ProducerBusyException)) {
                            log.error("Failed to get a producer for producing results computed from input topic {}",
                                msg.getTopicName());
                        }
                        TimeUnit.MILLISECONDS.sleep(500);
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void sendOutputMessage(InputMessage inputMsg,
                                  MessageBuilder outputMsgBuilder) {
        if (null == outputMsgBuilder) {
            inputMsg.ackCumulative();
            return;
        }

        // assign sequence id to output message for idempotent producing
        outputMsgBuilder = outputMsgBuilder
            .setSequenceId(Utils.getSequenceId(inputMsg.getActualMessage().getMessageId()));

        Producer producer;
        try {
            producer = outputProducer.getProducer(inputMsg.getTopicName(), inputMsg.getTopicPartition());
        } catch (PulsarClientException e) {
            log.error("Failed to get a producer for producing results computed from input topic {}",
                inputMsg.getTopicName());

            // if we fail to get a producer, put this message back to queue and reprocess it.
            processQueue.offerFirst(inputMsg);
            return;
        }

        Message outputMsg = outputMsgBuilder.build();
        producer.sendAsync(outputMsg)
            .thenAccept(messageId -> inputMsg.ackCumulative())
            .exceptionally(cause -> {
                log.error("Failed to send the process result {} of message {} to output topic {}",
                    outputMsg, inputMsg, functionDetails.getOutput(), cause);
                handleProcessException(inputMsg.getTopicName());
                return null;
            });
    }

    @Override
    public void handleProcessException(InputMessage msg, Exception cause) {
        handleProcessException(msg.getTopicName());
    }

    private void handleProcessException(String srcTopic) {
        // if the src message is coming from a shared subscription,
        // we don't need any special logic on handling failures, just don't ack.
        // the message will be redelivered to other consumer.
        //
        // BUT, if the src message is coming from a failover subscription,
        // we need to stop processing messages and recreate consumer to reprocess
        // the message. otherwise we might break the correctness of effectively-once
        //
        // in this case (effectively-once), we need to close the consumer
        // release the partition and open the consumer again. so we guarantee
        // that we always process messages in order
        //
        // but this is in pulsar's callback, so add this to a retry list. so we can
        // retry on java instance's main thread.
        addTopicToResubscribeList(srcTopic);
    }

    private synchronized void addTopicToResubscribeList(String topicName) {
        if (null == inputTopicsToResubscribe) {
            inputTopicsToResubscribe = new LinkedList<>();
        }
        inputTopicsToResubscribe.add(topicName);
    }

    private void resubscribeTopicsIfNeeded() {
        List<String> topicsToResubscribe;
        synchronized (this) {
            topicsToResubscribe = inputTopicsToResubscribe;
            inputTopicsToResubscribe = null;
        }
        if (null != topicsToResubscribe) {
            for (String topic : topicsToResubscribe) {
                resubscribe(topic);
            }
        }
    }

    private void resubscribe(String srcTopic) {
        // if we can not produce a message to output topic, then close the consumer of the src topic
        // and retry to instantiate a consumer again.
        Consumer consumer = inputConsumers.remove(srcTopic);
        if (consumer != null) {
            // TODO (sijie): currently we have to close the entire consumer for a given topic. However
            //               ideally we should do this in a finer granularity - we can close consumer
            //               on a given partition, without impact other partitions.
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                log.error("Failed to close consumer for input topic {} when handling produce exceptions",
                    srcTopic, e);
            }
        }
        // subscribe to the src topic again
        ConsumerConfiguration conf = createConsumerConfiguration(srcTopic);
        try {
            inputConsumers.put(
                srcTopic,
                client.subscribe(
                    srcTopic,
                    FunctionDetailsUtils.getFullyQualifiedName(functionDetails),
                    conf
                ));
        } catch (PulsarClientException e) {
            log.error("Failed to resubscribe to input topic {}. Added it to retry list and retry it later",
                srcTopic, e);
            addTopicToResubscribeList(srcTopic);
        }
    }

    @Override
    public void close() {
        super.close();
        // kill the result producer
        if (null != outputProducer) {
            outputProducer.close();
            outputProducer = null;
        }
    }
}
