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
                             FunctionDetails functionDetails) {
        super(client, functionDetails, SubscriptionType.Failover);
    }

    /**
     * An effectively-once processor can only use `Failover` subscription.
     */
    @Override
    protected SubscriptionType getSubscriptionType() {
        return SubscriptionType.Failover;
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
    public boolean prepareProcessMessage(InputMessage msg) throws InterruptedException {
        boolean prepared = super.prepareProcessMessage(msg);
        if (prepared) {
            // if the messages are received from old consumers, we discard it since new consumer was
            // re-created for the correctness of effectively-once
            if (msg.getConsumer() != inputConsumer) {
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
                                  MessageBuilder outputMsgBuilder) throws Exception {
        if (null == outputMsgBuilder) {
            inputMsg.ackCumulative();
            return;
        }

        // assign sequence id to output message for idempotent producing
        outputMsgBuilder = outputMsgBuilder
            .setSequenceId(Utils.getSequenceId(inputMsg.getActualMessage().getMessageId()));


        Producer producer = outputProducer.getProducer(inputMsg.getTopicName(), inputMsg.getTopicPartition());

        Message outputMsg = outputMsgBuilder.build();
        producer.sendAsync(outputMsg)
                .thenAccept(messageId -> inputMsg.ackCumulative())
                .join();
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
