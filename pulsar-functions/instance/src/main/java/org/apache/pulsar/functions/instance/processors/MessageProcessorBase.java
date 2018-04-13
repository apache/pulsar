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

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.InputMessage;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;

/**
 * The base implementation of {@link MessageProcessor}.
 */
@Slf4j
abstract class MessageProcessorBase implements MessageProcessor {

    protected final PulsarClient client;
    protected final FunctionDetails functionDetails;
    protected final SubscriptionType subType;
    protected final LinkedBlockingDeque<InputMessage> processQueue;

    @Getter
    protected final Map<String, Consumer> inputConsumers;
    protected Map<String, SerDe> inputSerDe;

    protected SerDe outputSerDe;

    protected MessageProcessorBase(PulsarClient client,
                                   FunctionDetails functionDetails,
                                   SubscriptionType subType,
                                   LinkedBlockingDeque<InputMessage> processQueue) {
        this.client = client;
        this.functionDetails = functionDetails;
        this.subType = subType;
        this.processQueue = processQueue;
        this.inputConsumers = Maps.newConcurrentMap();
    }

    //
    // Input
    //

    @Override
    public void setupInput(Map<String, SerDe> inputSerDe) throws Exception {
        log.info("Setting up input with input serdes: {}", inputSerDe);
        this.inputSerDe = inputSerDe;
        for (Map.Entry<String, String> entry : functionDetails.getCustomSerdeInputsMap().entrySet()) {
            ConsumerConfiguration conf = createConsumerConfiguration(entry.getKey());
            this.inputConsumers.put(entry.getKey(), client.subscribe(entry.getKey(),
                    FunctionDetailsUtils.getFullyQualifiedName(functionDetails), conf));
        }
        for (String topicName : functionDetails.getInputsList()) {
            ConsumerConfiguration conf = createConsumerConfiguration(topicName);
            this.inputConsumers.put(topicName, client.subscribe(topicName,
                    FunctionDetailsUtils.getFullyQualifiedName(functionDetails), conf));
        }
    }

    protected SubscriptionType getSubscriptionType() {
        return subType;
    }

    protected ConsumerConfiguration createConsumerConfiguration(String topicName) {
        log.info("Starting Consumer for topic " + topicName);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(getSubscriptionType());

        SerDe inputSerde = inputSerDe.get(topicName);
        conf.setMessageListener((consumer, msg) -> {
            try {
                InputMessage message = new InputMessage();
                message.setConsumer(consumer);
                message.setInputSerDe(inputSerde);
                message.setActualMessage(msg);
                message.setTopicName(topicName);
                processQueue.put(message);
                postReceiveMessage(message);
            } catch (InterruptedException e) {
                log.error("Function container {} is interrupted on enqueuing messages",
                        Thread.currentThread().getId(), e);
            }
        });
        return conf;
    }

    /**
     * Method called when a message is received from input after being put into the process queue.
     *
     * <p>The processor implementation can make a decision to process the message based on its processing guarantees.
     * for example, an at-most-once processor can ack the message immediately.
     *
     * @param message input message.
     */
    protected void postReceiveMessage(InputMessage message) {}

    //
    // Output
    //

    @Override
    public void setupOutput(SerDe outputSerDe) throws Exception {
        this.outputSerDe = outputSerDe;

        String outputTopic = functionDetails.getOutput();
        if (outputTopic != null
                && !functionDetails.getOutput().isEmpty()
                && outputSerDe != null) {
            log.info("Starting producer for output topic {}", outputTopic);
            initializeOutputProducer(outputTopic);
        }
    }

    protected abstract void initializeOutputProducer(String outputTopic) throws Exception;

    //
    // Process
    //

    @Override
    public void prepareDequeueMessageFromProcessQueue() {}

    @Override
    public boolean prepareProcessMessage(InputMessage msg) throws InterruptedException {
        return true;
    }

    @Override
    public void handleProcessException(InputMessage msg, Exception cause) {}

    @Override
    public void close() {
        // stop the consumer first, so no more messages are coming in
        inputConsumers.forEach((k, v) -> {
            try {
                v.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close consumer to input topic {}", k, e);
            }
        });
        inputConsumers.clear();
    }
}
