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
import java.util.Map;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiTopicsConsumerImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
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

    protected Map<String, SerDe> inputSerDe;

    protected SerDe outputSerDe;

    @Getter
    protected Consumer inputConsumer;

    protected List<String> topics;

    protected MessageProcessorBase(PulsarClient client,
                                   FunctionDetails functionDetails,
                                   SubscriptionType subType) {
        this.client = client;
        this.functionDetails = functionDetails;
        this.subType = subType;
        this.topics = new LinkedList<>();
    }

    //
    // Input
    //

    @Override
    public void setupInput(Map<String, SerDe> inputSerDe) throws Exception {
        log.info("Setting up input with input serdes: {}", inputSerDe);
        this.inputSerDe = inputSerDe;
        this.topics.addAll(this.functionDetails.getCustomSerdeInputsMap().keySet());
        this.topics.addAll(this.functionDetails.getInputsList());

        this.inputConsumer = this.client.newConsumer()
                .topics(this.topics)
                .subscriptionName(FunctionDetailsUtils.getFullyQualifiedName(this.functionDetails))
                .subscriptionType(getSubscriptionType())
                .subscribe();
    }

    protected SubscriptionType getSubscriptionType() {
        return subType;
    }

    public InputMessage recieveMessage() throws PulsarClientException {
        Message message = this.inputConsumer.receive();
        String topicName;
        if (message instanceof TopicMessageImpl) {
            topicName = ((TopicMessageImpl)message).getTopicName();
        } else {
            topicName = this.topics.get(0);
        }
        InputMessage inputMessage = new InputMessage();
                inputMessage.setConsumer(inputConsumer);
                inputMessage.setInputSerDe(inputSerDe.get(topicName));
                inputMessage.setActualMessage(message);
                inputMessage.setTopicName(topicName);
        return inputMessage;
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

        try {
            this.inputConsumer.close();
        } catch (PulsarClientException e) {
            log.warn("Failed to close consumer to input topics {}",
                    ((MultiTopicsConsumerImpl) this.inputConsumer).getTopics(), e);
        }
    }
}
