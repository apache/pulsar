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
package org.apache.pulsar.functions.instance;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.connect.core.Record;
import org.apache.pulsar.connect.core.Source;
import org.apache.pulsar.functions.proto.Function;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarSource<T> implements Source<T> {

    private PulsarClient pulsarClient;
    private PulsarConfig pulsarConfig;

    @Getter
    private org.apache.pulsar.client.api.Consumer inputConsumer;

    public PulsarSource(PulsarClient pulsarClient, PulsarConfig pulsarConfig) {
        this.pulsarClient = pulsarClient;
        this.pulsarConfig = pulsarConfig;
    }

    @Override
    public void open(Map<String, Object> config) throws Exception {
        this.inputConsumer = this.pulsarClient.newConsumer()
                .topics(new ArrayList<>(this.pulsarConfig.getTopicToSerdeMap().keySet()))
                .subscriptionName(this.pulsarConfig.getSubscription())
                .subscriptionType(this.pulsarConfig.getSubscriptionType())
                .ackTimeout(1, TimeUnit.MINUTES)
                .subscribe();
    }

    @Override
    public Record<T> read() throws Exception {
        org.apache.pulsar.client.api.Message<T> message = this.inputConsumer.receive();

        String topicName;
        String partitionId;
        if (message instanceof TopicMessageImpl) {
            topicName = ((TopicMessageImpl) message).getTopicName();
            TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) message.getMessageId();
            MessageIdImpl messageId = (MessageIdImpl) topicMessageId.getInnerMessageId();
            partitionId = Long.toString(messageId.getPartitionIndex());
        } else {
            topicName = this.pulsarConfig.getTopicToSerdeMap().keySet().iterator().next();
            partitionId = Long.toString(((MessageIdImpl) message.getMessageId()).getPartitionIndex());
        }

        Object object;
        try {
            object = this.pulsarConfig.getTopicToSerdeMap().get(topicName).deserialize(message.getData());
        } catch (Exception e) {
            //TODO Add deserialization exception stats
            throw new RuntimeException("Error occured when attempting to deserialize input:", e);
        }

        T input;
        try {
            input = (T) object;
        } catch (ClassCastException e) {
            throw new RuntimeException("Error in casting input to expected type:", e);
        }

        PulsarRecord<T> pulsarMessage = (PulsarRecord<T>) PulsarRecord.builder()
                .value(input)
                .messageId(message.getMessageId())
                .partitionId(partitionId)
                .sequenceId(message.getSequenceId())
                .topicName(topicName)
                .ackFunction(() -> {
                    if (pulsarConfig.getProcessingGuarantees()
                            == Function.FunctionDetails.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        inputConsumer.acknowledgeCumulativeAsync(message);
                    } else {
                        inputConsumer.acknowledgeAsync(message);
                    }
                }).failFunction(() -> {
                    if (pulsarConfig.getProcessingGuarantees()
                            == Function.FunctionDetails.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        throw new RuntimeException("Failed to process message: " + message.getMessageId());
                    }
                })
                .build();
        return pulsarMessage;
    }

    @Override
    public void close() throws Exception {
        this.inputConsumer.close();
    }
}
