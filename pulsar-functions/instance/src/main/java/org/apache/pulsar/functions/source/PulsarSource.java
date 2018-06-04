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
package org.apache.pulsar.functions.source;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageImpl;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Utils;
import org.apache.pulsar.io.core.Record;
import org.apache.pulsar.io.core.Source;
import org.jboss.util.Classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PulsarSource<T> implements Source<T> {

    private PulsarClient pulsarClient;
    private PulsarSourceConfig pulsarSourceConfig;
    private Map<String, SerDe> topicToSerDeMap = new HashMap<>();

    @Getter
    private org.apache.pulsar.client.api.Consumer inputConsumer;

    public PulsarSource(PulsarClient pulsarClient, PulsarSourceConfig pulsarConfig) {
        this.pulsarClient = pulsarClient;
        this.pulsarSourceConfig = pulsarConfig;
    }

    @Override
    public void open(Map<String, Object> config) throws Exception {
        // Setup Serialization/Deserialization
        setupSerDe();

        // Setup pulsar consumer
        ConsumerBuilder<byte[]> consumerBuilder = this.pulsarClient.newConsumer()
                .topics(new ArrayList<>(this.pulsarSourceConfig.getTopicSerdeClassNameMap().keySet()))
                .subscriptionName(this.pulsarSourceConfig.getSubscriptionName())
                .subscriptionType(this.pulsarSourceConfig.getSubscriptionType());

        if (pulsarSourceConfig.getTimeoutMs() != null) {
            consumerBuilder.ackTimeout(pulsarSourceConfig.getTimeoutMs(), TimeUnit.MILLISECONDS);
        }
        this.inputConsumer = consumerBuilder.subscribe();
    }

    @Override
    public Record<T> read() throws Exception {
        org.apache.pulsar.client.api.Message<T> message = this.inputConsumer.receive();

        String topicName;
        String partitionId;

        // If more than one topics are being read than the Message return by the consumer will be TopicMessageImpl
        // If there is only topic being read then the Message returned by the consumer wil be MessageImpl
        if (message instanceof TopicMessageImpl) {
            topicName = ((TopicMessageImpl) message).getTopicName();
            TopicMessageIdImpl topicMessageId = (TopicMessageIdImpl) message.getMessageId();
            MessageIdImpl messageId = (MessageIdImpl) topicMessageId.getInnerMessageId();
            partitionId = Long.toString(messageId.getPartitionIndex());
        } else {
            topicName = this.pulsarSourceConfig.getTopicSerdeClassNameMap().keySet().iterator().next();
            partitionId = Long.toString(((MessageIdImpl) message.getMessageId()).getPartitionIndex());
        }

        Object object;
        try {
            object = this.topicToSerDeMap.get(topicName).deserialize(message.getData());
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
                .partitionId(String.format("%s-%s", topicName, partitionId))
                .recordSequence(Utils.getSequenceId(message.getMessageId()))
                .topicName(topicName)
                .ackFunction(() -> {
                    if (pulsarSourceConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        inputConsumer.acknowledgeCumulativeAsync(message);
                    } else {
                        inputConsumer.acknowledgeAsync(message);
                    }
                }).failFunction(() -> {
                    if (pulsarSourceConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
                        throw new RuntimeException("Failed to process message: " + message.getMessageId());
                    }
                })
                .build();
        return pulsarMessage;
    }

    @Override
    public void close() throws Exception {
        if (this.inputConsumer != null) {
            this.inputConsumer.close();
        }
    }

    @VisibleForTesting
    void setupSerDe() throws ClassNotFoundException {

        Class<?> typeArg = Classes.loadClass(this.pulsarSourceConfig.getTypeClassName(),
                Thread.currentThread().getContextClassLoader());

        if (Void.class.equals(typeArg)) {
            throw new RuntimeException("Input type of Pulsar Function cannot be Void");
        }

        for (Map.Entry<String, String> entry : this.pulsarSourceConfig.getTopicSerdeClassNameMap().entrySet()) {
            String topic = entry.getKey();
            String serDeClassname = entry.getValue();
            if (serDeClassname == null || serDeClassname.isEmpty()) {
                serDeClassname = DefaultSerDe.class.getName();
            }
            SerDe serDe = InstanceUtils.initializeSerDe(serDeClassname,
                    Thread.currentThread().getContextClassLoader(), typeArg);
            this.topicToSerDeMap.put(topic, serDe);
        }

        for (SerDe serDe : this.topicToSerDeMap.values()) {
            if (serDe.getClass().getName().equals(DefaultSerDe.class.getName())) {
                if (!DefaultSerDe.IsSupportedType(typeArg)) {
                    throw new RuntimeException("Default Serde does not support " + typeArg);
                }
            } else {
                Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
                if (!typeArg.isAssignableFrom(inputSerdeTypeArgs[0])) {
                    throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                            + " function type = " + typeArg + " should be assignable from " + inputSerdeTypeArgs[0]);
                }
            }
        }
    }
}
