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
package org.apache.pulsar.functions.sink;

import com.google.common.annotations.VisibleForTesting;

import java.util.Base64;
import java.util.Map;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.InstanceUtils;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.instance.producers.AbstractOneOuputTopicProducers;
import org.apache.pulsar.functions.instance.producers.MultiConsumersOneOuputTopicProducers;
import org.apache.pulsar.functions.instance.producers.Producers;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import net.jodah.typetools.TypeResolver;

@Slf4j
public class PulsarSink<T> implements Sink<T> {

    private PulsarClient client;
    private PulsarSinkConfig pulsarSinkConfig;
    private SerDe<T> outputSerDe;

    private PulsarSinkProcessor pulsarSinkProcessor;

    private interface PulsarSinkProcessor<T> {
        void initializeOutputProducer(String outputTopic) throws Exception;

        void sendOutputMessage(MessageBuilder outputMsgBuilder,
                               Record<T> recordContext) throws Exception;

        void close() throws Exception;
    }

    private class PulsarSinkAtMostOnceProcessor implements PulsarSinkProcessor<T> {
        private Producer<byte[]> producer;

        @Override
        public void initializeOutputProducer(String outputTopic) throws Exception {
            this.producer = AbstractOneOuputTopicProducers.createProducer(
                    client, pulsarSinkConfig.getTopic());
        }

        @Override
        public void sendOutputMessage(MessageBuilder outputMsgBuilder,
                                      Record<T> recordContext) throws Exception {
            Message<byte[]> outputMsg = outputMsgBuilder.build();
            this.producer.sendAsync(outputMsg).handle((messageId, e) -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] failed to sink message {}", recordContext.getTopicName().orElseGet(() -> null),
                            e.getMessage());
                }
                recordContext.ack();
                return null;
            });
        }

        @Override
        public void close() throws Exception {
            if (null != producer) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    log.warn("Fail to close producer for processor {}", pulsarSinkConfig.getTopic(), e);
                }
            }
        }
    }

    private class PulsarSinkAtLeastOnceProcessor implements PulsarSinkProcessor<T> {
        private Producer<byte[]> producer;

        @Override
        public void initializeOutputProducer(String outputTopic) throws Exception {
            this.producer = AbstractOneOuputTopicProducers.createProducer(
                    client, pulsarSinkConfig.getTopic());
        }

        @Override
        public void sendOutputMessage(MessageBuilder outputMsgBuilder,
                                      Record<T> recordContext) throws Exception {
            Message<byte[]> outputMsg = outputMsgBuilder.build();
            this.producer.sendAsync(outputMsg).thenAccept(messageId -> recordContext.ack());
        }

        @Override
        public void close() throws Exception {
            if (null != producer) {
                try {
                    producer.close();
                } catch (PulsarClientException e) {
                    log.warn("Fail to close producer for processor {}", pulsarSinkConfig.getTopic(), e);
                }
            }
        }
    }

    private class PulsarSinkEffectivelyOnceProcessor implements PulsarSinkProcessor<T>, ConsumerEventListener {

        @Getter(AccessLevel.PACKAGE)
        protected Producers outputProducer;

        @Override
        public void initializeOutputProducer(String outputTopic) throws Exception {
            outputProducer = new MultiConsumersOneOuputTopicProducers(client, outputTopic);
            outputProducer.initialize();
        }

        @Override
        public void sendOutputMessage(MessageBuilder outputMsgBuilder, Record<T> recordContext)
                throws Exception {

            // assign sequence id to output message for idempotent producing
            if (recordContext.getRecordSequence().isPresent()) {
                outputMsgBuilder.setSequenceId(recordContext.getRecordSequence().get());
            }

            // currently on PulsarRecord
            Producer producer = outputProducer.getProducer(recordContext.getPartitionId().get());

            org.apache.pulsar.client.api.Message outputMsg = outputMsgBuilder.build();
            producer.sendAsync(outputMsg)
                    .thenAccept(messageId -> recordContext.ack())
                    .join();
        }

        @Override
        public void close() throws Exception {
            // kill the result producer
            if (null != outputProducer) {
                outputProducer.close();
                outputProducer = null;
            }
        }

        @Override
        public void becameActive(Consumer<?> consumer, int partitionId) {
            // if the instance becomes active for a given topic partition,
            // open a producer for the results computed from this topic partition.
            if (null != outputProducer) {
                try {
                    this.outputProducer.getProducer(String.format("%s-%d", consumer.getTopic(), partitionId));
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
                this.outputProducer.closeProducer(String.format("%s-%d", consumer.getTopic(), partitionId));
            }
        }
    }

    public PulsarSink(PulsarClient client, PulsarSinkConfig pulsarSinkConfig) {
        this.client = client;
        this.pulsarSinkConfig = pulsarSinkConfig;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {

        // Setup Serialization/Deserialization
        setupSerDe();

        FunctionConfig.ProcessingGuarantees processingGuarantees = this.pulsarSinkConfig.getProcessingGuarantees();
        switch (processingGuarantees) {
            case ATMOST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtMostOnceProcessor();
                break;
            case ATLEAST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtLeastOnceProcessor();
                break;
            case EFFECTIVELY_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkEffectivelyOnceProcessor();
                break;
        }
        this.pulsarSinkProcessor.initializeOutputProducer(this.pulsarSinkConfig.getTopic());
    }

    @Override
    public void write(Record<T> record) throws Exception {

        byte[] output;
        try {
            output = this.outputSerDe.serialize(record.getValue());
        } catch (Exception e) {
            //TODO Add serialization exception stats
            throw new RuntimeException("Error occured when attempting to serialize output:", e);
        }

        MessageBuilder msgBuilder = MessageBuilder.create();
        if (record.getKey().isPresent()) {
            msgBuilder.setKey(record.getKey().get());
        }

        msgBuilder.setContent(output);

        if (!record.getProperties().isEmpty()) {
            msgBuilder.setProperties(record.getProperties());
        }

        SinkRecord<T> sinkRecord = (SinkRecord<T>) record;
        if (sinkRecord.getSourceRecord() instanceof PulsarRecord) {
            PulsarRecord<T> pulsarRecord = (PulsarRecord<T>) sinkRecord.getSourceRecord();
            // forward user properties to sink-topic
            msgBuilder.setProperty("__pfn_input_topic__", pulsarRecord.getTopicName().get()).setProperty(
                    "__pfn_input_msg_id__",
                    new String(Base64.getEncoder().encode(pulsarRecord.getMessageId().toByteArray())));
        }

        this.pulsarSinkProcessor.sendOutputMessage(msgBuilder, record);
    }

    @Override
    public void close() throws Exception {
        if (this.pulsarSinkProcessor != null) {
            this.pulsarSinkProcessor.close();
        }
    }

    @VisibleForTesting
    void setupSerDe() throws ClassNotFoundException {
        if (StringUtils.isEmpty(this.pulsarSinkConfig.getTypeClassName())) {
            this.outputSerDe = InstanceUtils.initializeDefaultSerDe(byte[].class);
            return;
        }

        Class<?> typeArg = Reflections.loadClass(this.pulsarSinkConfig.getTypeClassName(),
                Thread.currentThread().getContextClassLoader());

        if (!Void.class.equals(typeArg)) { // return type is not `Void.class`
            if (this.pulsarSinkConfig.getSerDeClassName() == null
                    || this.pulsarSinkConfig.getSerDeClassName().isEmpty()
                    || this.pulsarSinkConfig.getSerDeClassName().equals(DefaultSerDe.class.getName())) {
                this.outputSerDe = InstanceUtils.initializeDefaultSerDe(typeArg);
            } else {
                this.outputSerDe = InstanceUtils.initializeSerDe(this.pulsarSinkConfig.getSerDeClassName(),
                        Thread.currentThread().getContextClassLoader(), typeArg);
            }
            Class<?>[] outputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, outputSerDe.getClass());
            if (outputSerDe.getClass().getName().equals(DefaultSerDe.class.getName())) {
                if (!DefaultSerDe.IsSupportedType(typeArg)) {
                    throw new RuntimeException("Default Serde does not support type " + typeArg);
                }
            } else if (!outputSerdeTypeArgs[0].isAssignableFrom(typeArg)) {
                throw new RuntimeException("Inconsistent types found between function output type and output serde type: "
                        + " function type = " + typeArg + "should be assignable from " + outputSerdeTypeArgs[0]);
            }
        }
    }
}
