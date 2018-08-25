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
import java.util.Optional;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerEventListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.instance.producers.AbstractOneOuputTopicProducers;
import org.apache.pulsar.functions.instance.producers.MultiConsumersOneOuputTopicProducers;
import org.apache.pulsar.functions.instance.producers.Producers;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.FunctionConfig;
import org.apache.pulsar.functions.utils.Reflections;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

@Slf4j
public class PulsarSink<T> implements Sink<T> {

    private final PulsarClient client;
    private final PulsarSinkConfig pulsarSinkConfig;

    private PulsarSinkProcessor<T> pulsarSinkProcessor;

    private final TopicSchema topicSchema;

    private interface PulsarSinkProcessor<T> {
        void initializeOutputProducer(String outputTopic, Schema<T> schema) throws Exception;

        TypedMessageBuilder<T> newMessage(Record<T> record) throws Exception;

        void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) throws Exception;

        abstract void close() throws Exception;
    }

    private class PulsarSinkAtMostOnceProcessor implements PulsarSinkProcessor<T> {
        private Producer<T> producer;

        @Override
        public void initializeOutputProducer(String outputTopic, Schema<T> schema) throws Exception {
            this.producer = AbstractOneOuputTopicProducers.createProducer(
                    client, pulsarSinkConfig.getTopic(), schema);
        }

        @Override
        public TypedMessageBuilder<T> newMessage(Record<T> record) {
            return producer.newMessage();
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) throws Exception {
            msg.sendAsync();
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
        private Producer<T> producer;

        @Override
        public void initializeOutputProducer(String outputTopic, Schema<T> schema) throws Exception {
            this.producer = AbstractOneOuputTopicProducers.createProducer(
                    client, pulsarSinkConfig.getTopic(), schema);
        }

        @Override
        public TypedMessageBuilder<T> newMessage(Record<T> record) {
            return producer.newMessage();
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) throws Exception {
            msg.sendAsync().thenAccept(messageId -> record.ack());
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
        protected Producers<T> outputProducer;

        @Override
        public void initializeOutputProducer(String outputTopic, Schema<T> schema) throws Exception {
            outputProducer = new MultiConsumersOneOuputTopicProducers<T>(client, outputTopic, schema);
            outputProducer.initialize();
        }

        @Override
        public TypedMessageBuilder<T> newMessage(Record<T> record) throws Exception {
            // Route message to appropriate partition producer
            return outputProducer.getProducer(record.getPartitionId().get()).newMessage();
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record)
                throws Exception {

            // assign sequence id to output message for idempotent producing
            if (record.getRecordSequence().isPresent()) {
                msg.sequenceId(record.getRecordSequence().get());
            }

            msg.sendAsync()
                    .thenAccept(messageId -> record.ack())
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
        this.topicSchema = new TopicSchema(client);
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        Schema<T> schema = initializeSchema();

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
        this.pulsarSinkProcessor.initializeOutputProducer(this.pulsarSinkConfig.getTopic(), schema);
    }

    @Override
    public void write(Record<T> record) throws Exception {
        TypedMessageBuilder<T> msg = pulsarSinkProcessor.newMessage(record);
        if (record.getKey().isPresent()) {
            msg.key(record.getKey().get());
        }

        msg.value(record.getValue());

        if (!record.getProperties().isEmpty()) {
            msg.properties(record.getProperties());
        }

        SinkRecord<T> sinkRecord = (SinkRecord<T>) record;
        if (sinkRecord.getSourceRecord() instanceof PulsarRecord) {
            PulsarRecord<T> pulsarRecord = (PulsarRecord<T>) sinkRecord.getSourceRecord();
            // forward user properties to sink-topic
            msg.property("__pfn_input_topic__", pulsarRecord.getTopicName().get())
               .property("__pfn_input_msg_id__",
                         new String(Base64.getEncoder().encode(pulsarRecord.getMessageId().toByteArray())));
        } else {
            // It is coming from some source
            Optional<Long> eventTime = sinkRecord.getSourceRecord().getEventTime();
            if (eventTime.isPresent()) {
                msg.eventTime(eventTime.get());
            }
        }

        pulsarSinkProcessor.sendOutputMessage(msg, record);
    }

    @Override
    public void close() throws Exception {
        if (this.pulsarSinkProcessor != null) {
            this.pulsarSinkProcessor.close();
        }
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Schema<T> initializeSchema() throws ClassNotFoundException {
        if (StringUtils.isEmpty(this.pulsarSinkConfig.getTypeClassName())) {
            return (Schema<T>) Schema.BYTES;
        }

        Class<?> typeArg = Reflections.loadClass(this.pulsarSinkConfig.getTypeClassName(),
                Thread.currentThread().getContextClassLoader());

        if (Void.class.equals(typeArg)) {
            // return type is 'void', so there's no schema to check
            return (Schema<T>) Schema.BYTES;
        }

        if (!StringUtils.isEmpty(pulsarSinkConfig.getSchemaType())) {
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    pulsarSinkConfig.getSchemaType());
        } else {
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    pulsarSinkConfig.getSerdeClassName());
        }
    }
}
