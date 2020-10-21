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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.FunctionResultRouter;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class PulsarSink<T> implements Sink<T> {

    private final PulsarClient client;
    private final PulsarSinkConfig pulsarSinkConfig;
    private final Map<String, String> properties;
    private final ClassLoader functionClassLoader;
    private ComponentStatsManager stats;

    @VisibleForTesting
    PulsarSinkProcessor<T> pulsarSinkProcessor;

    private final TopicSchema topicSchema;

    private interface PulsarSinkProcessor<T> {

        TypedMessageBuilder<T> newMessage(Record<T> record);

        void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record);

        void close() throws Exception;
    }

    private abstract class PulsarSinkProcessorBase implements PulsarSinkProcessor<T> {
        protected Map<String, Producer<T>> publishProducers = new ConcurrentHashMap<>();
        protected Schema schema;

        protected PulsarSinkProcessorBase(Schema schema) {
            this.schema = schema;
        }

        public Producer<T> createProducer(PulsarClient client, String topic, String producerName, Schema<T> schema)
                throws PulsarClientException {
            ProducerBuilder<T> builder = client.newProducer(schema)
                    .blockIfQueueFull(true)
                    .enableBatching(true)
                    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                    .compressionType(CompressionType.LZ4)
                    .hashingScheme(HashingScheme.Murmur3_32Hash) //
                    .messageRoutingMode(MessageRoutingMode.CustomPartition)
                    .messageRouter(FunctionResultRouter.of())
                    // set send timeout to be infinity to prevent potential deadlock with consumer
                    // that might happen when consumer is blocked due to unacked messages
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .topic(topic);
            if (producerName != null) {
                builder.producerName(producerName);
            }
            if (pulsarSinkConfig.getProducerSpec() != null) {
                if (pulsarSinkConfig.getProducerSpec().getMaxPendingMessages() != 0) {
                    builder.maxPendingMessages(pulsarSinkConfig.getProducerSpec().getMaxPendingMessages());
                }
                if (pulsarSinkConfig.getProducerSpec().getMaxPendingMessagesAcrossPartitions() != 0) {
                    builder.maxPendingMessagesAcrossPartitions(pulsarSinkConfig.getProducerSpec().getMaxPendingMessagesAcrossPartitions());
                }
            }

            return builder.properties(properties).create();
        }

        protected Producer<T> getProducer(String destinationTopic, Schema schema) {
            return getProducer(destinationTopic, null, destinationTopic, schema);
        }

        protected Producer<T> getProducer(String producerId, String producerName, String topicName, Schema schema) {
            return publishProducers.computeIfAbsent(producerId, s -> {
                try {
                    return createProducer(
                            client,
                            topicName,
                            producerName,
                            schema != null ? schema : this.schema);
                } catch (PulsarClientException e) {
                    log.error("Failed to create Producer while doing user publish", e);
                    throw new RuntimeException(e);
                }
            });
        }

        @Override
        public void close() throws Exception {
            List<CompletableFuture<Void>> closeFutures = new ArrayList<>(publishProducers.size());
            for (Map.Entry<String, Producer<T>> entry: publishProducers.entrySet()) {
                Producer<T> producer = entry.getValue();
                closeFutures.add(producer.closeAsync());
            }
            try {
                org.apache.pulsar.common.util.FutureUtil.waitForAll(closeFutures);
            } catch (Exception e) {
                log.warn("Failed to close all the producers", e);
            }
        }

        public Function<Throwable, Void> getPublishErrorHandler(Record<T> record, boolean failSource) {

            return throwable -> {
                SinkRecord<T> sinkRecord = (SinkRecord<T>) record;
                Record<T> srcRecord = sinkRecord.getSourceRecord();
                if (failSource) {
                    srcRecord.fail();
                }

                String topic = record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic());

                String errorMsg = null;
                if (srcRecord instanceof PulsarRecord) {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s] with src message id [%s]", topic, throwable.getMessage(), ((PulsarRecord) srcRecord).getMessageId());
                    log.error(errorMsg);
                } else {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s] with src sequence id [%s]", topic, throwable.getMessage(), record.getRecordSequence().get());
                    log.error(errorMsg);
                }
                stats.incrSinkExceptions(new Exception(errorMsg));
                return null;
            };
        }
    }

    @VisibleForTesting
    class PulsarSinkAtMostOnceProcessor extends PulsarSinkProcessorBase {
        public PulsarSinkAtMostOnceProcessor(Schema schema) {
            super(schema);
            // initialize default topic
            try {
                publishProducers.put(pulsarSinkConfig.getTopic(),
                        createProducer(client, pulsarSinkConfig.getTopic(), null, schema));
            } catch (PulsarClientException e) {
                log.error("Failed to create Producer while doing user publish", e);
                throw new RuntimeException(e);            }
        }

        @Override
        public TypedMessageBuilder<T> newMessage(Record<T> record) {
            if (record.getSchema() != null) {
                return getProducer(record
                        .getDestinationTopic()
                        .orElse(pulsarSinkConfig.getTopic()), record.getSchema())
                        .newMessage(record.getSchema());
            } else {
                return getProducer(record
                        .getDestinationTopic()
                        .orElse(pulsarSinkConfig.getTopic()), record.getSchema())
                        .newMessage();
            }
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) {
            msg.sendAsync().thenAccept(messageId -> {
                //no op
            }).exceptionally(getPublishErrorHandler(record, false));
        }
    }

    @VisibleForTesting
    class PulsarSinkAtLeastOnceProcessor extends PulsarSinkAtMostOnceProcessor {
        public PulsarSinkAtLeastOnceProcessor(Schema schema) {
            super(schema);
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) {
            msg.sendAsync()
                    .thenAccept(messageId -> record.ack())
                    .exceptionally(getPublishErrorHandler(record, true));
        }
    }

    @VisibleForTesting
    class PulsarSinkEffectivelyOnceProcessor extends PulsarSinkProcessorBase {

        public PulsarSinkEffectivelyOnceProcessor(Schema schema) {
            super(schema);
        }

        @Override
        public TypedMessageBuilder<T> newMessage(Record<T> record) {
            if (!record.getPartitionId().isPresent()) {
                throw new RuntimeException("PartitionId needs to be specified for every record while in Effectively-once mode");
            }

            Producer<T> producer = getProducer(
                    String.format("%s-%s",record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic()), record.getPartitionId().get()),
                    record.getPartitionId().get(),
                    record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic()),
                    record.getSchema()
            );
            if (record.getSchema() != null) {
                return producer.newMessage(record.getSchema());
            } else {
                return producer.newMessage();
            }
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, Record<T> record) {

            if (!record.getRecordSequence().isPresent()) {
                throw new RuntimeException("RecordSequence needs to be specified for every record while in Effectively-once mode");
            }

            // assign sequence id to output message for idempotent producing
            msg.sequenceId(record.getRecordSequence().get());
            CompletableFuture<MessageId> future = msg.sendAsync();

            future.thenAccept(messageId -> record.ack()).exceptionally(getPublishErrorHandler(record, true));
        }
    }

    public PulsarSink(PulsarClient client, PulsarSinkConfig pulsarSinkConfig, Map<String, String> properties,
                      ComponentStatsManager stats, ClassLoader functionClassLoader) {
        this.client = client;
        this.pulsarSinkConfig = pulsarSinkConfig;
        this.topicSchema = new TopicSchema(client);
        this.properties = properties;
        this.stats = stats;
        this.functionClassLoader = functionClassLoader;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Opening pulsar sink with config: {}", pulsarSinkConfig);

        Schema<T> schema = initializeSchema();
        if (schema == null) {
            log.info("Since output type is null, not creating any real sink");
            return;
        }

        FunctionConfig.ProcessingGuarantees processingGuarantees = this.pulsarSinkConfig.getProcessingGuarantees();
        switch (processingGuarantees) {
            case ATMOST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtMostOnceProcessor(schema);
                break;
            case ATLEAST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtLeastOnceProcessor(schema);
                break;
            case EFFECTIVELY_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkEffectivelyOnceProcessor(schema);
                break;
        }
    }

    @Override
    public void write(Record<T> record) {
        TypedMessageBuilder<T> msg = pulsarSinkProcessor.newMessage(record);

        if (record.getKey().isPresent() && !(record.getSchema() instanceof KeyValueSchema &&
                ((KeyValueSchema) record.getSchema()).getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED)) {
            msg.key(record.getKey().get());
        }

        msg.value(record.getValue());

        if (!record.getProperties().isEmpty() && pulsarSinkConfig.isForwardSourceMessageProperty()) {
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
            eventTime.ifPresent(msg::eventTime);
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

        Class<?> typeArg = Reflections.loadClass(this.pulsarSinkConfig.getTypeClassName(), functionClassLoader);
        if (Void.class.equals(typeArg)) {
            // return type is 'void', so there's no schema to check
            return null;
        }
        ConsumerConfig consumerConfig = new ConsumerConfig();
        consumerConfig.setSchemaProperties(pulsarSinkConfig.getSchemaProperties());
        if (!StringUtils.isEmpty(pulsarSinkConfig.getSchemaType())) {
            consumerConfig.setSchemaType(pulsarSinkConfig.getSchemaType());
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    consumerConfig, false);
        } else {
            consumerConfig.setSchemaType(pulsarSinkConfig.getSerdeClassName());
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    consumerConfig, false, functionClassLoader);
        }
    }
}
