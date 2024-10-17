/*
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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.AbstractSinkRecord;
import org.apache.pulsar.functions.instance.ProducerBuilderFactory;
import org.apache.pulsar.functions.instance.ProducerCache;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

@Slf4j
public class PulsarSink<T> implements Sink<T> {

    private final PulsarClient client;
    private final PulsarSinkConfig pulsarSinkConfig;
    private final Map<String, String> properties;
    private final ClassLoader functionClassLoader;
    private ComponentStatsManager stats;
    private final ProducerCache producerCache;

    @VisibleForTesting
    PulsarSinkProcessor<T> pulsarSinkProcessor;

    private final TopicSchema topicSchema;
    private Schema<T> schema;
    private ProducerBuilderFactory producerBuilderFactory;

    private interface PulsarSinkProcessor<T> {

        TypedMessageBuilder<T> newMessage(AbstractSinkRecord<T> record);

        void sendOutputMessage(TypedMessageBuilder<T> msg, AbstractSinkRecord<T> record);

        void close() throws Exception;
    }

    abstract class PulsarSinkProcessorBase implements PulsarSinkProcessor<T> {
        protected Producer<T> getProducer(String destinationTopic, Schema schema) {
            return getProducer(destinationTopic, schema, null, null);
        }

        protected Producer<T> getProducer(String topicName, Schema schema, String producerName, String partitionId) {
            return producerCache.getOrCreateProducer(ProducerCache.CacheArea.SINK_RECORD_CACHE, topicName, partitionId,
                    () -> {
                        Producer<T> producer = createProducer(topicName, schema, producerName);
                        log.info(
                                "Initialized producer with name '{}' on topic '{}' with schema {} partitionId {} "
                                        + "-> {}",
                                producerName, topicName, schema, partitionId, producer);
                        return producer;
                    });
        }

        @Override
        public void close() throws Exception {
            // no op
        }

        public Function<Throwable, Void> getPublishErrorHandler(AbstractSinkRecord<T> record, boolean failSource) {

            return throwable -> {
                Record<?> srcRecord = record.getSourceRecord();
                if (failSource) {
                    srcRecord.fail();
                }

                String topic = record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic());

                String errorMsg;
                if (srcRecord instanceof PulsarRecord) {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s] with src message id [%s]",
                            topic, throwable.getMessage(), ((PulsarRecord) srcRecord).getMessageId());
                } else {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s]", topic,
                            throwable.getMessage());
                    if (record.getRecordSequence().isPresent()) {
                        errorMsg = String.format(errorMsg + " with src sequence id [%s]",
                                record.getRecordSequence().get());
                    }
                }
                log.error(errorMsg);
                stats.incrSinkExceptions(new Exception(errorMsg));
                return null;
            };
        }
    }

    @VisibleForTesting
    class PulsarSinkAtMostOnceProcessor extends PulsarSinkProcessorBase {
        public PulsarSinkAtMostOnceProcessor() {
            if (!(schema instanceof AutoConsumeSchema)) {
                // initialize default topic
                getProducer(pulsarSinkConfig.getTopic(), schema);
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("The Pulsar producer is not initialized until the first record is"
                        + " published for `AUTO_CONSUME` schema.");
                }
            }
        }

        @Override
        public TypedMessageBuilder<T> newMessage(AbstractSinkRecord<T> record) {
            Schema<T> schemaToWrite = record.getSchema();
            if (!record.shouldSetSchema()) {
                // we are receiving data directly from another Pulsar topic
                // and the Function return type is not a Record
                // we must use the destination topic schema
                schemaToWrite = schema;
            }

            if (schemaToWrite != null) {
                return getProducer(record
                        .getDestinationTopic()
                        .orElse(pulsarSinkConfig.getTopic()), schemaToWrite)
                        .newMessage(schemaToWrite);
            } else {
                return getProducer(record
                        .getDestinationTopic()
                        .orElse(pulsarSinkConfig.getTopic()), null)
                        .newMessage();
            }
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, AbstractSinkRecord<T> record) {
            msg.sendAsync().thenAccept(messageId -> {
                //no op
            }).exceptionally(getPublishErrorHandler(record, false));
        }
    }

    @VisibleForTesting
    class PulsarSinkAtLeastOnceProcessor extends PulsarSinkAtMostOnceProcessor {
        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, AbstractSinkRecord<T> record) {
            msg.sendAsync()
                    .thenAccept(messageId -> record.ack())
                    .exceptionally(getPublishErrorHandler(record, true));
        }
    }

    @VisibleForTesting
    class PulsarSinkManualProcessor extends PulsarSinkAtMostOnceProcessor {
        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, AbstractSinkRecord<T> record) {
            super.sendOutputMessage(msg, record);
        }
    }

    @VisibleForTesting
    class PulsarSinkEffectivelyOnceProcessor extends PulsarSinkProcessorBase {
        @Override
        public TypedMessageBuilder<T> newMessage(AbstractSinkRecord<T> record) {
            if (!record.getPartitionId().isPresent()) {
                throw new RuntimeException(
                        "PartitionId needs to be specified for every record while in Effectively-once mode");
            }
            Schema<T> schemaToWrite = record.getSchema();
            if (!record.shouldSetSchema()) {
                // we are receiving data directly from another Pulsar topic
                // and the Function return type is not a Record
                // we must use the destination topic schema
                schemaToWrite = schema;
            }
            String topicName = record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic());
            String partitionId = record.getPartitionId().get();
            String producerName = partitionId;
            Producer<T> producer = getProducer(topicName, schemaToWrite, producerName, partitionId);
            if (schemaToWrite != null) {
                return producer.newMessage(schemaToWrite);
            } else {
                return producer.newMessage();
            }
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, AbstractSinkRecord<T> record) {

            if (!record.getRecordSequence().isPresent()) {
                throw new RuntimeException(
                        "RecordSequence needs to be specified for every record while in Effectively-once mode");
            }

            // assign sequence id to output message for idempotent producing
            msg.sequenceId(record.getRecordSequence().get());
            CompletableFuture<MessageId> future = msg.sendAsync();

            future.thenAccept(messageId -> record.ack()).exceptionally(getPublishErrorHandler(record, true));
        }
    }

    public PulsarSink(PulsarClient client, PulsarSinkConfig pulsarSinkConfig, Map<String, String> properties,
                      ComponentStatsManager stats, ClassLoader functionClassLoader, ProducerCache producerCache) {
        this.client = client;
        this.pulsarSinkConfig = pulsarSinkConfig;
        this.topicSchema = new TopicSchema(client, functionClassLoader);
        this.properties = properties;
        this.stats = stats;
        this.functionClassLoader = functionClassLoader;
        this.producerCache = producerCache;
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("Opening pulsar sink with config: {}", pulsarSinkConfig);

        schema = initializeSchema();
        if (schema == null) {
            log.info("Since output type is null, not creating any real sink");
            return;
        }
        producerBuilderFactory =
                new ProducerBuilderFactory(client, pulsarSinkConfig.getProducerConfig(), functionClassLoader, null);

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
            case MANUAL:
                this.pulsarSinkProcessor = new PulsarSinkManualProcessor();
                break;
        }
    }

    @Override
    public void write(Record<T> record) {
        AbstractSinkRecord<T> sinkRecord = (AbstractSinkRecord<T>) record;
        TypedMessageBuilder<T> msg = pulsarSinkProcessor.newMessage(sinkRecord);

        if (record.getKey().isPresent() && !(record.getSchema() instanceof KeyValueSchema
                && ((KeyValueSchema) record.getSchema()).getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED)) {
            msg.key(record.getKey().get());
        }

        msg.value(record.getValue());

        if (!record.getProperties().isEmpty()
            && (sinkRecord.shouldAlwaysSetMessageProperties() || pulsarSinkConfig.isForwardSourceMessageProperty())) {
            msg.properties(record.getProperties());
        }

        if (sinkRecord.getSourceRecord() instanceof PulsarRecord) {
            PulsarRecord<T> pulsarRecord = (PulsarRecord<T>) sinkRecord.getSourceRecord();
            // forward user properties to sink-topic
            msg.property("__pfn_input_topic__", pulsarRecord.getTopicName().get())
                    .property("__pfn_input_msg_id__",
                            new String(Base64.getEncoder().encode(pulsarRecord.getMessageId().toByteArray()),
                                    StandardCharsets.UTF_8));
        } else {
            // It is coming from some source
            Optional<Long> eventTime = sinkRecord.getSourceRecord().getEventTime();
            eventTime.ifPresent(msg::eventTime);
        }

        pulsarSinkProcessor.sendOutputMessage(msg, sinkRecord);
    }

    @Override
    public void close() throws Exception {
        if (this.pulsarSinkProcessor != null) {
            this.pulsarSinkProcessor.close();
        }
    }

    Producer<T> createProducer(String topicName, Schema<T> schema, String producerName) {
        Schema<T> schemaToUse = schema != null ? schema : this.schema;
        try {
            log.info("Initializing producer {} on topic {} with schema {}", producerName, topicName, schemaToUse);
            return producerBuilderFactory.createProducerBuilder(topicName, schemaToUse, producerName)
                    .properties(properties)
                    .create();
        } catch (PulsarClientException e) {
            throw new RuntimeException("Failed to create Producer for topic " + topicName
                    + " producerName " + producerName + " schema " + schemaToUse, e);
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
            if (GenericRecord.class.isAssignableFrom(typeArg)) {
                consumerConfig.setSchemaType(SchemaType.AUTO_CONSUME.toString());
                SchemaType configuredSchemaType = SchemaType.valueOf(pulsarSinkConfig.getSchemaType());
                if (SchemaType.AUTO_CONSUME != configuredSchemaType) {
                    log.info("The configured schema type {} is not able to write GenericRecords."
                        + " So overwrite the schema type to be {}", configuredSchemaType, SchemaType.AUTO_CONSUME);
                }
            } else {
                consumerConfig.setSchemaType(pulsarSinkConfig.getSchemaType());
            }
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    consumerConfig, false);
        } else {
            consumerConfig.setSchemaType(pulsarSinkConfig.getSerdeClassName());
            return (Schema<T>) topicSchema.getSchema(pulsarSinkConfig.getTopic(), typeArg,
                    consumerConfig, false, functionClassLoader);
        }
    }


}
