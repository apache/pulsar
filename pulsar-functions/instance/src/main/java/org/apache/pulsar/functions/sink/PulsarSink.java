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

import static org.apache.commons.lang.StringUtils.isEmpty;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.ProducerCryptoFailureAction;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.CryptoConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.FunctionResultRouter;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.functions.utils.CryptoUtils;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

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

        TypedMessageBuilder<T> newMessage(SinkRecord<T> record);

        void sendOutputMessage(TypedMessageBuilder<T> msg, SinkRecord<T> record);

        void close() throws Exception;
    }

    abstract class PulsarSinkProcessorBase implements PulsarSinkProcessor<T> {
        protected Map<String, Producer<T>> publishProducers = new ConcurrentHashMap<>();
        protected Schema schema;
        protected Crypto crypto;

        protected PulsarSinkProcessorBase(Schema schema, Crypto crypto) {
            this.schema = schema;
            this.crypto = crypto;
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
            if (pulsarSinkConfig.getProducerConfig() != null) {
                ProducerConfig producerConfig = pulsarSinkConfig.getProducerConfig();
                if (producerConfig.getMaxPendingMessages() != 0) {
                    builder.maxPendingMessages(producerConfig.getMaxPendingMessages());
                }
                if (producerConfig.getMaxPendingMessagesAcrossPartitions() != 0) {
                    builder.maxPendingMessagesAcrossPartitions(producerConfig.getMaxPendingMessagesAcrossPartitions());
                }
                if (producerConfig.getCryptoConfig() != null) {
                    builder.cryptoKeyReader(crypto.keyReader);
                    builder.cryptoFailureAction(crypto.failureAction);
                    for (String encryptionKeyName : crypto.getEncryptionKeys()) {
                        builder.addEncryptionKey(encryptionKeyName);
                    }
                }
                if (producerConfig.getBatchBuilder() != null) {
                    if (producerConfig.getBatchBuilder().equals("KEY_BASED")) {
                        builder.batcherBuilder(BatcherBuilder.KEY_BASED);
                    } else {
                        builder.batcherBuilder(BatcherBuilder.DEFAULT);
                    }
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
                    log.info("Initializing producer {} on topic {} with schema {}",
                        producerName, topicName, schema);
                    Producer<T> producer = createProducer(
                            client,
                            topicName,
                            producerName,
                            schema != null ? schema : this.schema);
                    log.info("Initialized producer {} on topic {} with schema {}: {} -> {}",
                        producerName, topicName, schema, producerId, producer);
                    return producer;
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

        public Function<Throwable, Void> getPublishErrorHandler(SinkRecord<T> record, boolean failSource) {

            return throwable -> {
                Record<T> srcRecord = record.getSourceRecord();
                if (failSource) {
                    srcRecord.fail();
                }

                String topic = record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic());

                String errorMsg = null;
                if (srcRecord instanceof PulsarRecord) {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s] with src message id [%s]", topic, throwable.getMessage(), ((PulsarRecord) srcRecord).getMessageId());
                } else {
                    errorMsg = String.format("Failed to publish to topic [%s] with error [%s]", topic, throwable.getMessage());
                    if (record.getRecordSequence().isPresent()) {
                        errorMsg = String.format(errorMsg + " with src sequence id [%s]", record.getRecordSequence().get());
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
        public PulsarSinkAtMostOnceProcessor(Schema schema, Crypto crypto) {
            super(schema, crypto);
            if (!(schema instanceof AutoConsumeSchema)) {
                // initialize default topic
                try {
                    publishProducers.put(pulsarSinkConfig.getTopic(),
                        createProducer(client, pulsarSinkConfig.getTopic(), null, schema));
                } catch (PulsarClientException e) {
                    log.error("Failed to create Producer while doing user publish", e);
                    throw new RuntimeException(e);
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("The Pulsar producer is not initialized until the first record is"
                        + " published for `AUTO_CONSUME` schema.");
                }
            }
        }

        @Override
        public TypedMessageBuilder<T> newMessage(SinkRecord<T> record) {
            Schema<T> schemaToWrite = record.getSchema();
            if (record.getSourceRecord() instanceof PulsarRecord) {
                // we are receiving data directly from another Pulsar topic
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
        public void sendOutputMessage(TypedMessageBuilder<T> msg, SinkRecord<T> record) {
            msg.sendAsync().thenAccept(messageId -> {
                //no op
            }).exceptionally(getPublishErrorHandler(record, false));
        }
    }

    @VisibleForTesting
    class PulsarSinkAtLeastOnceProcessor extends PulsarSinkAtMostOnceProcessor {
        public PulsarSinkAtLeastOnceProcessor(Schema schema, Crypto crypto) {
            super(schema, crypto);
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, SinkRecord<T> record) {
            msg.sendAsync()
                    .thenAccept(messageId -> record.ack())
                    .exceptionally(getPublishErrorHandler(record, true));
        }
    }

    @VisibleForTesting
    class PulsarSinkEffectivelyOnceProcessor extends PulsarSinkProcessorBase {

        public PulsarSinkEffectivelyOnceProcessor(Schema schema, Crypto crypto) {
            super(schema, crypto);
        }

        @Override
        public TypedMessageBuilder<T> newMessage(SinkRecord<T> record) {
            if (!record.getPartitionId().isPresent()) {
                throw new RuntimeException("PartitionId needs to be specified for every record while in Effectively-once mode");
            }
            Schema<T> schemaToWrite = record.getSchema();
            if (record.getSourceRecord() instanceof PulsarRecord) {
                // we are receiving data directly from another Pulsar topic
                // we must use the destination topic schema
                schemaToWrite = schema;
            }
            Producer<T> producer = getProducer(
                    String.format("%s-%s",record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic()), record.getPartitionId().get()),
                    record.getPartitionId().get(),
                    record.getDestinationTopic().orElse(pulsarSinkConfig.getTopic()),
                    schemaToWrite
            );
            if (schemaToWrite != null) {
                return producer.newMessage(schemaToWrite);
            } else {
                return producer.newMessage();
            }
        }

        @Override
        public void sendOutputMessage(TypedMessageBuilder<T> msg, SinkRecord<T> record) {

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

        Crypto crypto = initializeCrypto();
        if (crypto == null) {
            log.info("crypto key reader is not provided, not enabling end to end encryption");
        }

        FunctionConfig.ProcessingGuarantees processingGuarantees = this.pulsarSinkConfig.getProcessingGuarantees();
        switch (processingGuarantees) {
            case ATMOST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtMostOnceProcessor(schema, crypto);
                break;
            case ATLEAST_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkAtLeastOnceProcessor(schema, crypto);
                break;
            case EFFECTIVELY_ONCE:
                this.pulsarSinkProcessor = new PulsarSinkEffectivelyOnceProcessor(schema, crypto);
                break;
        }
    }

    @Override
    public void write(Record<T> record) {
        SinkRecord<T> sinkRecord = (SinkRecord<T>) record;
        TypedMessageBuilder<T> msg = pulsarSinkProcessor.newMessage(sinkRecord);

        if (record.getKey().isPresent() && !(record.getSchema() instanceof KeyValueSchema &&
                ((KeyValueSchema) record.getSchema()).getKeyValueEncodingType() == KeyValueEncodingType.SEPARATED)) {
            msg.key(record.getKey().get());
        }

        msg.value(record.getValue());

        if (!record.getProperties().isEmpty() && pulsarSinkConfig.isForwardSourceMessageProperty()) {
            msg.properties(record.getProperties());
        }

        if (sinkRecord.getSourceRecord() instanceof PulsarRecord) {
            PulsarRecord<T> pulsarRecord = (PulsarRecord<T>) sinkRecord.getSourceRecord();
            // forward user properties to sink-topic
            msg.property("__pfn_input_topic__", pulsarRecord.getTopicName().get())
               .property("__pfn_input_msg_id__",
                         new String(Base64.getEncoder().encode(pulsarRecord.getMessageId().toByteArray()), StandardCharsets.UTF_8));
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

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    Crypto initializeCrypto() throws ClassNotFoundException {
        if (pulsarSinkConfig.getProducerConfig() == null
                || pulsarSinkConfig.getProducerConfig().getCryptoConfig() == null
                || isEmpty(pulsarSinkConfig.getProducerConfig().getCryptoConfig().getCryptoKeyReaderClassName())) {
            return null;
        }

        CryptoConfig cryptoConfig = pulsarSinkConfig.getProducerConfig().getCryptoConfig();

        // add provider only if it's not in the JVM
        if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
            Security.addProvider(new BouncyCastleProvider());
        }

        final String[] encryptionKeys = cryptoConfig.getEncryptionKeys();
        Crypto.CryptoBuilder bldr = Crypto.builder()
                .failureAction(cryptoConfig.getProducerCryptoFailureAction())
                .encryptionKeys(encryptionKeys);

        bldr.keyReader(CryptoUtils.getCryptoKeyReaderInstance(
                cryptoConfig.getCryptoKeyReaderClassName(), cryptoConfig.getCryptoKeyReaderConfig(), functionClassLoader));

        return bldr.build();
    }

    @Data
    @Builder
    private static class Crypto {
        private CryptoKeyReader keyReader;
        private ProducerCryptoFailureAction failureAction;
        private String[] encryptionKeys;
    }
}
