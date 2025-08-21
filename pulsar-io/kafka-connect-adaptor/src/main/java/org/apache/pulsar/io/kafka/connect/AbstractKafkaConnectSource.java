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
package org.apache.pulsar.io.kafka.connect;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;

/**
 * A pulsar source that runs.
 */
@Slf4j
public abstract class AbstractKafkaConnectSource<T> implements Source<T> {

    // kafka connect related variables
    private SourceTaskContext sourceTaskContext;
    private SourceConnector connector;
    @Getter
    private SourceTask sourceTask;
    public Converter keyConverter;
    public Converter valueConverter;

    // pulsar io related variables
    private Iterator<SourceRecord> currentBatch = null;
    private CompletableFuture<Void> flushFuture;
    private OffsetBackingStore offsetStore;
    private OffsetStorageReader offsetReader;
    private String topicNamespace;
    @Getter
    public OffsetStorageWriter offsetWriter;
    // number of outstandingRecords that have been polled but not been acked
    private final AtomicInteger outstandingRecords = new AtomicInteger(0);
    private final AtomicBoolean flushing = new AtomicBoolean(false);

    public static final String CONNECTOR_CLASS = "kafkaConnectorSourceClass";

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        Map<String, String> stringConfig = new HashMap<>();
        config.forEach((key, value) -> {
            if (value instanceof String) {
                stringConfig.put(key, (String) value);
            }
        });

        topicNamespace = stringConfig.get(PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG);

        // initialize the key and value converter
        keyConverter = Class.forName(stringConfig.get(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG))
                .asSubclass(Converter.class)
                .getDeclaredConstructor()
                .newInstance();
        valueConverter = Class.forName(stringConfig.get(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG))
                .asSubclass(Converter.class)
                .getDeclaredConstructor()
                .newInstance();

        if (keyConverter instanceof AvroConverter) {
            keyConverter = new AvroConverter(new MockSchemaRegistryClient());
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
        }
        if (valueConverter instanceof AvroConverter) {
            valueConverter = new AvroConverter(new MockSchemaRegistryClient());
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock");
        }
        keyConverter.configure(config, true);
        valueConverter.configure(config, false);

        offsetStore = new PulsarOffsetBackingStore(sourceContext.getPulsarClient());
        PulsarKafkaWorkerConfig pulsarKafkaWorkerConfig = new PulsarKafkaWorkerConfig(stringConfig);
        offsetStore.configure(pulsarKafkaWorkerConfig);
        offsetStore.start();

        offsetReader = new OffsetStorageReaderImpl(
                offsetStore,
                "pulsar-kafka-connect-adaptor",
                keyConverter,
                valueConverter
        );
        offsetWriter = new OffsetStorageWriter(
                offsetStore,
                "pulsar-kafka-connect-adaptor",
                keyConverter,
                valueConverter
        );

        sourceTaskContext = new PulsarIOSourceTaskContext(offsetReader, pulsarKafkaWorkerConfig);

        final Map<String, String> taskConfig;
        if (config.get(CONNECTOR_CLASS) != null) {
            String kafkaConnectorFQClassName = config.get(CONNECTOR_CLASS).toString();
            Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
            connector = (SourceConnector) clazz.getConstructor().newInstance();

            Class<? extends Task> taskClass = connector.taskClass();
            sourceTask = (SourceTask) taskClass.getConstructor().newInstance();

            connector.initialize(new PulsarKafkaSinkContext());
            connector.start(stringConfig);

            List<Map<String, String>> configs = connector.taskConfigs(1);
            checkNotNull(configs);
            checkArgument(configs.size() == 1);
            taskConfig = configs.get(0);
        } else {
            // for backward compatibility with old configuration
            // that use the task directly

            // get the source class name from config and create source task from reflection
            sourceTask = Class.forName(stringConfig.get(TaskConfig.TASK_CLASS_CONFIG))
                    .asSubclass(SourceTask.class)
                    .getDeclaredConstructor()
                    .newInstance();
            taskConfig = stringConfig;
        }

        sourceTask.initialize(sourceTaskContext);
        sourceTask.start(taskConfig);
    }

    private void onOffsetsFlushed(Throwable error) {
        if (error != null) {
            log.error("Failed to flush offsets to storage: ", error);
            currentBatch = null;
            offsetWriter.cancelFlush();
            flushFuture.completeExceptionally(new Exception("No Offsets Added Error", error));
            return;
        }
        try {
            sourceTask.commit();
            if (log.isDebugEnabled()) {
                log.debug("Finished flushing offsets to storage");
            }
            currentBatch = null;
            flushFuture.complete(null);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            log.warn("Flush interrupted, cancelling", ie);
            offsetWriter.cancelFlush();
            flushFuture.completeExceptionally(new Exception("Failed to commit offsets", ie));
        } catch (Throwable t) {
            log.warn("Flush failed, cancelling", t);
            offsetWriter.cancelFlush();
            flushFuture.completeExceptionally(new Exception("Failed to commit offsets", t));
        }
    }

    private void triggerOffsetsFlushIfNeeded() {
        // Only flush when we have a batch in flight, nothing outstanding, and a pending future
        if (flushFuture == null || flushFuture.isDone() || outstandingRecords.get() != 0) {
            return;
        }
        // Ensure we initiate flush only once per batch
        if (!flushing.compareAndSet(false, true)) {
            return; // someone else is flushing
        }
        try {
            if (offsetWriter.beginFlush()) {
                offsetWriter.doFlush((error, ignored) -> {
                    try {
                        onOffsetsFlushed(error);
                    } finally {
                        flushing.set(false);
                    }
                });
            } else {
                // Nothing staged to flush; still treat as successful flush so read() can progress
                try {
                    onOffsetsFlushed(null);
                } finally {
                    flushing.set(false);
                }
            }
        } catch (org.apache.kafka.connect.errors.ConnectException alreadyFlushing) {
            // Another thread initiated the flush; let their callback complete the future.
            // Keep 'flushing' = true until read() finalizes the batch.
        } catch (Exception t) {
            try {
                onOffsetsFlushed(t);
            } finally {
                flushing.set(false);
            }
        }
    }

    @Override
    public synchronized Record<T> read() throws Exception {
        while (true) {
            if (currentBatch == null) {
                flushFuture = new CompletableFuture<>();
                List<SourceRecord> recordList = sourceTask.poll();
                if (recordList == null || recordList.isEmpty()) {
                    continue;
                }
                outstandingRecords.addAndGet(recordList.size());
                currentBatch = recordList.iterator();
            }
            if (currentBatch.hasNext()) {
                AbstractKafkaSourceRecord<T> processRecord = processSourceRecord(currentBatch.next());
                if (processRecord == null || processRecord.isEmpty()) {
                    outstandingRecords.decrementAndGet();
                    // If the entire batch is filtered, flush offsets now so it won't block later
                    triggerOffsetsFlushIfNeeded();
                } else {
                    return processRecord;
                }
            } else {
                // No more records in this batch: wait for offsets to be committed before next batch
                triggerOffsetsFlushIfNeeded();
                try {
                    flushFuture.get();
                } catch (ExecutionException ex) {
                    log.error("execution exception while get flushFuture", ex);
                    throw new Exception("Flush failed", ex.getCause());
                } finally {
                    flushing.set(false);
                    flushFuture = null;
                    currentBatch = null;
                }
            }
        }
    }

    @Override
    public void close() {
        if (sourceTask != null) {
            sourceTask.stop();
            sourceTask = null;
        }

        if (connector != null) {
            connector.stop();
            connector = null;
        }

        if (offsetStore != null) {
            offsetStore.stop();
            offsetStore = null;
        }
    }

    public abstract AbstractKafkaSourceRecord<T> processSourceRecord(SourceRecord srcRecord);

    private static final Map<String, String> PROPERTIES = Collections.emptyMap();
    private static final Optional<Long> RECORD_SEQUENCE = Optional.empty();

    public abstract class AbstractKafkaSourceRecord<T> implements Record {
        @Getter
        Optional<String> key;
        @Getter
        T value;
        @Getter
        Optional<String> topicName;
        @Getter
        Optional<Long> eventTime;
        @Getter
        Optional<String> partitionId;
        @Getter
        Optional<String> destinationTopic;
        @Getter
        Optional<Integer> partitionIndex;

        KafkaSchemaWrappedSchema keySchema;

        KafkaSchemaWrappedSchema valueSchema;

        AbstractKafkaSourceRecord(SourceRecord srcRecord) {
            String topic = srcRecord.topic();
            if (topic.contains("://")) {
                try {
                    TopicName.get(topic);
                    this.destinationTopic = Optional.of(topic);
                } catch (IllegalArgumentException e) {
                    this.destinationTopic = Optional.of("persistent://" + topicNamespace + "/" + topic);
                }
            } else {
                this.destinationTopic = Optional.of("persistent://" + topicNamespace + "/" + topic);
            }
            this.partitionIndex = Optional.ofNullable(srcRecord.kafkaPartition());
        }

        @Override
        public Optional<Long> getRecordSequence() {
            return RECORD_SEQUENCE;
        }

        @Override
        public Map<String, String> getProperties() {
            return PROPERTIES;
        }

        public boolean isEmpty() {
            return this.value == null;
        }

        @Override
        public void ack() {
            // Decrement and let the centralized flusher decide if we should flush now
            if (outstandingRecords.decrementAndGet() == 0) {
                triggerOffsetsFlushIfNeeded();
            }
        }

        @Override
        public void fail() {
            if (flushFuture != null) {
                flushFuture.completeExceptionally(new Exception("Sink Error"));
            }
        }
    }

}
