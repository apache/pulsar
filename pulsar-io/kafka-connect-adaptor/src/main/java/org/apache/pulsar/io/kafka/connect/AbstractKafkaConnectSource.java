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
package org.apache.pulsar.io.kafka.connect;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroConverter;
import org.apache.pulsar.kafka.shade.io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.pulsar.kafka.shade.io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

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

    @Override
    public synchronized Record<T> read() throws Exception {
        while (true) {
            if (currentBatch == null) {
                flushFuture = new CompletableFuture<>();
                List<SourceRecord> recordList = sourceTask.poll();
                if (recordList == null || recordList.isEmpty()) {
                    Thread.sleep(1000);
                    continue;
                }
                outstandingRecords.addAndGet(recordList.size());
                currentBatch = recordList.iterator();
            }
            if (currentBatch.hasNext()) {
                AbstractKafkaSourceRecord<T> processRecord = processSourceRecord(currentBatch.next());
                if (processRecord.isEmpty()) {
                    outstandingRecords.decrementAndGet();
                    continue;
                } else {
                    return processRecord;
                }
            } else {
                // there is no records any more, then waiting for the batch to complete writing
                // to sink and the offsets are committed as well, then do next round read.
                try {
                    flushFuture.get();
                } catch (ExecutionException ex) {
                    // log the error, continue execution
                    log.error("execution exception while get flushFuture", ex);
                    throw new Exception("Flush failed", ex.getCause());
                } finally {
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
            this.destinationTopic = Optional.of("persistent://" + topicNamespace + "/" + srcRecord.topic());
            this.partitionIndex = Optional.ofNullable(srcRecord.kafkaPartition());
        }

        @Override
        public Schema getSchema() {
            return null;
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

        private void completedFlushOffset(Throwable error, Void result) {
            if (error != null) {
                log.error("Failed to flush offsets to storage: ", error);
                currentBatch = null;
                offsetWriter.cancelFlush();
                flushFuture.completeExceptionally(new Exception("No Offsets Added Error"));
            } else {
                try {
                    sourceTask.commit();

                    log.info("Finished flushing offsets to storage");
                    currentBatch = null;
                    flushFuture.complete(null);
                } catch (InterruptedException exception) {
                    log.warn("Flush of {} offsets interrupted, cancelling", this);
                    Thread.currentThread().interrupt();
                    offsetWriter.cancelFlush();
                    flushFuture.completeExceptionally(new Exception("Failed to commit offsets", exception));
                } catch (Throwable t) {
                    // SourceTask can throw unchecked ConnectException/KafkaException.
                    // Make sure the future is cancelled in that case
                    log.warn("Flush of {} offsets failed, cancelling", this);
                    offsetWriter.cancelFlush();
                    flushFuture.completeExceptionally(new Exception("Failed to commit offsets", t));
                }
            }
        }

        @Override
        public void ack() {
            // TODO: should flush for each batch. not wait for a time for acked all.
            // How to handle order between each batch. QueueList<pair<batch, automicInt>>. check if head is all acked.
            boolean canFlush = (outstandingRecords.decrementAndGet() == 0);

            // consumed all the records, flush the offsets
            if (canFlush && flushFuture != null) {
                if (!offsetWriter.beginFlush()) {
                    log.error("When beginFlush, No offsets to commit!");
                    flushFuture.completeExceptionally(new Exception("No Offsets Added Error when beginFlush"));
                    return;
                }

                Future<Void> doFlush = offsetWriter.doFlush(this::completedFlushOffset);
                if (doFlush == null) {
                    // Offsets added in processSourceRecord, But here no offsets to commit
                    log.error("No offsets to commit!");
                    flushFuture.completeExceptionally(new Exception("No Offsets Added Error"));
                    return;
                }
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
