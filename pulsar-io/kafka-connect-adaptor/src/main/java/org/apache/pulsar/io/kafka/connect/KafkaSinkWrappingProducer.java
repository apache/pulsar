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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG;

/***
 * Adapter from a SinkTask to a KafkaProducer to use producer api to write to the sink.
 *
 * Supports kafka Producer's config options of
 *  - linger.ms
 *  - batch.size
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public class KafkaSinkWrappingProducer<K, V> implements Producer<K, V> {

    private final SinkConnector connector;
    private final SinkTask task;
    private final Schema defaultKeySchema;
    private final Schema defaultValueSchema;
    private final PulsarKafkaSinkContext sinkContext;
    private final PulsarKafkaSinkTaskContext taskContext;
    private final int batchSize;
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("pulsar-io-kafka-adaptor-sink-flush-%d")
                    .build());
    private final AtomicInteger numPendingRecords = new AtomicInteger(0);

    private volatile CompletableFuture<Void> pendingFlush = new CompletableFuture<>();

    private static long getLingerMs(Properties props) {
        long lingerMs = 2147483647L; // as in kafka
        final String lingerPropName = "linger.ms";
        if (props.containsKey(lingerPropName)) {
            try {
                lingerMs = Long.parseLong((String) props.get(lingerPropName));
            } catch (NumberFormatException nfe) {
                log.warn("Could not parse property " + lingerPropName +
                        " from " + props.get(lingerPropName) + " - will use default", nfe);
            }
        }
        return lingerMs;
    }

    private static int getBatchSize(Properties props) {
        final String batchSizePropName = "batch.size";
        int batchSize = 1;
        if (props.containsKey(batchSizePropName)) {
            try {
                batchSize = Math.max(batchSize, Integer.parseInt((String) props.get(batchSizePropName)));
            } catch (NumberFormatException nfe) {
                log.warn("Could not parse property " + batchSizePropName +
                        " from " + props.get(batchSizePropName) + " - will use default", nfe);
            }
        }

        return batchSize;
    }

    public static <K, V> Producer<K, V> create(String kafkaConnectorFQClassName,
                                               Properties props,
                                               Schema keySchema,
                                               Schema valueSchema) {
        try {
            Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
            SinkConnector connector = (SinkConnector) clazz.getConstructor().newInstance();

            Class<? extends Task> taskClass = connector.taskClass();
            PulsarKafkaSinkContext sinkContext = new PulsarKafkaSinkContext();
            connector.initialize(sinkContext);
            connector.start(Maps.fromProperties(props));

            List<Map<String, String>> configs = connector.taskConfigs(1);
            configs.forEach(x -> {
                x.put(OFFSET_STORAGE_TOPIC_CONFIG, props.getProperty(OFFSET_STORAGE_TOPIC_CONFIG));
                x.put(PULSAR_SERVICE_URL_CONFIG, props.getProperty(PULSAR_SERVICE_URL_CONFIG));
            });
            SinkTask task = (SinkTask) taskClass.getConstructor().newInstance();
            PulsarKafkaSinkTaskContext taskContext =
                    new PulsarKafkaSinkTaskContext(configs.get(0), task::open);
            task.initialize(taskContext);
            task.start(configs.get(0));

            Producer<K, V> producer = new KafkaSinkWrappingProducer<>(connector, task,
                    keySchema, valueSchema,
                    sinkContext, taskContext, props);

            return producer;
        } catch (Exception e) {
            log.error("Failed to create KafkaSinkWrappingProducer with {}, {} & {}",
                    props, keySchema.name(), valueSchema.name(), e);
            throw new IllegalArgumentException("failed to create KafkaSink with given parameters", e);
        }
    }

    private KafkaSinkWrappingProducer(SinkConnector connector,
                                      SinkTask task,
                                      Schema defaultKeySchema,
                                      Schema defaultValueSchema,
                                      PulsarKafkaSinkContext sinkContext,
                                      PulsarKafkaSinkTaskContext taskContext,
                                      Properties props) {
        this.connector = connector;
        this.task = task;
        this.defaultKeySchema = defaultKeySchema;
        this.defaultValueSchema = defaultValueSchema;
        this.sinkContext = sinkContext;
        this.taskContext = taskContext;
        this.batchSize = getBatchSize(props);

        long lingerMs = getLingerMs(props);
        scheduledExecutor.scheduleAtFixedRate(() -> this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void initTransactions() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> map, String s)
            throws ProducerFencedException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        throw new UnsupportedOperationException("not supported");
    }

    private SinkRecord toSinkRecord(ProducerRecord<K, V> producerRecord) {
        int partition = producerRecord.partition() == null ? 0 : producerRecord.partition();
        Schema keySchema = defaultKeySchema;
        Schema valueSchema = defaultValueSchema;

        if (producerRecord instanceof ProducerRecordWithSchema) {
            ProducerRecordWithSchema rec = (ProducerRecordWithSchema) producerRecord;
            keySchema = rec.getKeySchema();
            valueSchema = rec.getValueSchema();
        }

        long offset = taskContext.currentOffset(producerRecord.topic(), partition).incrementAndGet();
        SinkRecord sinkRecord = new SinkRecord(producerRecord.topic(),
                partition,
                keySchema,
                producerRecord.key(),
                valueSchema,
                producerRecord.value(),
                offset,
                producerRecord.timestamp(),
                TimestampType.NO_TIMESTAMP_TYPE);
        return sinkRecord;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        sinkContext.throwIfNeeded();
        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
        try {
            task.put(Lists.newArrayList(toSinkRecord(producerRecord)));
        } catch (Exception ex) {
            result.completeExceptionally(ex);
            return result;
        }
        pendingFlush.whenComplete((ignore, ex) -> {
            if (ex == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(ex);
            }
        });
        numPendingRecords.incrementAndGet();
        flushIfNeeded(false);
        return result;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        sinkContext.throwIfNeeded();
        CompletableFuture<RecordMetadata> result = new CompletableFuture<>();
        result.whenComplete((ignore, ex) -> {
            if (ex == null) {
                callback.onCompletion(null, null);
            } else {
                if (ex instanceof Exception) {
                    callback.onCompletion(null, (Exception) ex);
                } else {
                    callback.onCompletion(null, new Exception(ex));
                }
            }
        });

        try {
            task.put(Lists.newArrayList(toSinkRecord(producerRecord)));
        } catch (Exception ex) {
            result.completeExceptionally(ex);
            return result;
        }
        pendingFlush.whenComplete((ignore, ex) -> {
            if (ex == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(ex);
            }
        });
        numPendingRecords.incrementAndGet();
        flushIfNeeded(false);
        return result;
    }

    private void flushIfNeeded(boolean force) {
        if (force || numPendingRecords.get() >= batchSize) {
            flush();
        }
    }

    @Override
    public void flush() {
        if (log.isDebugEnabled()) {
            log.debug("flush requested, pending: {}, batchSize: {}",
                    numPendingRecords.get(), batchSize);
        }

        sinkContext.throwIfNeeded();

        if (numPendingRecords.getAndSet(0) == 0) {
            return;
        }

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = taskContext.currentOffsets();
        CompletableFuture<Void> flushCf;
        synchronized (this) {
            flushCf = pendingFlush;
            pendingFlush = new CompletableFuture<>();
        }

        try {
            task.flush(currentOffsets);
            taskContext.flushOffsets(currentOffsets);
            flushCf.complete(null);
        } catch (Throwable t) {
            log.error("error flushing pending records", t);
            flushCf.completeExceptionally(t);
        }
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new UnsupportedOperationException("not implemented");
    }

    @VisibleForTesting
    long currentOffset(String topic, int partition) {
        return taskContext.currentOffset(topic, partition).get();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() {
        this.close(Duration.ofHours(1L));
    }


    @Override
    public void close(Duration duration) {
        sinkContext.throwIfNeeded();
        scheduledExecutor.shutdown();
        flush();
        task.stop();
        connector.stop();
        taskContext.close();
    }
}
