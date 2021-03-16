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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.PULSAR_SERVICE_URL_CONFIG;

@Slf4j
public class KafkaConnectSink implements Sink<Object> {

    private boolean unwrapKeyValueIfAvailable;

    private final static ImmutableMap<Class<?>, Schema> primitiveTypeToSchema;
    static {
        primitiveTypeToSchema = ImmutableMap.<Class<?>, Schema>builder()
                .put(Boolean.class, Schema.BOOLEAN_SCHEMA)
                .put(Byte.class, Schema.INT8_SCHEMA)
                .put(Short.class, Schema.INT16_SCHEMA)
                .put(Integer.class, Schema.INT32_SCHEMA)
                .put(Long.class, Schema.INT64_SCHEMA)
                .put(Float.class, Schema.FLOAT32_SCHEMA)
                .put(Double.class, Schema.FLOAT64_SCHEMA)
                .put(String.class, Schema.STRING_SCHEMA)
                .put(byte[].class, Schema.BYTES_SCHEMA)
                .build();
    }

    private PulsarKafkaSinkContext sinkContext;
    private PulsarKafkaSinkTaskContext taskContext;
    private SinkConnector connector;
    private SinkTask task;

    private Schema defaultKeySchema;
    private Schema defaultValueSchema;


    private int batchSize;
    private long lingerMs;
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("pulsar-io-kafka-adaptor-sink-flush-%d")
                    .build());
    private final AtomicInteger numPendingRecords = new AtomicInteger(0);

    private volatile CompletableFuture<Void> pendingFlush = new CompletableFuture<>();
    private volatile boolean isRunning = false;

    private Properties props = new Properties();
    private PulsarKafkaConnectSinkConfig kafkaSinkConfig;

    protected String topicName;

    @Override
    public void write(Record<Object> sourceRecord) {
        if (log.isDebugEnabled()) {
            log.debug("Record sending to kafka, record={}.", sourceRecord);
        }

        if (!isRunning) {
            log.error("Sink is stopped. Cannot send the record {}", sourceRecord);
            sourceRecord.fail();
            return;
        }

        try {
            SinkRecord record = toSinkRecord(sourceRecord);
            task.put(Lists.newArrayList(record));
        } catch (Exception ex) {
            log.error("Error sending the record {}", sourceRecord, ex);
            sourceRecord.fail();
            return;
        }
        pendingFlush.whenComplete((ignore, ex) -> {
            if (ex == null) {
                sourceRecord.ack();
            } else {
                log.error("Error sending the record {}", sourceRecord, ex);
                sourceRecord.fail();
            }
            throw new IllegalArgumentException();
        });
        numPendingRecords.incrementAndGet();
        flushIfNeeded(false);
    }

    @Override
    public void close() throws Exception {
        isRunning = false;
        flushIfNeeded(true);
        scheduledExecutor.shutdown();
        if (!scheduledExecutor.awaitTermination(10 * lingerMs, TimeUnit.MILLISECONDS)) {
            log.error("scheduledExecutor did not terminate in {} ms", 10 * lingerMs);
        }

        task.stop();
        connector.stop();
        taskContext.close();

        log.info("Kafka sink stopped.");
    }

    @Override
    public void open(Map<String, Object> config, SinkContext ctx) throws Exception {
        kafkaSinkConfig = PulsarKafkaConnectSinkConfig.load(config);
        Objects.requireNonNull(kafkaSinkConfig.getTopic(), "Kafka topic is not set");
        topicName = kafkaSinkConfig.getTopic();
        unwrapKeyValueIfAvailable = kafkaSinkConfig.isUnwrapKeyValueIfAvailable();

        String kafkaConnectorFQClassName = kafkaSinkConfig.getKafkaConnectorSinkClass();
        kafkaSinkConfig.getKafkaConnectorConfigProperties().entrySet()
                .forEach(kv -> props.put(kv.getKey(), kv.getValue()));

        defaultKeySchema = (Schema)Schema.class
                .getField(kafkaSinkConfig.getDefaultKeySchema()).get(null);
        defaultValueSchema = (Schema)Schema.class
                .getField(kafkaSinkConfig.getDefaultValueSchema()).get(null);

        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SinkConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sinkContext = new PulsarKafkaSinkContext();
        connector.initialize(sinkContext);
        connector.start(Maps.fromProperties(props));

        List<Map<String, String>> configs = connector.taskConfigs(1);
        configs.forEach(x -> {
            x.put(OFFSET_STORAGE_TOPIC_CONFIG, kafkaSinkConfig.getOffsetStorageTopic());
            x.put(PULSAR_SERVICE_URL_CONFIG, kafkaSinkConfig.getPulsarServiceUrl());
        });
        task = (SinkTask) taskClass.getConstructor().newInstance();
        taskContext =
                new PulsarKafkaSinkTaskContext(configs.get(0), task::open);
        task.initialize(taskContext);
        task.start(configs.get(0));

        batchSize = kafkaSinkConfig.getBatchSize();
        lingerMs = kafkaSinkConfig.getLingerTimeMs();
        scheduledExecutor.scheduleAtFixedRate(() ->
                this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);

        isRunning = true;
        log.info("Kafka sink started : {}.", props);
    }

    private void flushIfNeeded(boolean force) {
        if (force || numPendingRecords.get() >= batchSize) {
            scheduledExecutor.submit(this::flush);
        }
    }

    public void flush() {
        if (log.isDebugEnabled()) {
            log.debug("flush requested, pending: {}, batchSize: {}",
                    numPendingRecords.get(), batchSize);
        }

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

    /**
     * org.apache.kafka.connect.data.Schema for the object
     * @param obj
     * @return org.apache.kafka.connect.data.Schema
     */
    public static Schema getKafkaConnectSchemaForObject(Object obj, Schema defaultSchema) {
        if (obj == null) {
            return defaultSchema;
        }

        if (primitiveTypeToSchema.containsKey(obj.getClass())) {
            return primitiveTypeToSchema.get(obj.getClass());
        }

        // Other types are not supported yet.
        // Will fallback to defaults provided.
        return defaultSchema;
    }

    private SinkRecord toSinkRecord(Record<Object> sourceRecord) {
        final int partition = 0;
        final Object key;
        final Object value;
        if (unwrapKeyValueIfAvailable && sourceRecord.getValue() instanceof KeyValue) {
            KeyValue<Object, Object> kv = (KeyValue<Object, Object>) sourceRecord.getValue();
            key = kv.getKey();
            value = kv.getValue();
        } else {
            key = sourceRecord.getKey().orElse(null);
            value = sourceRecord.getValue();
        }
        final Schema keySchema = getKafkaConnectSchemaForObject(key, defaultKeySchema);
        final Schema valueSchema = getKafkaConnectSchemaForObject(value, defaultValueSchema);

        long offset = taskContext.currentOffset(topicName, partition).incrementAndGet();
        SinkRecord sinkRecord = new SinkRecord(topicName,
                partition,
                keySchema,
                key,
                valueSchema,
                value,
                offset,
                sourceRecord.getEventTime().orElse(null),
                TimestampType.NO_TIMESTAMP_TYPE);
        return sinkRecord;
    }

    @VisibleForTesting
    protected long currentOffset() {
        return taskContext.currentOffset(topicName, 0).get();
    }

}
