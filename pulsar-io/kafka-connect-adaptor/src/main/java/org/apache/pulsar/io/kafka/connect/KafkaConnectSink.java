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
import com.google.common.base.Preconditions;
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
import org.apache.pulsar.client.api.schema.KeyValueSchema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaConnectData;
import org.apache.pulsar.io.kafka.connect.schema.PulsarSchemaToKafkaSchema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG;

@Slf4j
public class KafkaConnectSink implements Sink<GenericObject> {
    private boolean unwrapKeyValueIfAvailable;

    private PulsarKafkaSinkContext sinkContext;
    @VisibleForTesting
    PulsarKafkaSinkTaskContext taskContext;
    private SinkConnector connector;
    private SinkTask task;

    private long maxBatchSize;
    private final AtomicLong currentBatchSize = new AtomicLong(0L);

    private long lingerMs;
    private final ScheduledExecutorService scheduledExecutor =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                    .setNameFormat("pulsar-io-kafka-adaptor-sink-flush-%d")
                    .build());
    private final ConcurrentLinkedDeque<Record<GenericObject>> pendingFlushQueue = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean isFlushRunning = new AtomicBoolean(false);
    private volatile boolean isRunning = false;

    private final Properties props = new Properties();
    private PulsarKafkaConnectSinkConfig kafkaSinkConfig;

    protected String topicName;

    @Override
    public void write(Record<GenericObject> sourceRecord) {
        if (log.isDebugEnabled()) {
            log.debug("Record sending to kafka, record={}.", sourceRecord);
        }

        if (!isRunning) {
            log.warn("Sink is stopped. Cannot send the record {}", sourceRecord);
            sourceRecord.fail();
            return;
        }

        // while sourceRecord.getMessage() is Optional<>
        // it should always be present in Sink which gets instance of PulsarRecord
        // let's avoid checks for .isPresent() in teh rest of the code
        Preconditions.checkArgument(sourceRecord.getMessage().isPresent());
        try {
            SinkRecord record = toSinkRecord(sourceRecord);
            task.put(Lists.newArrayList(record));
        } catch (Exception ex) {
            log.error("Error sending the record {}", sourceRecord, ex);
            sourceRecord.fail();
            return;
        }
        pendingFlushQueue.add(sourceRecord);
        currentBatchSize.addAndGet(sourceRecord.getMessage().get().size());
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
        Preconditions.checkArgument(ctx.getSubscriptionType() == SubscriptionType.Failover
                || ctx.getSubscriptionType() == SubscriptionType.Exclusive,
                "Source must run with Exclusive or Failover subscription type");
        topicName = kafkaSinkConfig.getTopic();
        unwrapKeyValueIfAvailable = kafkaSinkConfig.isUnwrapKeyValueIfAvailable();

        String kafkaConnectorFQClassName = kafkaSinkConfig.getKafkaConnectorSinkClass();
        kafkaSinkConfig.getKafkaConnectorConfigProperties().forEach(props::put);

        Class<?> clazz = Class.forName(kafkaConnectorFQClassName);
        connector = (SinkConnector) clazz.getConstructor().newInstance();

        Class<? extends Task> taskClass = connector.taskClass();
        sinkContext = new PulsarKafkaSinkContext();
        connector.initialize(sinkContext);
        connector.start(Maps.fromProperties(props));

        List<Map<String, String>> configs = connector.taskConfigs(1);
        Preconditions.checkNotNull(configs);
        Preconditions.checkArgument(configs.size() == 1);

        configs.forEach(x -> {
            x.put(OFFSET_STORAGE_TOPIC_CONFIG, kafkaSinkConfig.getOffsetStorageTopic());
        });
        task = (SinkTask) taskClass.getConstructor().newInstance();
        taskContext =
                new PulsarKafkaSinkTaskContext(configs.get(0), ctx, task::open);
        task.initialize(taskContext);
        task.start(configs.get(0));

        maxBatchSize = kafkaSinkConfig.getBatchSize();
        lingerMs = kafkaSinkConfig.getLingerTimeMs();

        isRunning = true;
        scheduledExecutor.scheduleWithFixedDelay(() ->
                this.flushIfNeeded(true), lingerMs, lingerMs, TimeUnit.MILLISECONDS);

        log.info("Kafka sink started : {}.", props);
    }

    private void flushIfNeeded(boolean force) {
        if (isFlushRunning.get()) {
            return;
        }
        if (force || currentBatchSize.get() >= maxBatchSize) {
            scheduledExecutor.submit(this::flush);
        }
    }

    // flush always happens on the same thread
    public void flush() {
        if (log.isDebugEnabled()) {
            log.debug("flush requested, pending: {}, batchSize: {}",
                    currentBatchSize.get(), maxBatchSize);
        }

        if (pendingFlushQueue.isEmpty()) {
            return;
        }

        if (!isFlushRunning.compareAndSet(false, true)) {
            return;
        }

        final Record<GenericObject> lastNotFlushed = pendingFlushQueue.getLast();
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = taskContext.currentOffsets();
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = task.preCommit(currentOffsets);
            if (committedOffsets.isEmpty()) {
                log.info("Task returned empty committedOffsets map; skipping flush; task will retry later");
                return;
            }
            taskContext.flushOffsets(currentOffsets);
            ackUntil(lastNotFlushed, Record::ack);
            log.info("Flush succeeded");
        } catch (Throwable t) {
            log.error("error flushing pending records", t);
            ackUntil(lastNotFlushed, Record::fail);
        } finally {
            isFlushRunning.compareAndSet(true, false);
        }
    }

    private void ackUntil(Record<GenericObject> lastNotFlushed, java.util.function.Consumer<Record<GenericObject>> cb) {
        while (!pendingFlushQueue.isEmpty()) {
            Record<GenericObject> r = pendingFlushQueue.pollFirst();
            cb.accept(r);
            currentBatchSize.addAndGet(-1 * r.getMessage().get().size());
            if (r == lastNotFlushed) {
                break;
            }
        }
    }

    @SuppressWarnings("rawtypes")
    protected SinkRecord toSinkRecord(Record<GenericObject> sourceRecord) {
        final int partition = sourceRecord.getPartitionIndex().orElse(0);
        final String topic = sourceRecord.getTopicName().orElse(topicName);
        final Object key;
        final Object value;
        final Schema keySchema;
        final Schema valueSchema;

        // sourceRecord is never instanceof KVRecord
        // https://github.com/apache/pulsar/pull/10113
        if (unwrapKeyValueIfAvailable && sourceRecord.getSchema() != null
                && sourceRecord.getSchema().getSchemaInfo() != null
                && sourceRecord.getSchema().getSchemaInfo().getType() == SchemaType.KEY_VALUE) {
            KeyValueSchema kvSchema = (KeyValueSchema) sourceRecord.getSchema();
            KeyValue kv = (KeyValue) sourceRecord.getValue().getNativeObject();
            keySchema = PulsarSchemaToKafkaSchema.getKafkaConnectSchema(kvSchema.getKeySchema());
            valueSchema = PulsarSchemaToKafkaSchema.getKafkaConnectSchema(kvSchema.getValueSchema());
            key = kv.getKey();
            value = kv.getValue();
        } else {
            if (sourceRecord.getMessage().get().hasBase64EncodedKey()) {
                key = sourceRecord.getMessage().get().getKeyBytes();
                keySchema = Schema.BYTES_SCHEMA;
            } else {
                key = sourceRecord.getKey().orElse(null);
                keySchema = Schema.STRING_SCHEMA;
            }
            valueSchema = PulsarSchemaToKafkaSchema.getKafkaConnectSchema(sourceRecord.getSchema());
            value = KafkaConnectData.getKafkaConnectData(sourceRecord.getValue().getNativeObject(), valueSchema);
        }

        long offset = sourceRecord.getRecordSequence()
                .orElse(-1L);
        if (offset < 0) {
            log.error("Message without sequenceId. Key: {} Value: {}", key, value);
            throw new IllegalStateException("Message without sequenceId");
        }
        taskContext.updateLastOffset(new TopicPartition(topic, partition), offset);

        Long timestamp = null;
        TimestampType timestampType = TimestampType.NO_TIMESTAMP_TYPE;
        if (sourceRecord.getEventTime().isPresent()) {
            timestamp = sourceRecord.getEventTime().get();
            timestampType = TimestampType.CREATE_TIME;
        } else {
            // publishTime is not a log append time.
            // keep timestampType = TimestampType.NO_TIMESTAMP_TYPE
            timestamp = sourceRecord.getMessage().get().getPublishTime();
        }
        return new SinkRecord(topic,
                partition,
                keySchema,
                key,
                valueSchema,
                value,
                offset,
                timestamp,
                timestampType);
    }

    @VisibleForTesting
    protected long currentOffset(String topic, int partition) {
        return taskContext.currentOffset(topic, partition);
    }

}
