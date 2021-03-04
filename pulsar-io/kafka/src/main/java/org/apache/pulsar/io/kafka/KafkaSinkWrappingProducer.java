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

package org.apache.pulsar.io.kafka;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/***
 * Adapter from a SinkTask to a KafkaProducer to use producer api to write to the sink
 *
 * TODO: record metadata / return RecordMetadata/PartitionInfo where needed (if needed)
 *
 * @param <K>
 * @param <V>
 */
public class KafkaSinkWrappingProducer<K, V> implements Producer<K, V> {

    private final SinkConnector connector;
    private final SinkTask task;
    private final Schema defaultKeySchema;
    private final Schema defaultValueSchema;

    // todo: per topic
    private final AtomicLong offset = new AtomicLong(0);

    public KafkaSinkWrappingProducer(SinkConnector connector,
                                     SinkTask task,
                                     Schema defaultKeySchema,
                                     Schema defaultValueSchema) {
        this.connector = connector;
        this.task = task;
        this.defaultKeySchema = defaultKeySchema;
        this.defaultValueSchema = defaultValueSchema;
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

        SinkRecord sinkRecord = new SinkRecord(producerRecord.topic(),
                partition,
                keySchema,
                producerRecord.key(),
                valueSchema,
                producerRecord.value(),
                offset.getAndIncrement(),
                producerRecord.timestamp(),
                TimestampType.NO_TIMESTAMP_TYPE);
        return sinkRecord;
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord) {
        task.put(Lists.newArrayList(toSinkRecord(producerRecord)));
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> producerRecord, Callback callback) {
        try {
            task.put(Lists.newArrayList(toSinkRecord(producerRecord)));
            callback.onCompletion(null, null);
        } catch (Exception e) {
            callback.onCompletion(null, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() {
        task.flush(Maps.newHashMap());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String s) {
        return Lists.newLinkedList();
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return null;
    }

    @Override
    public void close() {
        this.close(Duration.ofHours(1L));
    }


    @Override
    public void close(Duration duration) {
        task.flush(Maps.newHashMap());
        task.stop();
        connector.stop();
    }
}
