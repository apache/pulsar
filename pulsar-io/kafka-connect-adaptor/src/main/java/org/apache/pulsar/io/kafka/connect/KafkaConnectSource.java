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

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A pulsar source that runs
 */
@Slf4j
public class KafkaConnectSource implements Source<byte[]> {

    // kafka connect related variables
    private SourceTaskContext sourceTaskContext;
    @Getter
    private SourceTask sourceTask;
    private Converter keyConverter;
    private Converter valueConverter;

    // pulsar io related variables
    private Iterator<SourceRecord> currentBatch = null;
    private CompletableFuture<Void> flushFuture;
    private OffsetBackingStore offsetStore;
    private OffsetStorageReader offsetReader;
    @Getter
    private OffsetStorageWriter offsetWriter;
    private IdentityHashMap<SourceRecord, SourceRecord> outstandingRecords = new IdentityHashMap<>();

    @Override
    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        Map<String, String> stringConfig = new HashMap<>();
        config.forEach((key, value) -> {
            if (value instanceof String) {
                stringConfig.put(key, (String) value);
            }
        });

        // get the source class name from config and create source task from reflection
        sourceTask = ((Class<? extends SourceTask>)config.get(TaskConfig.TASK_CLASS_CONFIG))
            .asSubclass(SourceTask.class)
            .getDeclaredConstructor()
            .newInstance();

        // initialize the key and value converter
        keyConverter = ((Class<? extends Converter>)config.get(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG))
            .asSubclass(Converter.class)
            .getDeclaredConstructor()
            .newInstance();
        valueConverter = ((Class<? extends Converter>)config.get(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG))
            .asSubclass(Converter.class)
            .getDeclaredConstructor()
            .newInstance();

        offsetStore = new PulsarOffsetBackingStore();
        offsetStore.configure(new PulsarKafkaWorkerConfig(stringConfig));
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

        sourceTaskContext = new PulsarIOSourceTaskContext(offsetReader);

        sourceTask.initialize(sourceTaskContext);
        sourceTask.start(stringConfig);
    }

    @Override
    public Record<byte[]> read() throws Exception {
        while (true) {
            if (currentBatch == null) {
                flushFuture = new CompletableFuture<>();
                currentBatch = sourceTask.poll().iterator();
            }
            if (currentBatch.hasNext()) {
                return processSourceRecord(currentBatch.next());
            } else {
                boolean hasOutstandingRecords;
                synchronized (this) {
                    hasOutstandingRecords = !outstandingRecords.isEmpty();
                }
                if (hasOutstandingRecords) {
                    // there is no records any more, then waiting for the batch to complete writing
                    // to sink and the offsets are committed as well
                    flushFuture.get();
                    flushFuture = null;
                }
                currentBatch = null;
            }
        }
    }

    private synchronized Record<byte[]> processSourceRecord(final SourceRecord srcRecord) {
        outstandingRecords.put(srcRecord, srcRecord);
        offsetWriter.offset(srcRecord.sourcePartition(), srcRecord.sourceOffset());
        return new Record<byte[]>() {
            @Override
            public Optional<String> getKey() {
                byte[] keyBytes = keyConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.keySchema(), srcRecord.key());
                return Optional.of(Base64.getEncoder().encodeToString(keyBytes));
            }

            @Override
            public byte[] getValue() {
                return valueConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());
            }

            @Override
            public Optional<String> getTopicName() {
                return Optional.of(srcRecord.topic());
            }

            @Override
            public Optional<Long> getEventTime() {
                return Optional.of(srcRecord.timestamp());
            }

            @Override
            public Optional<String> getPartitionId() {
                String partitionId = srcRecord.sourcePartition()
                    .entrySet()
                    .stream()
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(Collectors.joining(","));
                return Optional.of(partitionId);
            }

            @Override
            public Optional<Long> getRecordSequence() {
                return Optional.empty();
            }

            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public void ack() {
                boolean canComplete;
                synchronized (KafkaConnectSource.this) {
                    outstandingRecords.remove(srcRecord);
                    canComplete = outstandingRecords.isEmpty();
                }
                if (canComplete && flushFuture != null) {
                    flushFuture.complete(null);
                }
            }

            @Override
            public void fail() {
                if (flushFuture != null) {
                    flushFuture.completeExceptionally(new Exception("Sink Error"));
                }
            }
        };
    }


    @Override
    public void close() throws Exception {
        sourceTask.stop();
    }
}
