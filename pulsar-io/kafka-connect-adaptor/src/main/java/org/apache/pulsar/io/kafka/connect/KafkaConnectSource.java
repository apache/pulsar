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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.connect.avro.AvroData;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.predicates.Predicate;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;

/**
 * A pulsar source that runs.
 */
@Slf4j
public class KafkaConnectSource extends AbstractKafkaConnectSource<KeyValue<byte[], byte[]>> {

    private final Cache<org.apache.kafka.connect.data.Schema, KafkaSchemaWrappedSchema> readerCache =
            CacheBuilder.newBuilder().maximumSize(10000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build();

    private boolean jsonWithEnvelope = false;
    private static final String JSON_WITH_ENVELOPE_CONFIG = "json-with-envelope";

    private Map<String, Predicate<SourceRecord>> predicates = new HashMap<>();

    private record PredicatedTransform(
            Predicate<SourceRecord> predicate,
            Transformation<SourceRecord> transform,
            boolean negated
    ) {}
    private List<PredicatedTransform> transformations = new ArrayList<>();

    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        if (config.get(JSON_WITH_ENVELOPE_CONFIG) != null) {
            jsonWithEnvelope = Boolean.parseBoolean(config.get(JSON_WITH_ENVELOPE_CONFIG).toString());
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, jsonWithEnvelope);
        } else {
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        }
        log.info("jsonWithEnvelope: {}", jsonWithEnvelope);

        initPredicates(config);
        initTransforms(config);
        super.open(config, sourceContext);
    }

    private void initPredicates(Map<String, Object> config) {
        Object predicatesListObj = config.get("predicates");
        if (predicatesListObj != null) {
            String predicatesList = predicatesListObj.toString();
            for (String predicateName : predicatesList.split(",")) {
                predicateName = predicateName.trim();
                String prefix = "predicates." + predicateName + ".";
                String typeKey = prefix + "type";
                Object classNameObj = config.get(typeKey);
                if (classNameObj == null) {
                    continue;
                }
                String className = classNameObj.toString();
                try {
                    @SuppressWarnings("unchecked")
                    Class<Predicate<SourceRecord>> clazz =
                        (Class<Predicate<SourceRecord>>) Class.forName(className);
                    Predicate<SourceRecord> predicate = clazz.getDeclaredConstructor().newInstance();
                    java.util.Map<String, Object> predicateConfig = config.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(prefix))
                        .collect(java.util.stream.Collectors.toMap(
                            e -> e.getKey().substring(prefix.length()),
                            java.util.Map.Entry::getValue
                        ));
                    log.info("predicate config: {}", predicateConfig);
                    predicate.configure(predicateConfig);
                    predicates.put(predicateName, predicate);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate predicate: " + className, e);
                }
            }
        }
    }

    private void initTransforms(Map<String, Object> config) {
        transformations.clear();
        Object transformsListObj = config.get("transforms");
        if (transformsListObj != null) {
            String transformsList = transformsListObj.toString();
            for (String transformName : transformsList.split(",")) {
                transformName = transformName.trim();
                String prefix = "transforms." + transformName + ".";
                String typeKey = prefix + "type";
                Object classNameObj = config.get(typeKey);
                if (classNameObj == null) {
                    continue;
                }
                String className = classNameObj.toString();
                try {
                    @SuppressWarnings("unchecked")
                    Class<Transformation<SourceRecord>> clazz =
                        (Class<Transformation<SourceRecord>>) Class.forName(className);
                    Transformation<SourceRecord> transform = clazz.getDeclaredConstructor().newInstance();
                    java.util.Map<String, Object> transformConfig = config.entrySet().stream()
                        .filter(e -> e.getKey().startsWith(prefix))
                        .collect(java.util.stream.Collectors.toMap(
                            e -> e.getKey().substring(prefix.length()),
                            java.util.Map.Entry::getValue
                        ));
                    log.info("transform config: {}", transformConfig);
                    String predicateName = (String) transformConfig.get("predicate");
                    boolean negated = Boolean.parseBoolean(
                        String.valueOf(transformConfig.getOrDefault("negate", "false")));
                    Predicate<SourceRecord> predicate = null;
                    if (predicateName != null) {
                        predicate = predicates.get(predicateName);
                        if (predicate == null) {
                            log.warn("Transform {} references non-existent predicate: {}",
                                    transformName, predicateName);
                        }
                    }
                    transform.configure(transformConfig);
                    transformations.add(new PredicatedTransform(predicate, transform, negated));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to instantiate SMT: " + className, e);
                }
            }
        }
    }

    private static final AvroData avroData = new AvroData(1000);

    public synchronized KafkaSourceRecord processSourceRecord(final SourceRecord srcRecord) {
        SourceRecord transformedRecord = applyTransforms(srcRecord);

        offsetWriter.offset(srcRecord.sourcePartition(), srcRecord.sourceOffset());
        if (transformedRecord == null) {
            return null;
        }

        KafkaSourceRecord record = new KafkaSourceRecord(transformedRecord);
        return record;
    }

    public SourceRecord applyTransforms(SourceRecord record) {
        SourceRecord current = record;
        for (PredicatedTransform pt : transformations) {
            if (current == null) {
                break;
            }

            if (pt.predicate != null && !(pt.negated != pt.predicate.test(current))) {
                continue;
            }

            current = pt.transform.apply(current);
        }
        return current;
    }

    public class KafkaSourceRecord extends AbstractKafkaSourceRecord<KeyValue<byte[], byte[]>>
            implements KVRecord<byte[], byte[]> {

        final int keySize;
        final int valueSize;

        final SourceRecord srcRecord;

        KafkaSourceRecord(SourceRecord srcRecord) {
            super(srcRecord);
            this.srcRecord = srcRecord;

            byte[] keyBytes = keyConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.keySchema(), srcRecord.key());
            keySize = keyBytes != null ? keyBytes.length : 0;
            this.key = keyBytes != null ? Optional.of(Base64.getEncoder().encodeToString(keyBytes)) : Optional.empty();

            byte[] valueBytes = valueConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());
            valueSize = valueBytes != null ? valueBytes.length : 0;

            this.value = new KeyValue<>(keyBytes, valueBytes);

            this.topicName = Optional.of(srcRecord.topic());

            if (srcRecord.keySchema() != null) {
                keySchema = readerCache.getIfPresent(srcRecord.keySchema());
            }
            if (srcRecord.valueSchema() != null) {
                valueSchema = readerCache.getIfPresent(srcRecord.valueSchema());
            }

            if (srcRecord.keySchema() != null && keySchema == null) {
                keySchema = new KafkaSchemaWrappedSchema(
                        avroData.fromConnectSchema(srcRecord.keySchema()), keyConverter);
                readerCache.put(srcRecord.keySchema(), keySchema);
            }

            if (srcRecord.valueSchema() != null && valueSchema == null) {
                valueSchema = new KafkaSchemaWrappedSchema(
                        avroData.fromConnectSchema(srcRecord.valueSchema()), valueConverter);
                readerCache.put(srcRecord.valueSchema(), valueSchema);
            }

            this.eventTime = Optional.ofNullable(srcRecord.timestamp());
            this.partitionId = Optional.of(srcRecord.sourcePartition()
                .entrySet()
                .stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(",")));
            this.partitionIndex = Optional.ofNullable(srcRecord.kafkaPartition());
        }

        @Override
        public boolean isEmpty(){
            return this.value.getValue() == null;
        }

        @Override
        public Schema<byte[]> getKeySchema() {
            if (jsonWithEnvelope || keySchema == null) {
                return Schema.BYTES;
            } else {
                return keySchema;
            }
        }

        @Override
        public Schema<byte[]> getValueSchema() {
            if (jsonWithEnvelope || valueSchema == null) {
                return Schema.BYTES;
            } else {
                return valueSchema;
            }
        }

        @Override
        public KeyValueEncodingType getKeyValueEncodingType() {
            if (jsonWithEnvelope) {
                return KeyValueEncodingType.INLINE;
            } else {
                return KeyValueEncodingType.SEPARATED;
            }
        }

        @Override
        public void ack() {
            // first try to commitRecord() for the current record in the batch
            // then call super.ack() which calls commit() after complete batch of records is processed
            try {
                if (log.isDebugEnabled()) {
                    log.debug("commitRecord() for record: {}", srcRecord);
                }
                getSourceTask().commitRecord(srcRecord,
                        new RecordMetadata(
                                new TopicPartition(srcRecord.topic() == null
                                            ? topicName.orElse("UNDEFINED")
                                            : srcRecord.topic(),
                                        srcRecord.kafkaPartition() == null ? 0 : srcRecord.kafkaPartition()),
                                -1L, // baseOffset == -1L means no offset
                                0, // batchIndex, doesn't matter if baseOffset == -1L
                                null == srcRecord.timestamp() ? -1L : srcRecord.timestamp(),
                                keySize, // serializedKeySize
                                valueSize // serializedValueSize
                        ));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Source task failed to commit record, "
                        + "source task should resend data, will get duplicate", e);
                return;
            }
            super.ack();
        }

    }

}
