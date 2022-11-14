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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.functions.api.KVRecord;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.schema.KafkaSchemaWrappedSchema;
import org.apache.pulsar.kafka.shade.io.confluent.connect.avro.AvroData;

import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A pulsar source that runs
 */
@Slf4j
public class KafkaConnectSource extends AbstractKafkaConnectSource<KeyValue<byte[], byte[]>> {

    private final Cache<org.apache.kafka.connect.data.Schema, KafkaSchemaWrappedSchema> readerCache =
            CacheBuilder.newBuilder().maximumSize(10000)
                    .expireAfterAccess(30, TimeUnit.MINUTES).build();

    private boolean jsonWithEnvelope = false;
    private static final String JSON_WITH_ENVELOPE_CONFIG = "json-with-envelope";

    public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
        if (config.get(JSON_WITH_ENVELOPE_CONFIG) != null) {
            jsonWithEnvelope = Boolean.parseBoolean(config.get(JSON_WITH_ENVELOPE_CONFIG).toString());
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, jsonWithEnvelope);
        } else {
            config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        }
        log.info("jsonWithEnvelope: {}", jsonWithEnvelope);
        super.open(config, sourceContext);
    }


    public synchronized KafkaSourceRecord processSourceRecord(final SourceRecord srcRecord) {
        KafkaSourceRecord record = new KafkaSourceRecord(srcRecord);
        offsetWriter.offset(srcRecord.sourcePartition(), srcRecord.sourceOffset());
        return record;
    }

    private static final AvroData avroData = new AvroData(1000);

    private class KafkaSourceRecord extends AbstractKafkaSourceRecord<KeyValue<byte[], byte[]>> implements KVRecord<byte[], byte[]> {

        KafkaSourceRecord(SourceRecord srcRecord) {
            super(srcRecord);
            byte[] keyBytes = keyConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.keySchema(), srcRecord.key());
            this.key = keyBytes != null ? Optional.of(Base64.getEncoder().encodeToString(keyBytes)) : Optional.empty();

            byte[] valueBytes = valueConverter.fromConnectData(
                    srcRecord.topic(), srcRecord.valueSchema(), srcRecord.value());

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

    }

}
