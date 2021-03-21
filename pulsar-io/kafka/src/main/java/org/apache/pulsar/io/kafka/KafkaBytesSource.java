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

import java.nio.ByteBuffer;
import java.util.*;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AutoProduceBytesSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 * Simple Kafka Source that just transfers the value part of the kafka records
 * as Strings
 */
@Connector(
    name = "kafka",
    type = IOType.SOURCE,
    help = "The KafkaBytesSource is used for moving messages from Kafka to Pulsar.",
    configClass = KafkaSourceConfig.class
)
@Slf4j
public class KafkaBytesSource extends KafkaAbstractSource<ByteBuffer> {

    private AvroSchemaCache schemaCache;
    private Schema keySchema;
    private Schema valueSchema;
    private boolean produceKeyValue;

    @Override
    protected Properties beforeCreateConsumer(Properties props) {
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        log.info("Created kafka consumer config : {}", props);

        keySchema = getSchemaFromDeserializer(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, props, true);
        valueSchema = getSchemaFromDeserializer(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, props, false);

        boolean needsSchemaCache = keySchema instanceof AutoProduceBytesSchema
                                    || valueSchema instanceof AutoProduceBytesSchema;

        if (needsSchemaCache) {
            initSchemaCache(props);
        }

        if (keySchema != Schema.STRING) {
            produceKeyValue = true;
        }
        log.info("keySchema {}, valueSchema {} useKV {}", keySchema, valueSchema, produceKeyValue);

        return props;
    }

    private void initSchemaCache(Properties props) {
        KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(props);
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(urls, maxSchemaObject);
        schemaCache = new AvroSchemaCache(schemaRegistryClient);
    }

    @Override
    public Object extractValue(ConsumerRecord<Object, Object> consumerRecord) {
        if (produceKeyValue) {
            throw new IllegalStateException();
        }
        return extractSimpleValue(consumerRecord.value());
    }

    public KafkaRecord buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
        if (produceKeyValue) {
            Object key = extractSimpleValue(consumerRecord.key());
            Object value = extractSimpleValue(consumerRecord.value());
            Schema currentKeySchema = getSchemaFromObject(consumerRecord.key(), keySchema);
            Schema currentValueSchema = getSchemaFromObject(consumerRecord.value(), valueSchema);
            log.info("buildKVRecord {} {} {} {}", key, value, currentKeySchema, currentValueSchema);
            if (currentKeySchema instanceof AutoProduceBytesSchema) {
                throw new RuntimeException();
            }
            if (currentValueSchema instanceof AutoProduceBytesSchema) {
                throw new RuntimeException();
            }
            return new KeyValueKafkaRecord(consumerRecord,
                    new KeyValue<>(key, value),
                    currentKeySchema,
                    currentValueSchema);

        } else {
            return new KafkaRecord(consumerRecord,
                    extractValue(consumerRecord),
                    extractSchema(consumerRecord));

        }
    }

    private static ByteBuffer extractSimpleValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof BytesWithKafkaSchema) {
            return ((BytesWithKafkaSchema) value).getValue();
        } else if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else {
            throw new IllegalArgumentException("Unexpected type from Kafka: "+value.getClass());
        }
    }

    @Override
    public org.apache.pulsar.client.api.Schema<ByteBuffer> extractSchema(ConsumerRecord<Object, Object> consumerRecord) {

        Object value = consumerRecord.value();
        Schema localValueSchema = getSchemaFromObject(value, valueSchema);

        if (produceKeyValue) {
            throw new IllegalStateException();
        } else {
            return localValueSchema;
        }
    }

    private Schema<ByteBuffer> getSchemaFromObject(Object value, Schema fallback) {
        if (value instanceof BytesWithKafkaSchema) {
            // this is a Struct with schema downloaded by the schema registry
            // the schema may be different from record to record
            return schemaCache.get(((BytesWithKafkaSchema) value).getSchemaId());
        } else {
            return new ByteBufferSchemaWrapper(fallback);
        }
    }

    public static class ExtractKafkaAvroSchemaDeserializer implements Deserializer<BytesWithKafkaSchema> {

        @Override
        public BytesWithKafkaSchema deserialize(String topic, byte[] payload) {
            if (payload == null) {
                return null;
            } else {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(payload);
                    buffer.get(); // magic number
                    int id = buffer.getInt();
                    return new BytesWithKafkaSchema(buffer, id);
                } catch (Exception err) {
                    throw new SerializationException("Error deserializing Avro message", err);
                }
            }
        }
    }

    private static Schema<?> getSchemaFromDeserializer(String key, Properties props, boolean isKey) {
        String kafkaDeserializerClass = props.getProperty(key);
        Objects.requireNonNull(kafkaDeserializerClass);

        // we want to simply transfer the bytes
        props.put(key, ByteBufferDeserializer.class.getCanonicalName());

        if (ByteArrayDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.BYTEBUFFER;
        } else if (ByteBufferDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.BYTEBUFFER;
        } else if (StringDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            if (isKey) {
                props.put(key, StringDeserializer.class.getCanonicalName());
            }
            return Schema.STRING;
        } else if (DoubleDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.DOUBLE;
        } else if (FloatDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.FLOAT;
        } else if (IntegerDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.INT32;
        } else if (LongDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.INT64;
        } else if (ShortDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            return Schema.INT16;
        } else if (KafkaAvroDeserializer.class.getName().equals(kafkaDeserializerClass)){
            // in this case we have to inject our custom deserializer
            // that extracts Avro schema information
            props.put(key, ExtractKafkaAvroSchemaDeserializer.class.getName());
            return Schema.AUTO_PRODUCE_BYTES();
        } else {
            throw new IllegalArgumentException("Unsupported deserializer "+kafkaDeserializerClass);
        }
    }

    Schema getKeySchema() {
        return keySchema;
    }

    Schema getValueSchema() {
        return valueSchema;
    }

    boolean isProduceKeyValue() {
        return produceKeyValue;
    }

}