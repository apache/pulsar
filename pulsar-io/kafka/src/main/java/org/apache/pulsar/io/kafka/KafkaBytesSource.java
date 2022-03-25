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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

/**
 *  Kafka Source that transfers the data from Kafka to Pulsar and sets the Schema type properly.
 *  We use the key and the value deserializer in order to decide the type of Schema to be set on the topic on Pulsar.
 *  In case of KafkaAvroDeserializer we use the Schema Registry to download the schema and apply it to the topic.
 *  Please refer to {@link #getSchemaFromDeserializerAndAdaptConfiguration(String, Properties, boolean)} for the list
 *  of supported Deserializers.
 *  If you set StringDeserializer for the key then we use the raw key as key for the Pulsar message.
 *  If you set another Deserializer for the key we use the KeyValue schema type in Pulsar with the SEPARATED encoding.
 *  This way the Key is stored in the Pulsar key, encoded as base64 string and with a Schema, the Value of the message
 *  is stored in the Pulsar value with a Schema.
 *  This way there is a one-to-one mapping between Kafka key/value pair and the Pulsar data model.
 */
@Connector(
    name = "kafka",
    type = IOType.SOURCE,
    help = "Transfer data from Kafka to Pulsar.",
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

        keySchema = getSchemaFromDeserializerAndAdaptConfiguration(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                props, true);
        valueSchema = getSchemaFromDeserializerAndAdaptConfiguration(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                props, false);

        boolean needsSchemaCache = keySchema == DeferredSchemaPlaceholder.INSTANCE
                                    || valueSchema == DeferredSchemaPlaceholder.INSTANCE;

        if (needsSchemaCache) {
            initSchemaCache(props);
        }

        if (keySchema.getSchemaInfo().getType() != SchemaType.STRING) {
            // if the Key is a String we can use native Pulsar Key
            // otherwise we use KeyValue schema
            // that allows you to set a schema for the Key and a schema for the Value.
            // using SEPARATED encoding the key is saved into the binary key
            // so it is used for routing and for compaction
            produceKeyValue = true;
        }

        return props;
    }

    private void initSchemaCache(Properties props) {
        KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(props);
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(urls, maxSchemaObject);
        log.info("initializing SchemaRegistry Client, urls:{}, maxSchemasPerSubject: {}", urls, maxSchemaObject);
        schemaCache = new AvroSchemaCache(schemaRegistryClient);
    }

    @Override
    public KafkaRecord buildRecord(ConsumerRecord<Object, Object> consumerRecord) {
        if (produceKeyValue) {
            Object key = extractSimpleValue(consumerRecord.key());
            Object value = extractSimpleValue(consumerRecord.value());
            Schema currentKeySchema = getSchemaFromObject(consumerRecord.key(), keySchema);
            Schema currentValueSchema = getSchemaFromObject(consumerRecord.value(), valueSchema);
            return new KeyValueKafkaRecord(consumerRecord,
                    new KeyValue<>(key, value),
                    currentKeySchema,
                    currentValueSchema);

        } else {
            Object value = consumerRecord.value();
            return new KafkaRecord(consumerRecord,
                    extractSimpleValue(value),
                    getSchemaFromObject(value, valueSchema));

        }
    }

    private static ByteBuffer extractSimpleValue(Object value) {
        // we have substituted the original Deserializer with
        // ByteBufferDeserializer in order to save memory copies
        // so here we can have only a ByteBuffer or at most a
        // BytesWithKafkaSchema in case of ExtractKafkaAvroSchemaDeserializer
        if (value == null) {
            return null;
        } else if (value instanceof BytesWithKafkaSchema) {
            return ((BytesWithKafkaSchema) value).getValue();
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else {
            throw new IllegalArgumentException("Unexpected type from Kafka: " + value.getClass());
        }
    }

    private Schema<ByteBuffer> getSchemaFromObject(Object value, Schema fallback) {
        if (value instanceof BytesWithKafkaSchema) {
            // this is a Struct with schema downloaded by the schema registry
            // the schema may be different from record to record
            return schemaCache.get(((BytesWithKafkaSchema) value).getSchemaId());
        } else {
            return fallback;
        }
    }

    private static Schema<ByteBuffer> getSchemaFromDeserializerAndAdaptConfiguration(String key, Properties props,
                                                                                     boolean isKey) {
        String kafkaDeserializerClass = props.getProperty(key);
        Objects.requireNonNull(kafkaDeserializerClass);

        // we want to simply transfer the bytes,
        // by default we override the Kafka Consumer configuration
        // to pass the original ByteBuffer
        props.put(key, ByteBufferDeserializer.class.getCanonicalName());

        Schema<?> result;
        if (ByteArrayDeserializer.class.getName().equals(kafkaDeserializerClass)
            || ByteBufferDeserializer.class.getName().equals(kafkaDeserializerClass)
            || BytesDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.BYTEBUFFER;
        } else if (StringDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            if (isKey) {
                // for the key we use the String value and we want StringDeserializer
                props.put(key, kafkaDeserializerClass);
            }
            result = Schema.STRING;
        } else if (DoubleDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.DOUBLE;
        } else if (FloatDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.FLOAT;
        } else if (IntegerDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT32;
        } else if (LongDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT64;
        } else if (ShortDeserializer.class.getName().equals(kafkaDeserializerClass)) {
            result = Schema.INT16;
        } else if (KafkaAvroDeserializer.class.getName().equals(kafkaDeserializerClass)){
            // in this case we have to inject our custom deserializer
            // that extracts Avro schema information
            props.put(key, ExtractKafkaAvroSchemaDeserializer.class.getName());
            // this is only a placeholder, we are not really using AUTO_PRODUCE_BYTES
            // but we the schema is created by downloading the definition from the SchemaRegistry
            return DeferredSchemaPlaceholder.INSTANCE;
        } else {
            throw new IllegalArgumentException("Unsupported deserializer " + kafkaDeserializerClass);
        }
        return new ByteBufferSchemaWrapper(result);
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

     static final class DeferredSchemaPlaceholder extends ByteBufferSchemaWrapper {
        DeferredSchemaPlaceholder() {
            super(SchemaInfoImpl
                    .builder()
                    .type(SchemaType.AVRO)
                    .properties(Collections.emptyMap())
                    .schema(new byte[0])
                    .build());
        }
        static final DeferredSchemaPlaceholder INSTANCE = new DeferredSchemaPlaceholder();
    }

}