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
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.schema.GenericRecord;
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
public class KafkaBytesSource extends KafkaAbstractSource<byte[]> {

    private AvroSchemaCache<GenericRecord> schemaCache;

    private static final Collection<String> SUPPORTED_KEY_DESERIALIZERS =
            Collections.unmodifiableCollection(Arrays.asList(StringDeserializer.class.getName()));

    private static final Collection<String> SUPPORTED_VALUE_DESERIALIZERS =
            Collections.unmodifiableCollection(Arrays.asList(ByteArrayDeserializer.class.getName(), KafkaAvroDeserializer.class.getName()));

    @Override
    protected Properties beforeCreateConsumer(Properties props) {
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        log.info("Created kafka consumer config : {}", props);

        String currentKeyDeserializer = props.getProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
        if (!SUPPORTED_KEY_DESERIALIZERS.contains(currentKeyDeserializer)) {
            throw new IllegalArgumentException("Unsupported key deserializer: " + currentKeyDeserializer + ", only " + SUPPORTED_KEY_DESERIALIZERS);
        }

        String currentValueDeserializer = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        if (!SUPPORTED_VALUE_DESERIALIZERS.contains(currentValueDeserializer)) {
            throw new IllegalArgumentException("Unsupported value deserializer: " + currentValueDeserializer + ", only " + SUPPORTED_VALUE_DESERIALIZERS);
        }

        // replace KafkaAvroDeserializer with our custom implementation
        if (currentValueDeserializer != null && currentValueDeserializer.equals(KafkaAvroDeserializer.class.getName())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SchemaExtractorDeserializer.class.getName());
            KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(props);
            List<String> urls = config.getSchemaRegistryUrls();
            int maxSchemaObject = config.getMaxSchemasPerSubject();
            SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(urls, maxSchemaObject);
            schemaCache = new AvroSchemaCache<GenericRecord>(schemaRegistryClient);
        }
        return props;
    }

    @Override
    public Object extractValue(ConsumerRecord<Object, Object> consumerRecord) {
        Object value = consumerRecord.value();
        if (value instanceof BytesWithSchema) {
            return ((BytesWithSchema) value).getValue();
        }
        return value;
    }

    @Override
    public org.apache.pulsar.client.api.Schema<?> extractSchema(ConsumerRecord<Object, Object> consumerRecord) {
        Object value = consumerRecord.value();
        if (value instanceof BytesWithSchema) {
            return schemaCache.get(((BytesWithSchema) value).getSchemaId());
        } else {
            return org.apache.pulsar.client.api.Schema.BYTES;
        }
    }

    public static class SchemaExtractorDeserializer implements Deserializer<BytesWithSchema> {

        @Override
        public BytesWithSchema deserialize(String topic, byte[] payload) {
            if (payload == null) {
                return null;
            } else {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(payload);
                    buffer.get(); // magic number
                    int id = buffer.getInt();
                    byte[] avroEncodedData = new byte[buffer.remaining()];
                    buffer.get(avroEncodedData);
                    return new BytesWithSchema(avroEncodedData, id);
                } catch (Exception err) {
                    throw new SerializationException("Error deserializing Avro message", err);
                }
            }
        }
    }

}
