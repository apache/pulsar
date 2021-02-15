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
import java.util.Properties;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
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

    @Override
    protected Properties beforeCreateConsumer(Properties props) {
        props.putIfAbsent("schema.registry.url", "http://localhost:8081");
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());

        String currentValue = props.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        // replace KafkaAvroDeserializer with our custom implementation
        if (currentValue != null && currentValue.equals(KafkaAvroDeserializer.class.getName())) {
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, NoCopyKafkaAvroDeserializer.class.getName());
        }

        log.info("Created kafka consumer config : {}", props);
        return props;
    }


    @Override
    public Object convert(ConsumerRecord<String, Object> record) {
        Object value = record.value();
        if (value instanceof RecordWithSchema) {
            RecordWithSchema recordWithSchema = (RecordWithSchema) record.value();
            return new BytesWithAvroPulsarSchema(recordWithSchema.getSchema(), recordWithSchema.getValue(), schemaCache);
        } else {
            return value;
        }
    }

    @Override
    public Object extractValue(Object value) {
        if (value instanceof BytesWithAvroPulsarSchema) {
            return ((BytesWithAvroPulsarSchema) value).getValue();
        }
        return value;
    }

    @Override
    public org.apache.pulsar.client.api.Schema<?> extractSchema(Object value) {
        if (value instanceof BytesWithAvroPulsarSchema) {
            return ((BytesWithAvroPulsarSchema) value).getPulsarSchema();
        } else if (value instanceof String) {
            return org.apache.pulsar.client.api.Schema.STRING;
        } else {
            return org.apache.pulsar.client.api.Schema.BYTES;
        }
    }

    private final PulsarSchemaCache<GenericRecord> schemaCache = new PulsarSchemaCache<>();

    @AllArgsConstructor
    @Getter
    private static class RecordWithSchema {
        byte[] value;
        Schema schema;
    }

    public static class NoCopyKafkaAvroDeserializer extends KafkaAvroDeserializer {

        @Override
        protected Object deserialize(boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload, Schema readerSchema) throws SerializationException {
            if (payload == null) {
                return null;
            } else {
                int id = -1;
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(payload);
                    buffer.get(); // magic number
                    id = buffer.getInt();
                    String subject = getSubjectName(topic, isKey != null ? isKey : false);
                    Schema schema = this.schemaRegistry.getBySubjectAndId(subject, id);
                    byte[] avroEncodedData = new byte[buffer.remaining()];
                    buffer.get(avroEncodedData);
                    return new RecordWithSchema(
                            avroEncodedData,
                            schema
                    );
                } catch (Exception err) {
                    throw new SerializationException("Error deserializing Avro message for id " + id, err);
                }
            }
        }
    }

}
