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
package org.apache.pulsar.io.kafka;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.KafkaAbstractSource.KafkaRecord;
import org.apache.pulsar.io.kafka.KafkaAbstractSource.KeyValueKafkaRecord;
import org.bouncycastle.util.encoders.Base64;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class KafkaBytesSourceTest {

    @Test
    public void testNoKeyValueSchema() throws Exception {

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                StringDeserializer.class.getName(), Schema.STRING);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                ByteBufferDeserializer.class.getName(), Schema.BYTEBUFFER);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                BytesDeserializer.class.getName(), Schema.BYTEBUFFER);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                DoubleDeserializer.class.getName(), Schema.DOUBLE);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                FloatDeserializer.class.getName(), Schema.FLOAT);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                IntegerDeserializer.class.getName(), Schema.INT32);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                LongDeserializer.class.getName(), Schema.INT64);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                ShortDeserializer.class.getName(), Schema.INT16);

        validateSchemaNoKeyValue(StringDeserializer.class.getName(), Schema.STRING,
                KafkaAvroDeserializer.class.getName(), KafkaBytesSource.DeferredSchemaPlaceholder.INSTANCE);

    }

    private void validateSchemaNoKeyValue(String keyDeserializationClass, Schema<?> expectedKeySchema,
                                          String valueDeserializationClass, Schema<?> expectedValueSchema) throws Exception {
        try (KafkaBytesSource source = new KafkaBytesSource()) {
            Map<String, Object> config = new HashMap<>();
            config.put("topic", "test");
            config.put("bootstrapServers", "localhost:9092");
            config.put("groupId", "test");
            config.put("valueDeserializationClass", valueDeserializationClass);
            config.put("keyDeserializationClass", keyDeserializationClass);
            config.put("consumerConfigProperties", ImmutableMap.builder()
                    .put("schema.registry.url", "http://localhost:8081")
                    .build());
            source.open(config, Mockito.mock(SourceContext.class));
            assertFalse(source.isProduceKeyValue());
            Schema<ByteBuffer> keySchema = source.getKeySchema();
            Schema<ByteBuffer> valueSchema = source.getValueSchema();
            assertEquals(keySchema.getSchemaInfo().getType(), expectedKeySchema.getSchemaInfo().getType());
            assertEquals(valueSchema.getSchemaInfo().getType(), expectedValueSchema.getSchemaInfo().getType());
        }
    }

    @Test
    public void testKeyValueSchema() throws Exception {
        validateSchemaKeyValue(IntegerDeserializer.class.getName(), Schema.INT32,
                StringDeserializer.class.getName(), Schema.STRING,
                ByteBuffer.wrap(new IntegerSerializer().serialize("test", 10)),
                ByteBuffer.wrap(new StringSerializer().serialize("test", "test")));
    }

    @Test
    public void testCopyKafkaHeadersEnabled() throws Exception {
        ByteBuffer key = ByteBuffer.wrap(new IntegerSerializer().serialize("test", 10));
        ByteBuffer value = ByteBuffer.wrap(new StringSerializer().serialize("test", "test"));
        try (KafkaBytesSource source = new KafkaBytesSource()) {
            Map<String, Object> config = new HashMap<>();
            config.put("copyHeadersEnabled", true);
            config.put("topic", "test");
            config.put("bootstrapServers", "localhost:9092");
            config.put("groupId", "test");
            config.put("valueDeserializationClass", IntegerDeserializer.class.getName());
            config.put("keyDeserializationClass", StringDeserializer.class.getName());
            config.put("consumerConfigProperties", ImmutableMap.builder()
                    .put("schema.registry.url", "http://localhost:8081")
                    .build());
            source.open(config, Mockito.mock(SourceContext.class));
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>("test", 88, 99, key, value);
            record.headers().add("k1", "v1".getBytes(StandardCharsets.UTF_8));
            record.headers().add("k2", new byte[]{0xF});

            Map<String, String> props = source.copyKafkaHeaders(record);
            assertEquals(props.size(), 5);
            assertTrue(props.containsKey("__kafka_topic"));
            assertTrue(props.containsKey("__kafka_partition"));
            assertTrue(props.containsKey("__kafka_offset"));
            assertTrue(props.containsKey("k1"));
            assertTrue(props.containsKey("k2"));

            assertEquals(props.get("__kafka_topic"), "test");
            assertEquals(props.get("__kafka_partition"), "88");
            assertEquals(props.get("__kafka_offset"), "99");
            assertEquals(Base64.decode(props.get("k1")), "v1".getBytes(StandardCharsets.UTF_8));
            assertEquals(Base64.decode(props.get("k2")), new byte[]{0xF});
        }
    }

    @Test
    public void testCopyKafkaHeadersDisabled() throws Exception {
        ByteBuffer key = ByteBuffer.wrap(new IntegerSerializer().serialize("test", 10));
        ByteBuffer value = ByteBuffer.wrap(new StringSerializer().serialize("test", "test"));
        try (KafkaBytesSource source = new KafkaBytesSource()) {
            Map<String, Object> config = new HashMap<>();
            config.put("topic", "test");
            config.put("bootstrapServers", "localhost:9092");
            config.put("groupId", "test");
            config.put("valueDeserializationClass", IntegerDeserializer.class.getName());
            config.put("keyDeserializationClass", StringDeserializer.class.getName());
            config.put("consumerConfigProperties", ImmutableMap.builder()
                    .put("schema.registry.url", "http://localhost:8081")
                    .build());
            source.open(config, Mockito.mock(SourceContext.class));
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>("test", 88, 99, key, value);
            record.headers().add("k1", "v1".getBytes(StandardCharsets.UTF_8));
            record.headers().add("k2", new byte[]{0xF});

            Map<String, String> props = source.copyKafkaHeaders(record);
            assertTrue(props.isEmpty());
        }
    }

    private void validateSchemaKeyValue(String keyDeserializationClass, Schema<?> expectedKeySchema,
                                          String valueDeserializationClass, Schema<?> expectedValueSchema,
                                          ByteBuffer key,
                                        ByteBuffer value) throws Exception {
        try (KafkaBytesSource source = new KafkaBytesSource()) {
            Map<String, Object> config = new HashMap<>();
            config.put("topic", "test");
            config.put("bootstrapServers", "localhost:9092");
            config.put("groupId", "test");
            config.put("valueDeserializationClass", valueDeserializationClass);
            config.put("keyDeserializationClass", keyDeserializationClass);
            config.put("consumerConfigProperties", ImmutableMap.builder()
                    .put("schema.registry.url", "http://localhost:8081")
                    .build());
            source.open(config, Mockito.mock(SourceContext.class));
            assertTrue(source.isProduceKeyValue());
            Schema<ByteBuffer> keySchema = source.getKeySchema();
            Schema<ByteBuffer> valueSchema = source.getValueSchema();
            assertEquals(keySchema.getSchemaInfo().getType(), expectedKeySchema.getSchemaInfo().getType());
            assertEquals(valueSchema.getSchemaInfo().getType(), expectedValueSchema.getSchemaInfo().getType());

            KafkaRecord<ByteBuffer> record = source.buildRecord(new ConsumerRecord<>("test", 0, 0, key, value));
            assertThat(record, instanceOf(KeyValueKafkaRecord.class));
            KeyValueKafkaRecord<ByteBuffer, ByteBuffer> kvRecord = (KeyValueKafkaRecord<ByteBuffer, ByteBuffer>) record;
            assertSame(keySchema, kvRecord.getKeySchema());
            assertSame(valueSchema, kvRecord.getValueSchema());
            assertEquals(KeyValueEncodingType.SEPARATED, kvRecord.getKeyValueEncodingType());
            KeyValue<ByteBuffer, ByteBuffer> kvValue = (KeyValue<ByteBuffer, ByteBuffer>) kvRecord.getValue();
            log.info("key {}", Arrays.toString(toArray(key)));
            log.info("value {}", Arrays.toString(toArray(value)));
            log.info("key {}", Arrays.toString(toArray(kvValue.getKey())));
            log.info("value {}", Arrays.toString(toArray(kvValue.getValue())));

            assertEquals(ByteBuffer.wrap(toArray(key)).compareTo(kvValue.getKey()), 0);
            assertEquals(ByteBuffer.wrap(toArray(value)).compareTo(kvValue.getValue()), 0);
        }
    }

    private static byte[] toArray(ByteBuffer b) {
        byte[]res = new byte[b.remaining()];
        b.mark();
        b.get(res);
        b.reset();
        return res;
    }
}
