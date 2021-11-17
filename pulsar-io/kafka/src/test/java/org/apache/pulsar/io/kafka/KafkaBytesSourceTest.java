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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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

    private void validateSchemaNoKeyValue(String keyDeserializationClass, Schema expectedKeySchema,
                                          String valueDeserializationClass, Schema expectedValueSchema) throws Exception {
        KafkaBytesSource source = new KafkaBytesSource();
        Map<String, Object> config = new HashMap<>();
        config.put("topic","test");
        config.put("bootstrapServers","localhost:9092");
        config.put("groupId", "test");
        config.put("valueDeserializationClass", valueDeserializationClass);
        config.put("keyDeserializationClass", keyDeserializationClass);
        config.put("consumerConfigProperties", ImmutableMap.builder()
                .put("schema.registry.url", "http://localhost:8081")
                .build());
        source.open(config, Mockito.mock(SourceContext.class));
        assertFalse(source.isProduceKeyValue());
        Schema keySchema  = source.getKeySchema();
        Schema valueSchema = source.getValueSchema();
        assertEquals(keySchema.getSchemaInfo().getType(), expectedKeySchema.getSchemaInfo().getType());
        assertEquals(valueSchema.getSchemaInfo().getType(), expectedValueSchema.getSchemaInfo().getType());
    }

    @Test
    public void testKeyValueSchema() throws Exception {
        validateSchemaKeyValue(IntegerDeserializer.class.getName(), Schema.INT32,
                StringDeserializer.class.getName(), Schema.STRING,
                ByteBuffer.wrap(new IntegerSerializer().serialize("test", 10)),
                ByteBuffer.wrap(new StringSerializer().serialize("test", "test")));
    }

    private void validateSchemaKeyValue(String keyDeserializationClass, Schema expectedKeySchema,
                                          String valueDeserializationClass, Schema expectedValueSchema,
                                          ByteBuffer key,
                                        ByteBuffer value) throws Exception {
        KafkaBytesSource source = new KafkaBytesSource();
        Map<String, Object> config = new HashMap<>();
        config.put("topic","test");
        config.put("bootstrapServers","localhost:9092");
        config.put("groupId", "test");
        config.put("valueDeserializationClass", valueDeserializationClass);
        config.put("keyDeserializationClass", keyDeserializationClass);
        config.put("consumerConfigProperties", ImmutableMap.builder()
                .put("schema.registry.url", "http://localhost:8081")
                .build());
        source.open(config, Mockito.mock(SourceContext.class));
        assertTrue(source.isProduceKeyValue());
        Schema keySchema  = source.getKeySchema();
        Schema valueSchema = source.getValueSchema();
        assertEquals(keySchema.getSchemaInfo().getType(), expectedKeySchema.getSchemaInfo().getType());
        assertEquals(valueSchema.getSchemaInfo().getType(), expectedValueSchema.getSchemaInfo().getType());

        KafkaAbstractSource.KafkaRecord record = source.buildRecord(new ConsumerRecord<Object, Object>("test", 0, 0, key, value));
        assertThat(record, instanceOf(KafkaAbstractSource.KeyValueKafkaRecord.class));
        KafkaAbstractSource.KeyValueKafkaRecord kvRecord = (KafkaAbstractSource.KeyValueKafkaRecord) record;
        assertSame(keySchema, kvRecord.getKeySchema());
        assertSame(valueSchema, kvRecord.getValueSchema());
        assertEquals(KeyValueEncodingType.SEPARATED, kvRecord.getKeyValueEncodingType());
        KeyValue kvValue = (KeyValue) kvRecord.getValue();
        log.info("key {}", Arrays.toString(toArray(key)));
        log.info("value {}", Arrays.toString(toArray(value)));

        log.info("key {}", Arrays.toString(toArray((ByteBuffer) kvValue.getKey())));
        log.info("value {}", Arrays.toString(toArray((ByteBuffer) kvValue.getValue())));

        assertEquals(ByteBuffer.wrap(toArray(key)).compareTo((ByteBuffer) kvValue.getKey()), 0);
        assertEquals(ByteBuffer.wrap(toArray(value)).compareTo((ByteBuffer) kvValue.getValue()), 0);
    }

    private static byte[] toArray(ByteBuffer b) {
        byte[]res = new byte[b.remaining()];
        b.mark();
        b.get(res);
        b.reset();
        return res;
    }
}
