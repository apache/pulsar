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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
public class KafkaConnectSinkTest extends ProducerConsumerBase  {

    public class ResultCaptor<T> implements Answer {
        private T result = null;
        public T getResult() {
            return result;
        }

        @Override
        public T answer(InvocationOnMock invocationOnMock) throws Throwable {
            result = (T) invocationOnMock.callRealMethod();
            return result;
        }
    }

    private String offsetTopicName =  "persistent://my-property/my-ns/kafka-connect-sink-offset";

    private Path file;
    private Map<String, Object> props;
    private SinkContext context;
    private PulsarClient client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        props = Maps.newHashMap();
        props.put("topic", "test-topic");
        props.put("offsetStorageTopic", offsetTopicName);
        props.put("kafkaConnectorSinkClass", "org.apache.kafka.connect.file.FileStreamSinkConnector");

        Map<String, String> kafkaConnectorProps = Maps.newHashMap();
        kafkaConnectorProps.put("file", file.toString());
        props.put("kafkaConnectorConfigProperties", kafkaConnectorProps);

        this.context = mock(SinkContext.class);
        this.client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build();
        when(context.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        when(context.getPulsarClient()).thenReturn(client);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (file != null && Files.exists(file)) {
            Files.delete(file);
        }

        if (this.client != null) {
            client.close();
        }

        super.internalCleanup();
    }

    @Test
    public void smokeTest() throws Exception {
        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        final GenericRecord rec = getGenericRecord("value", Schema.STRING);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .schema(Schema.STRING)
                .build();

        sink.write(record);
        sink.flush();

        assertEquals(status.get(), 1);

        sink.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        assertEquals(lines.get(0), "value");
    }

    @Test
    public void sanitizeTest() throws Exception {
        props.put("sanitizeTopicName", "true");
        KafkaConnectSink originalSink = new KafkaConnectSink();
        KafkaConnectSink sink = spy(originalSink);

        final ResultCaptor<SinkRecord> resultCaptor = new ResultCaptor<>();
        doAnswer(resultCaptor).when(sink).toSinkRecord(any());

        sink.open(props, context);

        final GenericRecord rec = getGenericRecord("value", Schema.STRING);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("persistent://a-b/c-d/fake-topic.a")
                .message(msg)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .schema(Schema.STRING)
                .build();

        sink.write(record);
        sink.flush();

        assertTrue(sink.currentOffset("persistent://a-b/c-d/fake-topic.a", 0) > 0L);

        assertEquals(status.get(), 1);
        assertEquals(resultCaptor.getResult().topic(), "persistent___a_b_c_d_fake_topic_a");

        sink.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        assertEquals(lines.get(0), "value");
    }

    @Test
    public void seekPauseResumeTest() throws Exception {
        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        final GenericRecord rec = getGenericRecord("value", Schema.STRING);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        final MessageId msgId = new MessageIdImpl(10, 10, 0);
        when(msg.getMessageId()).thenReturn(msgId);

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .schema(Schema.STRING)
                .build();

        sink.write(record);
        sink.flush();

        assertEquals(status.get(), 1);

        final TopicPartition tp = new TopicPartition("fake-topic", 0);
        assertNotEquals(MessageIdUtils.getOffset(msgId), 0);
        assertEquals(sink.currentOffset(tp.topic(), tp.partition()), MessageIdUtils.getOffset(msgId));

        sink.taskContext.offset(tp, 0);
        verify(context, times(1)).seek(Mockito.anyString(), Mockito.anyInt(), any());
        assertEquals(sink.currentOffset(tp.topic(), tp.partition()), 0);

        sink.taskContext.pause(tp);
        verify(context, times(1)).pause(tp.topic(), tp.partition());
        sink.taskContext.resume(tp);
        verify(context, times(1)).resume(tp.topic(), tp.partition());

        sink.close();
    }


    @Test
    public void subscriptionTypeTest() throws Exception {
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Failover is allowed");
            sink.open(props, context);
        }

        when(context.getSubscriptionType()).thenReturn(SubscriptionType.Exclusive);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Exclusive is allowed");
            sink.open(props, context);
        }

        when(context.getSubscriptionType()).thenReturn(SubscriptionType.Key_Shared);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Key_Shared is not allowed");
            sink.open(props, context);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

        when(context.getSubscriptionType()).thenReturn(SubscriptionType.Shared);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Shared is not allowed");
            sink.open(props, context);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

        when(context.getSubscriptionType()).thenReturn(null);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Type is required");
            sink.open(props, context);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

    }

    private SinkRecord recordSchemaTest(Object value, Schema schema, Object expected, String expectedSchema) throws Exception {
        return recordSchemaTest(value, schema, "key",  "STRING", expected, expectedSchema);
    }

    private SinkRecord recordSchemaTest(Object value, Schema schema, Object expectedKey, String expectedKeySchema,
                                  Object expected, String expectedSchema) throws Exception {
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        final GenericRecord rec = getGenericRecord(value, schema);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .schema(schema)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        sink.write(record);
        sink.flush();

        assertEquals(status.get(), 1);

        sink.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> result = om.readValue(lines.get(0), new TypeReference<Map<String, Object>>(){});

        assertEquals(expectedKey, result.get("key"));
        assertEquals(expected, result.get("value"));
        assertEquals(expectedKeySchema, result.get("keySchema"));
        assertEquals(expectedSchema, result.get("valueSchema"));

        if (schema.getSchemaInfo().getType().isPrimitive()) {
            // to test cast of primitive values
            Message msgOut = mock(MessageImpl.class);
            when(msgOut.getValue()).thenReturn(getGenericRecord(result.get("value"), schema));
            when(msgOut.getKey()).thenReturn(result.get("key").toString());
            when(msgOut.hasKey()).thenReturn(true);
            when(msgOut.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

            Record<GenericObject> recordOut = PulsarRecord.<String>builder()
                    .topicName("fake-topic")
                    .message(msgOut)
                    .schema(schema)
                    .ackFunction(status::incrementAndGet)
                    .failFunction(status::decrementAndGet)
                    .build();

            SinkRecord sinkRecord = sink.toSinkRecord(recordOut);
            return sinkRecord;
        } else {
            SinkRecord sinkRecord = sink.toSinkRecord(record);
            return sinkRecord;
        }
    }

    private GenericRecord getGenericRecord(Object value, Schema schema) {
        final GenericRecord rec;
        if (value instanceof GenericRecord) {
            rec = (GenericRecord) value;
        } else if (value instanceof org.apache.avro.generic.GenericRecord) {
            org.apache.avro.generic.GenericRecord avroRecord =
                    (org.apache.avro.generic.GenericRecord) value;
            org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) schema.getNativeSchema().get();
            List<Field> fields = avroSchema.getFields()
                    .stream()
                    .map(f -> new Field(f.name(), f.pos()))
                    .collect(Collectors.toList());

            return new GenericAvroRecord(new byte[]{ 1 }, avroSchema, fields, avroRecord);
        } else {
            rec = MockGenericObjectWrapper.builder()
                    .nativeObject(value)
                    .schemaType(schema != null ? schema.getSchemaInfo().getType() : null)
                    .schemaVersion(new byte[]{ 1 }).build();
        }
        return rec;
    }


    @Test
    public void genericRecordCastTest() throws Exception {
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        AvroSchema<PulsarSchemaToKafkaSchemaTest.StructWithAnnotations> pulsarAvroSchema
                = AvroSchema.of(PulsarSchemaToKafkaSchemaTest.StructWithAnnotations.class);

        final GenericData.Record obj = new GenericData.Record(pulsarAvroSchema.getAvroSchema());
        // schema type INT32
        obj.put("field1", (byte)10);
        // schema type STRING
        obj.put("field2", "test");
        // schema type INT64
        obj.put("field3", (short)100);

        final GenericRecord rec = getGenericRecord(obj, pulsarAvroSchema);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .schema(pulsarAvroSchema)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        SinkRecord sinkRecord = sink.toSinkRecord(record);

        Struct out = (Struct) sinkRecord.value();
        Assert.assertEquals(out.get("field1").getClass(), Integer.class);
        Assert.assertEquals(out.get("field2").getClass(), String.class);
        Assert.assertEquals(out.get("field3").getClass(), Long.class);

        Assert.assertEquals(out.get("field1"), 10);
        Assert.assertEquals(out.get("field2"), "test");
        Assert.assertEquals(out.get("field3"), 100L);

        sink.close();
    }

    @Test
    public void bytesRecordSchemaTest() throws Exception {
        byte[] in = "val".getBytes(StandardCharsets.US_ASCII);
        SinkRecord sinkRecord = recordSchemaTest(in, Schema.BYTES, "val", "BYTES");
        // test/mock writes it as string
        Assert.assertEquals(sinkRecord.value(), "val");
    }

    @Test
    public void stringRecordSchemaTest() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest("val", Schema.STRING, "val", "STRING");
        Assert.assertEquals(sinkRecord.value().getClass(), String.class);
        Assert.assertEquals(sinkRecord.value(), "val");
    }

    @Test
    public void booleanRecordSchemaTest() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest(true, Schema.BOOL, true, "BOOLEAN");
        Assert.assertEquals(sinkRecord.value().getClass(), Boolean.class);
        Assert.assertEquals(sinkRecord.value(), true);
    }

    @Test
    public void byteRecordSchemaTest() throws Exception {
        // int 1 is coming back from ObjectMapper
        SinkRecord sinkRecord = recordSchemaTest((byte)1, Schema.INT8, 1, "INT8");
        Assert.assertEquals(sinkRecord.value().getClass(), Byte.class);
        Assert.assertEquals(sinkRecord.value(), (byte)1);
    }

    @Test
    public void shortRecordSchemaTest() throws Exception {
        // int 1 is coming back from ObjectMapper
        SinkRecord sinkRecord = recordSchemaTest((short)1, Schema.INT16, 1, "INT16");
        Assert.assertEquals(sinkRecord.value().getClass(), Short.class);
        Assert.assertEquals(sinkRecord.value(), (short)1);
    }

    @Test
    public void integerRecordSchemaTest() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest(Integer.MAX_VALUE, Schema.INT32, Integer.MAX_VALUE, "INT32");
        Assert.assertEquals(sinkRecord.value().getClass(), Integer.class);
        Assert.assertEquals(sinkRecord.value(), Integer.MAX_VALUE);
    }

    @Test
    public void longRecordSchemaTest() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest(Long.MAX_VALUE, Schema.INT64, Long.MAX_VALUE, "INT64");
        Assert.assertEquals(sinkRecord.value().getClass(), Long.class);
        Assert.assertEquals(sinkRecord.value(), Long.MAX_VALUE);
    }

    @Test
    public void longRecordSchemaTestCast() throws Exception {
        // int 1 is coming from ObjectMapper, expect Long (as in schema) from sinkRecord
        SinkRecord sinkRecord = recordSchemaTest(1L, Schema.INT64, 1, "INT64");
        Assert.assertEquals(sinkRecord.value().getClass(), Long.class);
        Assert.assertEquals(sinkRecord.value(), 1L);
    }

    @Test
    public void floatRecordSchemaTest() throws Exception {
        // 1.0d is coming back from ObjectMapper, expect Float (as in schema) from sinkRecord
        SinkRecord sinkRecord = recordSchemaTest(1.0f, Schema.FLOAT, 1.0d, "FLOAT32");
        Assert.assertEquals(sinkRecord.value().getClass(), Float.class);
        Assert.assertEquals(sinkRecord.value(), 1.0f);
    }

    @Test
    public void doubleRecordSchemaTest() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest(Double.MAX_VALUE, Schema.DOUBLE, Double.MAX_VALUE, "FLOAT64");
        Assert.assertEquals(sinkRecord.value().getClass(), Double.class);
        Assert.assertEquals(sinkRecord.value(), Double.MAX_VALUE);
    }

    @Test
    public void doubleRecordSchemaTestCast() throws Exception {
        SinkRecord sinkRecord = recordSchemaTest(1.0d, Schema.DOUBLE, 1.0d, "FLOAT64");
        Assert.assertEquals(sinkRecord.value().getClass(), Double.class);
        Assert.assertEquals(sinkRecord.value(), 1.0d);
    }

    @Test
    public void jsonSchemaTest() throws Exception {
        JSONSchema<PulsarSchemaToKafkaSchemaTest.StructWithAnnotations> jsonSchema = JSONSchema
                .of(SchemaDefinition.<PulsarSchemaToKafkaSchemaTest.StructWithAnnotations>builder()
                        .withPojo(PulsarSchemaToKafkaSchemaTest.StructWithAnnotations.class)
                        .withAlwaysAllowNull(false)
                        .build());
        PulsarSchemaToKafkaSchemaTest.StructWithAnnotations obj = new PulsarSchemaToKafkaSchemaTest.StructWithAnnotations();
        obj.setField1(10);
        obj.setField2("test");
        obj.setField3(100L);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.valueToTree(obj);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("field1", 10);
        expected.put("field2", "test");
        // integer is coming back from ObjectMapper
        expected.put("field3", 100);

        SinkRecord sinkRecord = recordSchemaTest(jsonNode, jsonSchema, expected, "STRUCT");

        Struct out = (Struct) sinkRecord.value();
        Assert.assertEquals(out.get("field1").getClass(), Integer.class);
        Assert.assertEquals(out.get("field1"), 10);
        Assert.assertEquals(out.get("field2").getClass(), String.class);
        Assert.assertEquals(out.get("field2"), "test");
        Assert.assertEquals(out.get("field3").getClass(), Long.class);
        Assert.assertEquals(out.get("field3"), 100L);
    }

    @Test
    public void avroSchemaTest() throws Exception {
        AvroSchema<PulsarSchemaToKafkaSchemaTest.StructWithAnnotations> pulsarAvroSchema
                = AvroSchema.of(PulsarSchemaToKafkaSchemaTest.StructWithAnnotations.class);

        final GenericData.Record obj = new GenericData.Record(pulsarAvroSchema.getAvroSchema());
        obj.put("field1", 10);
        obj.put("field2", "test");
        obj.put("field3", 100L);

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("field1", 10);
        expected.put("field2", "test");
        // integer is coming back from ObjectMapper
        expected.put("field3", 100);

        SinkRecord sinkRecord = recordSchemaTest(obj, pulsarAvroSchema, expected, "STRUCT");

        Struct out = (Struct) sinkRecord.value();
        Assert.assertEquals(out.get("field1"), 10);
        Assert.assertEquals(out.get("field2"), "test");
        Assert.assertEquals(out.get("field3"), 100L);
    }

    @Test
    public void unknownRecordSchemaTest() throws Exception {
        Object obj = new Object();
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        final GenericRecord rec = getGenericRecord(obj, null);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(0, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        sink.write(record);
        sink.flush();

        assertEquals(status.get(), -1, "write should fail for unsupported schema");

        sink.close();
    }

    @Test
    public void schemaKeyValueSchemaTest() throws Exception {
        KeyValue<Integer, String> kv = new KeyValue<>(11, "value");
        SinkRecord sinkRecord = recordSchemaTest(kv, Schema.KeyValue(Schema.INT32, Schema.STRING), 11, "INT32", "value", "STRING");
        String val = (String) sinkRecord.value();
        Assert.assertEquals(val, "value");
        int key = (int) sinkRecord.key();
        Assert.assertEquals(key, 11);
    }

    @Test
    public void schemaKeyValueAvroSchemaTest() throws Exception {
        AvroSchema<PulsarSchemaToKafkaSchemaTest.StructWithAnnotations> pulsarAvroSchema
                = AvroSchema.of(PulsarSchemaToKafkaSchemaTest.StructWithAnnotations.class);

        final GenericData.Record key = new GenericData.Record(pulsarAvroSchema.getAvroSchema());
        key.put("field1", 11);
        key.put("field2", "key");
        key.put("field3", 101L);

        final GenericData.Record value = new GenericData.Record(pulsarAvroSchema.getAvroSchema());
        value.put("field1", 10);
        value.put("field2", "value");
        value.put("field3", 100L);

        Map<String, Object> expectedKey = new LinkedHashMap<>();
        expectedKey.put("field1", 11);
        expectedKey.put("field2", "key");
        // integer is coming back from ObjectMapper
        expectedKey.put("field3", 101);

        Map<String, Object> expectedValue = new LinkedHashMap<>();
        expectedValue.put("field1", 10);
        expectedValue.put("field2", "value");
        // integer is coming back from ObjectMapper
        expectedValue.put("field3", 100);

        KeyValue<GenericRecord, GenericRecord> kv = new KeyValue<>(getGenericRecord(key, pulsarAvroSchema),
                            getGenericRecord(value, pulsarAvroSchema));

        SinkRecord sinkRecord = recordSchemaTest(kv, Schema.KeyValue(pulsarAvroSchema, pulsarAvroSchema),
                expectedKey, "STRUCT", expectedValue, "STRUCT");

        Struct outValue = (Struct) sinkRecord.value();
        Assert.assertEquals((int)outValue.get("field1"), 10);
        Assert.assertEquals((String)outValue.get("field2"), "value");
        Assert.assertEquals((long)outValue.get("field3"), 100L);

        Struct outKey = (Struct) sinkRecord.key();
        Assert.assertEquals((int)outKey.get("field1"), 11);
        Assert.assertEquals((String)outKey.get("field2"), "key");
        Assert.assertEquals((long)outKey.get("field3"), 101L);
    }

    @Test
    public void nullKeyValueSchemaTest() throws Exception {
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        Message msg = mock(MessageImpl.class);
        // value is null
        when(msg.getValue()).thenReturn(null);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .schema(Schema.KeyValue(Schema.INT32, Schema.STRING))
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        sink.write(record);
        sink.flush();

        // expect fail
        assertEquals(status.get(), -1);

        sink.close();
    }

    @Test
    public void wrongKeyValueSchemaTest() throws Exception {
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        sink.open(props, context);

        Message msg = mock(MessageImpl.class);
        // value is of a wrong/unsupported type
        when(msg.getValue()).thenReturn(new AbstractMap.SimpleEntry<>(11, "value"));
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(1, 0, 0));

        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName("fake-topic")
                .message(msg)
                .schema(Schema.KeyValue(Schema.INT32, Schema.STRING))
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .build();

        sink.write(record);
        sink.flush();

        // expect fail
        assertEquals(status.get(), -1);

        sink.close();
    }

    @Test
    public void offsetTest() throws Exception {
        final AtomicLong entryId = new AtomicLong(0L);
        final GenericRecord rec = getGenericRecord("value", Schema.STRING);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getMessageId()).then(x -> new MessageIdImpl(0, entryId.getAndIncrement(), 0));

        final String topicName = "testTopic";
        final int partition = 1;
        final AtomicInteger status = new AtomicInteger(0);
        Record<GenericObject> record = PulsarRecord.<String>builder()
                .topicName(topicName)
                .partition(partition)
                .message(msg)
                .ackFunction(status::incrementAndGet)
                .failFunction(status::decrementAndGet)
                .schema(Schema.STRING)
                .build();

        KafkaConnectSink sink = new KafkaConnectSink();
        when(context.getSubscriptionType()).thenReturn(SubscriptionType.Exclusive);
        sink.open(props, context);

        // offset is -1 before any data is written (aka no offset)
        assertEquals(sink.currentOffset(topicName, partition), -1L);

        sink.write(record);
        sink.flush();

        // offset is 0 for the first written record
        assertEquals(sink.currentOffset(topicName, partition), 0);

        sink.write(record);
        sink.flush();
        // offset is 1 for the second written record
        assertEquals(sink.currentOffset(topicName, partition), 1);

        sink.close();

        // close the producer, open again
        sink = new KafkaConnectSink();
        when(context.getPulsarClient()).thenReturn(PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build());
        sink.open(props, context);

        // offset is 1 after reopening the producer
        assertEquals(sink.currentOffset(topicName, partition), 1);

        sink.write(record);
        sink.flush();
        // offset is 2 for the next written record
        assertEquals(sink.currentOffset(topicName, partition), 2);

        sink.close();
    }

}
