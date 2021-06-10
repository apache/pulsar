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
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.source.PulsarRecord;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.SinkContext;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
public class KafkaConnectSinkTest extends ProducerConsumerBase  {

    private String offsetTopicName =  "persistent://my-property/my-ns/kafka-connect-sink-offset";

    private Path file;
    private Map<String, Object> props;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        file = Paths.get(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        props = Maps.newHashMap();
        props.put("topic", "test-topic");
        props.put("pulsarServiceUrl", brokerUrl.toString());
        props.put("offsetStorageTopic", offsetTopicName);
        props.put("kafkaConnectorSinkClass", "org.apache.kafka.connect.file.FileStreamSinkConnector");

        Map<String, String> kafkaConnectorProps = Maps.newHashMap();
        kafkaConnectorProps.put("file", file.toString());
        props.put("kafkaConnectorConfigProperties", kafkaConnectorProps);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (file != null && Files.exists(file)) {
            Files.delete(file);
        }

        super.internalCleanup();
    }

    @Test
    public void smokeTest() throws Exception {
        KafkaConnectSink sink = new KafkaConnectSink();
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        sink.open(props, mockCtx);

        final GenericRecord rec = getGenericRecord("value", Schema.STRING);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(0, 0, 0));

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

        assertEquals(1, status.get());

        sink.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        assertEquals("value", lines.get(0));
    }

    @Test
    public void seekPauseResumeTest() throws Exception {
        KafkaConnectSink sink = new KafkaConnectSink();
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        sink.open(props, mockCtx);

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

        assertEquals(1, status.get());

        final TopicPartition tp = new TopicPartition("fake-topic", 0);
        assertNotEquals(0, MessageIdUtils.getOffset(msgId));
        assertEquals(MessageIdUtils.getOffset(msgId), sink.currentOffset(tp.topic(), tp.partition()));

        sink.taskContext.offset(tp, 0);
        verify(mockCtx, times(1)).seek(Mockito.anyString(), Mockito.anyInt(), any());
        assertEquals(0, sink.currentOffset(tp.topic(), tp.partition()));

        sink.taskContext.pause(tp);
        verify(mockCtx, times(1)).pause(tp.topic(), tp.partition());
        sink.taskContext.resume(tp);
        verify(mockCtx, times(1)).resume(tp.topic(), tp.partition());

        sink.close();
    }


    @Test
    public void subscriptionTypeTest() throws Exception {
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Exclusive);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Exclusive is allowed");
            sink.open(props, mockCtx);
        }

        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Failover is allowed");
            sink.open(props, mockCtx);
        }

        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Key_Shared);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Key_Shared is not allowed");
            sink.open(props, mockCtx);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Shared);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Shared is not allowed");
            sink.open(props, mockCtx);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

        when(mockCtx.getSubscriptionType()).thenReturn(null);
        try (KafkaConnectSink sink = new KafkaConnectSink()) {
            log.info("Type is required");
            sink.open(props, mockCtx);
            fail("expected exception");
        } catch (IllegalArgumentException iae) {
            // pass
        }

    }

    private void recordSchemaTest(Object value, Schema schema, Object expected, String expectedSchema) throws Exception {
        recordSchemaTest(value, schema, "key",  "STRING", expected, expectedSchema);
    }

    private void recordSchemaTest(Object value, Schema schema, Object expectedKey, String expectedKeySchema,
                                  Object expected, String expectedSchema) throws Exception {
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        sink.open(props, mockCtx);

        final GenericRecord rec = getGenericRecord(value, schema);
        Message msg = mock(MessageImpl.class);
        when(msg.getValue()).thenReturn(rec);
        when(msg.getKey()).thenReturn("key");
        when(msg.hasKey()).thenReturn(true);
        when(msg.getMessageId()).thenReturn(new MessageIdImpl(0, 0, 0));

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

        assertEquals(1, status.get());

        sink.close();

        List<String> lines = Files.readAllLines(file, StandardCharsets.US_ASCII);
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> result = om.readValue(lines.get(0), new TypeReference<Map<String, Object>>(){});

        assertEquals(expectedKey, result.get("key"));
        assertEquals(expected, result.get("value"));
        assertEquals(expectedKeySchema, result.get("keySchema"));
        assertEquals(expectedSchema, result.get("valueSchema"));
    }

    private GenericRecord getGenericRecord(Object value, Schema schema) {
        final GenericRecord rec;
        if(value instanceof GenericRecord) {
            rec = (GenericRecord) value;
        } else {
            rec = MockGenericObjectWrapper.builder()
                    .nativeObject(value)
                    .schemaType(schema != null ? schema.getSchemaInfo().getType() : null)
                    .schemaVersion(new byte[]{ 1 }).build();
        }
        return rec;
    }

    @Test
    public void bytesRecordSchemaTest() throws Exception {
        recordSchemaTest("val".getBytes(StandardCharsets.US_ASCII), Schema.BYTES, "val", "BYTES");
    }

    @Test
    public void stringRecordSchemaTest() throws Exception {
        recordSchemaTest("val", Schema.STRING, "val", "STRING");
    }

    @Test
    public void booleanRecordSchemaTest() throws Exception {
        recordSchemaTest(true, Schema.BOOL, true, "BOOLEAN");
    }

    @Test
    public void byteRecordSchemaTest() throws Exception {
        // int 1 is coming back from ObjectMapper
        recordSchemaTest((byte)1, Schema.INT8, 1, "INT8");
    }

    @Test
    public void shortRecordSchemaTest() throws Exception {
        // int 1 is coming back from ObjectMapper
        recordSchemaTest((short)1, Schema.INT16, 1, "INT16");
    }

    @Test
    public void integerRecordSchemaTest() throws Exception {
        recordSchemaTest(Integer.MAX_VALUE, Schema.INT32, Integer.MAX_VALUE, "INT32");
    }

    @Test
    public void longRecordSchemaTest() throws Exception {
        recordSchemaTest(Long.MAX_VALUE, Schema.INT64, Long.MAX_VALUE, "INT64");
    }

    @Test
    public void floatRecordSchemaTest() throws Exception {
        // 1.0d is coming back from ObjectMapper
        recordSchemaTest(1.0f, Schema.FLOAT, 1.0d, "FLOAT32");
    }

    @Test
    public void doubleRecordSchemaTest() throws Exception {
        recordSchemaTest(Double.MAX_VALUE, Schema.DOUBLE, Double.MAX_VALUE, "FLOAT64");
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

        Map<String, Object> expected = new LinkedHashMap<>();
        expected.put("field1", 10);
        expected.put("field2", "test");
        // integer is coming back from ObjectMapper
        expected.put("field3", 100);

        recordSchemaTest(obj, jsonSchema, expected, "STRUCT");
    }

    @Test
    public void unknownRecordSchemaTest() throws Exception {
        Object obj = new Object();
        props.put("kafkaConnectorSinkClass", SchemaedFileStreamSinkConnector.class.getCanonicalName());

        KafkaConnectSink sink = new KafkaConnectSink();
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Failover);
        sink.open(props, mockCtx);

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

        assertEquals("write should fail for unsupported schema",-1, status.get());

        sink.close();
    }

    @Test
    public void KeyValueSchemaTest() throws Exception {
        KeyValue<Integer, String> kv = new KeyValue<>(11, "value");
        recordSchemaTest(kv, Schema.KeyValue(Schema.INT32, Schema.STRING), 11, "INT32", "value", "STRING");
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
        SinkContext mockCtx = Mockito.mock(SinkContext.class);
        when(mockCtx.getSubscriptionType()).thenReturn(SubscriptionType.Exclusive);
        sink.open(props, mockCtx);

        // offset is -1 before any data is written (aka no offset)
        assertEquals(-1L, sink.currentOffset(topicName, partition));

        sink.write(record);
        sink.flush();

        // offset is 0 for the first written record
        assertEquals(0, sink.currentOffset(topicName, partition));

        sink.write(record);
        sink.flush();
        // offset is 1 for the second written record
        assertEquals(1, sink.currentOffset(topicName, partition));

        sink.close();

        // close the producer, open again
        sink = new KafkaConnectSink();
        sink.open(props, mockCtx);

        // offset is 1 after reopening the producer
        assertEquals(1, sink.currentOffset(topicName, partition));

        sink.write(record);
        sink.flush();
        // offset is 2 for the next written record
        assertEquals(2, sink.currentOffset(topicName, partition));

        sink.close();
    }

}
