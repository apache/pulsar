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
package org.apache.pulsar.functions.sink;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.SinkRecord;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.sink.PulsarSink.PulsarSinkProcessorBase;
import org.apache.pulsar.functions.source.TopicSchema;
import org.apache.pulsar.io.core.SinkContext;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class PulsarSinkTest {

    private static final String TOPIC = "persistent://sample/standalone/ns1/test_result";

    public static class TestSerDe implements SerDe<String> {

        @Override
        public String deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(String input) {
            return new byte[0];
        }
    }

    /**
     * Verify that JavaInstance does not support functions that take Void type as input.
     */
    private static PulsarClientImpl getPulsarClient() throws PulsarClientException {
        PulsarClientImpl pulsarClient = mock(PulsarClientImpl.class);
        ConnectionPool connectionPool = mock(ConnectionPool.class);
        when(pulsarClient.getCnxPool()).thenReturn(connectionPool);
        ConsumerBuilder consumerBuilder = mock(ConsumerBuilder.class);
        doReturn(consumerBuilder).when(consumerBuilder).topics(anyList());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionName(anyString());
        doReturn(consumerBuilder).when(consumerBuilder).subscriptionType(any());
        doReturn(consumerBuilder).when(consumerBuilder).ackTimeout(anyLong(), any());
        Consumer consumer = mock(Consumer.class);
        doReturn(consumer).when(consumerBuilder).subscribe();
        doReturn(consumerBuilder).when(pulsarClient).newConsumer(any());
        doReturn(CompletableFuture.completedFuture(Optional.empty())).when(pulsarClient).getSchema(anyString());

        ProducerBuilder producerBuilder = mock(ProducerBuilder.class);
        doReturn(producerBuilder).when(producerBuilder).blockIfQueueFull(anyBoolean());
        doReturn(producerBuilder).when(producerBuilder).enableBatching(anyBoolean());
        doReturn(producerBuilder).when(producerBuilder).batchingMaxPublishDelay(anyLong(), any());
        doReturn(producerBuilder).when(producerBuilder).compressionType(any());
        doReturn(producerBuilder).when(producerBuilder).hashingScheme(any());
        doReturn(producerBuilder).when(producerBuilder).messageRoutingMode(any());
        doReturn(producerBuilder).when(producerBuilder).messageRouter(any());
        doReturn(producerBuilder).when(producerBuilder).topic(anyString());
        doReturn(producerBuilder).when(producerBuilder).producerName(anyString());
        doReturn(producerBuilder).when(producerBuilder).property(anyString(), anyString());
        doReturn(producerBuilder).when(producerBuilder).properties(any());
        doReturn(producerBuilder).when(producerBuilder).sendTimeout(anyInt(), any());

        CompletableFuture completableFuture = new CompletableFuture<>();
        completableFuture.complete(mock(MessageId.class));
        TypedMessageBuilder typedMessageBuilder = mock(TypedMessageBuilder.class);
        doReturn(completableFuture).when(typedMessageBuilder).sendAsync();

        Producer producer = mock(Producer.class);
        doReturn(producer).when(producerBuilder).create();
        doReturn(typedMessageBuilder).when(producer).newMessage();
        doReturn(typedMessageBuilder).when(producer).newMessage(any(Schema.class));

        doReturn(producerBuilder).when(pulsarClient).newProducer();
        doReturn(producerBuilder).when(pulsarClient).newProducer(any());

        return pulsarClient;
    }

    @BeforeMethod
    public void setup() {

    }

    private static PulsarSinkConfig getPulsarConfigs() {
        PulsarSinkConfig pulsarConfig = new PulsarSinkConfig();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        pulsarConfig.setTopic(TOPIC);
        pulsarConfig.setSerdeClassName(TopicSchema.DEFAULT_SERDE);
        pulsarConfig.setTypeClassName(String.class.getName());
        return pulsarConfig;
    }

    @Getter
    @Setter
    public static class ComplexUserDefinedType {
        private String name;
        private Integer age;
    }

    public static class ComplexSerDe implements SerDe<ComplexUserDefinedType> {
        @Override
        public ComplexUserDefinedType deserialize(byte[] input) {
            return null;
        }

        @Override
        public byte[] serialize(ComplexUserDefinedType input) {
            return new byte[0];
        }
    }

    /**
     * Verify that JavaInstance does support functions that output Void type.
     */
    @Test
    public void testVoidOutputClasses() throws Exception {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(Void.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());

        try {
            Schema schema = pulsarSink.initializeSchema();
            assertNull(schema);
        } catch (Exception ex) {
            ex.printStackTrace();
            assertNull(ex);
            fail();
        }
    }

    @Test
    public void testInconsistentOutputType() throws IOException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to be inconsistent to that of SerDe
        pulsarConfig.setTypeClassName(Integer.class.getName());
        pulsarConfig.setSerdeClassName(TestSerDe.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());
        try {
            pulsarSink.initializeSchema();
            fail("Should fail constructing java instance if function type is inconsistent with serde type");
        } catch (RuntimeException ex) {
            log.error("RuntimeException: {}", ex, ex);
            assertTrue(ex.getMessage().startsWith("Inconsistent types found between function input type and serde type:"));
        } catch (Exception ex) {
            log.error("Exception: {}", ex, ex);
            fail();
        }
    }

    /**
     * Verify that Default Serializer works fine.
     */
    @Test
    public void testDefaultSerDe() throws PulsarClientException {

        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(String.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());

        try {
            pulsarSink.initializeSchema();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    /**
     * Verify that Explicit setting of Default Serializer works fine.
     */
    @Test
    public void testExplicitDefaultSerDe() throws PulsarClientException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(String.class.getName());
        pulsarConfig.setSerdeClassName(TopicSchema.DEFAULT_SERDE);
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());

        try {
            pulsarSink.initializeSchema();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testComplexOuputType() throws PulsarClientException {
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        // set type to void
        pulsarConfig.setTypeClassName(ComplexUserDefinedType.class.getName());
        pulsarConfig.setSerdeClassName(ComplexSerDe.class.getName());
        PulsarSink pulsarSink = new PulsarSink(getPulsarClient(), pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());

        try {
            pulsarSink.initializeSchema();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail();
        }
    }

    @Test
    public void testInitializeSchema() throws Exception {
        PulsarClient pulsarClient = getPulsarClient();

        // generic record type (no serde and no schema type)
        PulsarSinkConfig pulsarSinkConfig = getPulsarConfigs();
        pulsarSinkConfig.setSerdeClassName(null);
        pulsarSinkConfig.setTypeClassName(GenericRecord.class.getName());
        PulsarSink sink = new PulsarSink(
            pulsarClient, pulsarSinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());
        Schema<?> schema = sink.initializeSchema();
        assertTrue(schema instanceof AutoConsumeSchema);

        // generic record type (default serde and no schema type)
        pulsarSinkConfig = getPulsarConfigs();
        pulsarSinkConfig.setTypeClassName(GenericRecord.class.getName());
        sink = new PulsarSink(
            pulsarClient, pulsarSinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());
        schema = sink.initializeSchema();
        assertTrue(schema instanceof AutoConsumeSchema);

        // generic record type (no serde and wrong schema type)
        pulsarSinkConfig = getPulsarConfigs();
        pulsarSinkConfig.setSerdeClassName(null);
        pulsarSinkConfig.setSchemaType(SchemaType.AVRO.toString());
        pulsarSinkConfig.setTypeClassName(GenericRecord.class.getName());
        sink = new PulsarSink(
            pulsarClient, pulsarSinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());
        schema = sink.initializeSchema();
        assertTrue(schema instanceof AutoConsumeSchema);

        // generic record type (no serde and AUTO_CONSUME schema type)
        pulsarSinkConfig = getPulsarConfigs();
        pulsarSinkConfig.setSerdeClassName(null);
        pulsarSinkConfig.setSchemaType(SchemaType.AUTO_CONSUME.toString());
        pulsarSinkConfig.setTypeClassName(GenericRecord.class.getName());
        sink = new PulsarSink(
            pulsarClient, pulsarSinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());
        schema = sink.initializeSchema();
        assertTrue(schema instanceof AutoConsumeSchema);

        // generic record type (default serde and AUTO_CONSUME schema type)
        pulsarSinkConfig = getPulsarConfigs();
        pulsarSinkConfig.setSchemaType(SchemaType.AUTO_CONSUME.toString());
        pulsarSinkConfig.setTypeClassName(GenericRecord.class.getName());
        sink = new PulsarSink(
            pulsarClient, pulsarSinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());
        schema = sink.initializeSchema();
        assertTrue(schema instanceof AutoConsumeSchema);
    }

    @Test
    public void testSinkAndMessageRouting() throws Exception {

        String[] topics = {"topic-1", "topic-2", "topic-3", null};
        String defaultTopic = "default";
        PulsarSinkConfig pulsarConfig = getPulsarConfigs();
        pulsarConfig.setTopic(defaultTopic);
        PulsarClient pulsarClient;

        /** test At-least-once **/
        pulsarClient = getPulsarClient();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        PulsarSink pulsarSink = new PulsarSink(pulsarClient, pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class), Thread.currentThread().getContextClassLoader());

        pulsarSink.open(new HashMap<>(), mock(SinkContext.class));

        for (String topic : topics) {

            SinkRecord<String> record = new SinkRecord<>(new Record<String>() {
                @Override
                public Optional<String> getKey() {
                    return Optional.empty();
                }

                @Override
                public String getValue() {
                    return "in1";
                }

                @Override
                public Optional<String> getDestinationTopic() {
                    return getTopicOptional(topic);
                }
            }, "out1");


            pulsarSink.write(record);

            Assert.assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkAtLeastOnceProcessor);
            PulsarSink.PulsarSinkAtLeastOnceProcessor pulsarSinkAtLeastOnceProcessor
                    = (PulsarSink.PulsarSinkAtLeastOnceProcessor) pulsarSink.pulsarSinkProcessor;
            if (topic != null) {
                Assert.assertTrue(pulsarSinkAtLeastOnceProcessor.publishProducers.containsKey(topic));
            } else {
                Assert.assertTrue(pulsarSinkAtLeastOnceProcessor.publishProducers.containsKey(defaultTopic));
            }
            verify(pulsarClient.newProducer(), times(1)).topic(argThat(otherTopic -> {
                if (topic != null) {
                    return topic.equals(otherTopic);
                } else {
                    return defaultTopic.equals(otherTopic);
                }
            }));
        }

        /** test At-most-once **/
        pulsarClient = getPulsarClient();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATMOST_ONCE);
        pulsarSink = new PulsarSink(pulsarClient, pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class),
                Thread.currentThread().getContextClassLoader());

        pulsarSink.open(new HashMap<>(), mock(SinkContext.class));

        for (String topic : topics) {

            SinkRecord<String> record = new SinkRecord<>(new Record<String>() {
                @Override
                public Optional<String> getKey() {
                    return Optional.empty();
                }

                @Override
                public String getValue() {
                    return "in1";
                }

                @Override
                public Optional<String> getDestinationTopic() {
                    return getTopicOptional(topic);
                }
            }, "out1");


            pulsarSink.write(record);

            Assert.assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkAtMostOnceProcessor);
            PulsarSink.PulsarSinkAtMostOnceProcessor pulsarSinkAtLeastOnceProcessor
                    = (PulsarSink.PulsarSinkAtMostOnceProcessor) pulsarSink.pulsarSinkProcessor;
            if (topic != null) {
                Assert.assertTrue(pulsarSinkAtLeastOnceProcessor.publishProducers.containsKey(topic));
            } else {
                Assert.assertTrue(pulsarSinkAtLeastOnceProcessor.publishProducers.containsKey(defaultTopic));
            }
            verify(pulsarClient.newProducer(), times(1)).topic(argThat(o -> {
                return getTopicEquals(o, topic, defaultTopic);
            }));
        }

        /** test Effectively-once **/
        pulsarClient = getPulsarClient();
        pulsarConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        pulsarSink = new PulsarSink(pulsarClient, pulsarConfig, new HashMap<>(), mock(ComponentStatsManager.class),
                Thread.currentThread().getContextClassLoader());

        pulsarSink.open(new HashMap<>(), mock(SinkContext.class));

        for (String topic : topics) {

            SinkRecord<String> record = new SinkRecord<>(new Record<String>() {
                @Override
                public Optional<String> getKey() {
                    return Optional.empty();
                }

                @Override
                public String getValue() {
                    return "in1";
                }

                @Override
                public Optional<String> getDestinationTopic() {
                    return getTopicOptional(topic);
                }
                @Override
                public Optional<String> getPartitionId() {
                    if (topic != null) {
                        return Optional.of(topic + "-id-1");
                    } else {
                        return Optional.of(defaultTopic + "-id-1");
                    }
                }

                @Override
                public Optional<Integer> getPartitionIndex() {
                    return Optional.of(1);
                }

                @Override
                public Optional<Long> getRecordSequence() {
                    return Optional.of(1L);
                }
            }, "out1");


            assertEquals(1, record.getPartitionIndex().get().intValue());
            pulsarSink.write(record);

            Assert.assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkEffectivelyOnceProcessor);
            PulsarSink.PulsarSinkEffectivelyOnceProcessor pulsarSinkEffectivelyOnceProcessor
                    = (PulsarSink.PulsarSinkEffectivelyOnceProcessor) pulsarSink.pulsarSinkProcessor;
            if (topic != null) {
                Assert.assertTrue(pulsarSinkEffectivelyOnceProcessor.publishProducers.containsKey(String.format("%s-%s-id-1", topic, topic)));
            } else {
                Assert.assertTrue(pulsarSinkEffectivelyOnceProcessor.publishProducers.containsKey(String.format("%s-%s-id-1", defaultTopic, defaultTopic)));
            }

            verify(pulsarClient.newProducer(), times(1)).topic(argThat(o -> {
                return getTopicEquals(o, topic, defaultTopic);
            }));
            verify(pulsarClient.newProducer(), times(1)).producerName(argThat(o -> {
                if (topic != null) {
                    return String.format("%s-id-1", topic).equals(o);
                } else {
                    return String.format("%s-id-1", defaultTopic).equals(o);
                }
            }));
        }
    }

    @Test
    public void testWriteGenericRecordsAtMostOnce() throws Exception {
        testWriteGenericRecords(ProcessingGuarantees.ATMOST_ONCE);
    }

    @Test
    public void testWriteGenericRecordsAtLeastOnce() throws Exception {
        testWriteGenericRecords(ProcessingGuarantees.ATLEAST_ONCE);
    }

    @Test
    public void testWriteGenericRecordsEOS() throws Exception {
        testWriteGenericRecords(ProcessingGuarantees.EFFECTIVELY_ONCE);
    }

    private void testWriteGenericRecords(ProcessingGuarantees guarantees) throws Exception {
        String defaultTopic = "default";

        PulsarSinkConfig sinkConfig = getPulsarConfigs();
        sinkConfig.setTopic(defaultTopic);
        sinkConfig.setTypeClassName(GenericRecord.class.getName());
        sinkConfig.setProcessingGuarantees(guarantees);

        PulsarClient client = getPulsarClient();
        PulsarSink pulsarSink = new PulsarSink(
            client, sinkConfig, new HashMap<>(), mock(ComponentStatsManager.class),
            Thread.currentThread().getContextClassLoader());

        pulsarSink.open(new HashMap<>(), mock(SinkContext.class));

        if (ProcessingGuarantees.ATMOST_ONCE == guarantees) {
            assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkAtMostOnceProcessor);
        } else if (ProcessingGuarantees.ATLEAST_ONCE == guarantees) {
            assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkAtLeastOnceProcessor);
        } else {
            assertTrue(pulsarSink.pulsarSinkProcessor instanceof PulsarSink.PulsarSinkEffectivelyOnceProcessor);
        }
        PulsarSinkProcessorBase processor = (PulsarSinkProcessorBase) pulsarSink.pulsarSinkProcessor;
        assertFalse(processor.publishProducers.containsKey(defaultTopic));

        String[] topics = { "topic-1", "topic-2", "topic-3" };
        for (String topic : topics) {

            RecordSchemaBuilder builder = SchemaBuilder.record("MyRecord");
            builder.field("number").type(SchemaType.INT32);
            builder.field("text").type(SchemaType.STRING);
            GenericSchema<GenericRecord> schema = Schema.generic(builder.build(SchemaType.AVRO));

            GenericRecordBuilder recordBuilder = schema.newRecordBuilder();
            recordBuilder.set("number", 1);
            recordBuilder.set("text", topic);

            GenericRecord genericRecord = recordBuilder.build();

            SinkRecord<GenericRecord> record = new SinkRecord<>(new Record<GenericRecord>() {

                @Override
                public Optional<String> getDestinationTopic() {
                    return Optional.of(topic);
                }

                @Override
                public Schema<GenericRecord> getSchema() {
                    return schema;
                }

                @Override
                public GenericRecord getValue() {
                    return genericRecord;
                }

                @Override
                public Optional<String> getPartitionId() {
                    return Optional.of(topic + "-id-1");
                }

                @Override
                public Optional<Long> getRecordSequence() {
                    return Optional.of(1L);
                }
            }, genericRecord);

            pulsarSink.write(record);

            if (ProcessingGuarantees.EFFECTIVELY_ONCE == guarantees) {
                assertTrue(processor.publishProducers.containsKey(String.format("%s-%s-id-1", topic, topic)));
            } else {
                assertTrue(processor.publishProducers.containsKey(topic));
            }
            verify(client.newProducer(), times(1))
                .topic(argThat(
                    otherTopic -> topic != null ? topic.equals(otherTopic) : defaultTopic.equals(otherTopic)));

            verify(client, times(1))
                .newProducer(argThat(
                    otherSchema -> Objects.equals(otherSchema, schema)));
        }
    }

    private Optional<String> getTopicOptional(String topic) {
        if (topic != null) {
            return Optional.of(topic);
        } else {
            return Optional.empty();
        }
    }

    private boolean getTopicEquals(Object o, String topic, String defaultTopic) {
        if (topic != null) {
            return topic.equals(o);
        } else {
            return defaultTopic.equals(o);
        }
    }

}
