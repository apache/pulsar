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
package org.apache.pulsar.client.api;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema.Parser;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidMessageException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.HttpLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.reader.AvroReader;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-api")
@Slf4j
public class SimpleSchemaTest extends ProducerConsumerBase {

    private static final String NAMESPACE = "my-property/my-ns";

    @DataProvider(name = "batchingModes")
    public static Object[][] batchingModes() {
        return new Object[][] {
                { true },
                { false }
        };
    }

    @DataProvider(name = "batchingModesAndValueEncodingType")
    public static Object[][] batchingModesAndValueEncodingType() {
        return new Object[][] {
                { true, KeyValueEncodingType.INLINE },
                { true, KeyValueEncodingType.SEPARATED },
                { false, KeyValueEncodingType.INLINE },
                { false, KeyValueEncodingType.SEPARATED }
        };
    }


    @DataProvider(name = "topicDomain")
    public static Object[] topicDomain() {
        return new Object[] { "persistent://", "non-persistent://" };
    }

    private final boolean schemaValidationEnforced;

    public SimpleSchemaTest() {
        this(false);
    }

    protected SimpleSchemaTest(boolean schemaValidationEnforced) {
        this.schemaValidationEnforced = schemaValidationEnforced;
    }


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setSchemaValidationEnforced(schemaValidationEnforced);
        this.isTcpLookup = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void resetNamespace() throws Exception {
        List<String> list = admin.namespaces().getTopics(NAMESPACE);
        for (String topicName : list){
            if (!pulsar.getBrokerService().isSystemTopic(topicName)) {
                admin.topics().delete(topicName, false);
            }
        }
        PulsarClientImpl pulsarClientImpl = (PulsarClientImpl) pulsarClient;
        pulsarClientImpl.getSchemaProviderLoadingCache().invalidateAll();
    }

    @Test
    public void testString() throws Exception {
        final String topicName = String.format("persistent://%s/my-topic1", NAMESPACE);
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionName("my-subscriber-name").subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                     .topic(topicName).create()) {
            int N = 10;

            for (int i = 0; i < N; i++) {
                producer.send("my-message-" + i);
            }

            for (int i = 0; i < N; i++) {
                Message<String> msg = consumer.receive();
                assertEquals(msg.getValue(), "my-message-" + i);

                consumer.acknowledge(msg);
            }
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V1Data {
        int i;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class V2Data {
        int i;
        Integer j;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    static class IncompatibleData {
        int i;
        int j;
    }

    @Test
    public void newProducerNewTopicNewSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerTopicExistsWithoutSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topic).create()) {
            p.send(topic.getBytes(UTF_8));
        }

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerTopicExistsWithSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(1));
        }

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerWithoutSchemaOnTopicWithSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }

        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.BYTES).topic(topic).create()) {
            if (!schemaValidationEnforced) {
                p.send("junkdata".getBytes(UTF_8));
            } else {
                Assert.fail("Shouldn't be able to connect to a schema'd topic with no schema"
                        + " if SchemaValidationEnabled is enabled");
            }
        } catch (PulsarClientException e) {
            if (schemaValidationEnforced) {
                Assert.assertTrue(e instanceof IncompatibleSchemaException);
            } else {
                Assert.fail("Shouldn't throw IncompatibleSchemaException"
                        + " if SchemaValidationEnforced is disabled");
            }
        }

        // if using AUTO_PRODUCE_BYTES, producer can connect but the publish will fail
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topic).create()) {
            p.send("junkdata".getBytes(UTF_8));
        } catch (PulsarClientException e) {
            assertTrue(e.getCause() instanceof SchemaSerializationException);
        }

        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v1SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v1SchemaBytes));
        // if using NATIVE_AVRO, producer can connect but the publish will fail
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.NATIVE_AVRO(v1SchemaAvroNative)).topic(topic).create()) {
            p.send("junkdata".getBytes(UTF_8));
        } catch (PulsarClientException e) {
            assertTrue(e.getCause() instanceof SchemaSerializationException);
        }
   
    }

    @Test
    public void newProducerForMessageSchemaOnTopicWithMultiVersionSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        AvroWriter<V1Data> v1Writer = new AvroWriter<>(
                new Parser().parse(new ByteArrayInputStream(v1SchemaBytes)));
        Schema<V2Data> v2Schema = Schema.AVRO(V2Data.class);
        byte[] v2SchemaBytes = v2Schema.getSchemaInfo().getSchema();
        AvroWriter<V2Data> v2Writer = new AvroWriter<>(
                new Parser().parse(new ByteArrayInputStream(v2SchemaBytes)));
        try (Producer<V1Data> ignored = pulsarClient.newProducer(v1Schema)
                .topic(topic).create()) {
        }
        try (Producer<V2Data> p = pulsarClient.newProducer(Schema.AVRO(V2Data.class))
                .topic(topic).create()) {
            p.send(new V2Data(-1, -1));
        }
        V1Data dataV1 = new V1Data(2);
        V2Data dataV2 = new V2Data(3, 5);
        byte[] contentV1 = v1Writer.write(dataV1);
        byte[] contentV2 = v2Writer.write(dataV2);
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(topic).create();
             Consumer<V2Data> c = pulsarClient.newConsumer(v2Schema)
                     .topic(topic)
                     .subscriptionName("sub1").subscribe()) {
            Assert.expectThrows(SchemaSerializationException.class, () -> p.send(contentV1));

            p.newMessage(Schema.AUTO_PRODUCE_BYTES(Schema.AVRO(V1Data.class)))
                    .value(contentV1).send();
            p.send(contentV2);
            Message<V2Data> msg1 = c.receive();
            V2Data msg1Value = msg1.getValue();
            Assert.assertEquals(dataV1.i, msg1Value.i);
            assertNull(msg1Value.j);
            Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());

            Message<V2Data> msg2 = c.receive();
            Assert.assertEquals(dataV2, msg2.getValue());
            Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());

            try {
                p.newMessage(Schema.BYTES).value(contentV1).send();
                if (schemaValidationEnforced) {
                    Assert.fail("Shouldn't be able to send to a schema'd topic with no schema"
                            + " if SchemaValidationEnabled is enabled");
                }
                Message<V2Data> msg3 = c.receive();
                assertNull(msg3.getSchemaVersion());
                try {
                    msg3.getValue();
                    fail("Schema should be incompatible");
                } catch (SchemaSerializationException e) {
                    assertTrue(e.getCause() instanceof EOFException);
                }
            } catch (PulsarClientException e) {
                if (schemaValidationEnforced) {
                    Assert.assertTrue(e instanceof IncompatibleSchemaException);
                } else {
                    Assert.fail("Shouldn't throw IncompatibleSchemaException"
                            + " if SchemaValidationEnforced is disabled");
                }
            }
        }
    }

    @Test
    public void newNativeAvroProducerForMessageSchemaOnTopicWithMultiVersionSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v1SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v1SchemaBytes));
        AvroWriter<V1Data> v1Writer = new AvroWriter<>(v1SchemaAvroNative);
        Schema<V2Data> v2Schema = Schema.AVRO(V2Data.class);
        byte[] v2SchemaBytes = v2Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v2SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v2SchemaBytes));
        AvroWriter<V2Data> v2Writer = new AvroWriter<>(v2SchemaAvroNative);
        V1Data dataV1 = new V1Data(2);
        V2Data dataV2 = new V2Data(3, 5);
        byte[] contentV1 = v1Writer.write(dataV1);
        byte[] contentV2 = v2Writer.write(dataV2);
        try (Producer<byte[]> ignored = pulsarClient.newProducer(Schema.NATIVE_AVRO(v1SchemaAvroNative))
                .topic(topic).create()) {
        }
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.NATIVE_AVRO(v2SchemaAvroNative))
                .topic(topic).create()) {
            p.send(contentV2);
        }
        try (Producer<byte[]> p = pulsarClient.newProducer(Schema.NATIVE_AVRO(v1SchemaAvroNative))
                .topic(topic).create();
             Consumer<V2Data> c = pulsarClient.newConsumer(v2Schema)
                     .topic(topic)
                     .subscriptionName("sub1").subscribe()) {

            p.newMessage(Schema.NATIVE_AVRO(v1SchemaAvroNative))
                    .value(contentV1).send();
            p.newMessage(Schema.NATIVE_AVRO(v2SchemaAvroNative))
                    .value(contentV2).send();
            Message<V2Data> msg1 = c.receive();
            V2Data msg1Value = msg1.getValue();
            Assert.assertEquals(dataV1.i, msg1Value.i);
            assertNull(msg1Value.j);
            Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());

            Message<V2Data> msg2 = c.receive();
            Assert.assertEquals(dataV2, msg2.getValue());
            Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());

            try {
                p.newMessage(Schema.BYTES).value(contentV1).send();
                if (schemaValidationEnforced) {
                    Assert.fail("Shouldn't be able to send to a schema'd topic with no schema"
                            + " if SchemaValidationEnabled is enabled");
                }
                Message<V2Data> msg3 = c.receive();
                assertNull(msg3.getSchemaVersion());
                try {
                    msg3.getValue();
                    fail("Schema should be incompatible");
                } catch (SchemaSerializationException e) {
                    assertTrue(e.getCause() instanceof EOFException);
                }
            } catch (PulsarClientException e) {
                if (schemaValidationEnforced) {
                    Assert.assertTrue(e instanceof IncompatibleSchemaException);
                } else {
                    Assert.fail("Shouldn't throw IncompatibleSchemaException"
                            + " if SchemaValidationEnforced is disabled");
                }
            }
        }
    }

    @Test
    public void newProducerForMessageOnTopicWithDifferentSchemaType() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        V1Data data1 = new V1Data(2);
        V2Data data2 = new V2Data(3, 5);
        V1Data data3 = new V1Data(8);
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create();
             Consumer<V2Data> c = pulsarClient.newConsumer(Schema.AVRO(V2Data.class))
                     .topic(topic)
                     .subscriptionName("sub1").subscribe()) {
            p.newMessage().value(data1).send();
            p.newMessage(Schema.AVRO(V2Data.class)).value(data2).send();
            p.newMessage(Schema.AVRO(V1Data.class)).value(data3).send();
            Message<V2Data> msg1 = c.receive();
            V2Data msg1Value = msg1.getValue();
            Assert.assertEquals(data1.i, msg1Value.i);
            assertNull(msg1Value.j);
            Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());

            Message<V2Data> msg2 = c.receive();
            Assert.assertEquals(data2, msg2.getValue());
            Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());

            Message<V2Data> msg3 = c.receive();
            V2Data msg3Value = msg3.getValue();
            Assert.assertEquals(data3.i, msg3Value.i);
            assertNull(msg3Value.j);
            Assert.assertEquals(msg3.getSchemaVersion(), new LongSchemaVersion(0).bytes());
        }
    }

    @Test
    public void newProducerForMessageSchemaOnTopicInitialWithNoSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        AvroWriter<V1Data> v1Writer = new AvroWriter<>(
                new Parser().parse(new ByteArrayInputStream(v1SchemaBytes)));
        Schema<V2Data> v2Schema = Schema.AVRO(V2Data.class);
        byte[] v2SchemaBytes = v2Schema.getSchemaInfo().getSchema();
        AvroWriter<V2Data> v2Writer = new AvroWriter<>(
                new Parser().parse(new ByteArrayInputStream(v2SchemaBytes)));
        try (Producer<byte[]> p = pulsarClient.newProducer()
                .topic(topic).create();
             Consumer<byte[]> c = pulsarClient.newConsumer()
                     .topic(topic)
                     .subscriptionName("sub1").subscribe()) {
            for (int i = 0; i < 2; ++i) {
                V1Data dataV1 = new V1Data(i);
                V2Data dataV2 = new V2Data(i, -i);
                byte[] contentV1 = v1Writer.write(dataV1);
                byte[] contentV2 = v2Writer.write(dataV2);
                p.newMessage(Schema.AUTO_PRODUCE_BYTES(v1Schema)).value(contentV1).send();
                Message<byte[]> msg1 = c.receive();
                Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());
                Assert.assertEquals(msg1.getData(), contentV1);
                p.newMessage(Schema.AUTO_PRODUCE_BYTES(v2Schema)).value(contentV2).send();
                Message<byte[]> msg2 = c.receive();
                Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());
                Assert.assertEquals(msg2.getData(), contentV2);
            }
        }

        List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topic);
        allSchemas.forEach(schemaInfo -> {
            ((SchemaInfoImpl)schemaInfo).setTimestamp(0);
        });
        Assert.assertEquals(allSchemas, Arrays.asList(v1Schema.getSchemaInfo(),
                v2Schema.getSchemaInfo()));
    }

    @Test
    public void newNativeAvroProducerForMessageSchemaOnTopicInitialWithNoSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v1SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v1SchemaBytes));
        AvroWriter<V1Data> v1Writer = new AvroWriter<>(v1SchemaAvroNative);
        Schema<V2Data> v2Schema = Schema.AVRO(V2Data.class);
        byte[] v2SchemaBytes = v2Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v2SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v2SchemaBytes));
        AvroWriter<V2Data> v2Writer = new AvroWriter<>(v2SchemaAvroNative);

        try (Producer<byte[]> p = pulsarClient.newProducer()
                .topic(topic).create();
             Consumer<byte[]> c = pulsarClient.newConsumer()
                     .topic(topic)
                     .subscriptionName("sub1").subscribe()) {
            for (int i = 0; i < 2; ++i) {
                V1Data dataV1 = new V1Data(i);
                V2Data dataV2 = new V2Data(i, -i);
                byte[] contentV1 = v1Writer.write(dataV1);
                byte[] contentV2 = v2Writer.write(dataV2);
                p.newMessage(Schema.NATIVE_AVRO(v1SchemaAvroNative)).value(contentV1).send();
                Message<byte[]> msg1 = c.receive();
                Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());
                Assert.assertEquals(msg1.getData(), contentV1);
                p.newMessage(Schema.NATIVE_AVRO(v2SchemaAvroNative)).value(contentV2).send();
                Message<byte[]> msg2 = c.receive();
                Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());
                Assert.assertEquals(msg2.getData(), contentV2);
            }
        }

        List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topic);
        allSchemas.forEach(schemaInfo -> {
            ((SchemaInfoImpl)schemaInfo).setTimestamp(0);
        });
        Assert.assertEquals(allSchemas, Arrays.asList(v1Schema.getSchemaInfo(),
                v2Schema.getSchemaInfo()));
    }

    @Test
    public void newProducerForMessageSchemaWithBatch() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        @Cleanup
        Consumer<V2Data> c = pulsarClient.newConsumer(Schema.AVRO(V2Data.class))
                .topic(topic)
                .subscriptionName("sub1").subscribe();
        @Cleanup
        Producer<byte[]> p = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS).create();
        AvroWriter<V1Data> v1DataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(V1Data.class));
        AvroWriter<V2Data> v2DataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(V2Data.class));
        AvroWriter<IncompatibleData> incompatibleDataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(IncompatibleData.class));
        int total = 20;
        int batch = 5;
        int incompatible = 3;
        for (int i = 0; i < total; ++i) {
            if (i / batch % 2 == 0) {
                byte[] content = v1DataAvroWriter.write(new V1Data(i));
                p.newMessage(Schema.AUTO_PRODUCE_BYTES(Schema.AVRO(V1Data.class)))
                        .value(content).sendAsync();
            } else {
                byte[] content = v2DataAvroWriter.write(new V2Data(i, i + total));
                p.newMessage(Schema.AUTO_PRODUCE_BYTES(Schema.AVRO(V2Data.class)))
                        .value(content).sendAsync();
            }
            if ((i + 1) % incompatible == 0) {
                byte[] content = incompatibleDataAvroWriter.write(new IncompatibleData(-i, -i));
                try {
                    p.newMessage(Schema.AUTO_PRODUCE_BYTES(Schema.AVRO(IncompatibleData.class)))
                            .value(content).send();
                    fail("Expect: incompatible schema exception");
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof IncompatibleSchemaException, e.getMessage());
                }
            }
        }
        p.flush();
        for (int i = 0; i < total; ++i) {
            V2Data value = c.receive().getValue();
            if (i / batch % 2 == 0) {
                assertNull(value.j);
                Assert.assertEquals(value.i, i);
            } else {
                Assert.assertEquals(value, new V2Data(i, i + total));
            }
        }
        c.close();
    }


    @Test
    public void newNativeAvroProducerForMessageSchemaWithBatch() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        Schema<V1Data> v1Schema = Schema.AVRO(V1Data.class);
        byte[] v1SchemaBytes = v1Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v1SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v1SchemaBytes));
        AvroWriter<V1Data> v1Writer = new AvroWriter<>(v1SchemaAvroNative);
        Schema<V2Data> v2Schema = Schema.AVRO(V2Data.class);
        byte[] v2SchemaBytes = v2Schema.getSchemaInfo().getSchema();
        org.apache.avro.Schema v2SchemaAvroNative = new Parser().parse(new ByteArrayInputStream(v2SchemaBytes));
        AvroWriter<V2Data> v2Writer = new AvroWriter<>(v2SchemaAvroNative);

        @Cleanup
        Consumer<byte[]> c = pulsarClient.newConsumer(Schema.BYTES)
                .topic(topic)
                .subscriptionName("sub1").subscribe();
        @Cleanup
        Producer<byte[]> p = pulsarClient.newProducer(Schema.NATIVE_AVRO(v1SchemaAvroNative))
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(10, TimeUnit.SECONDS).create();
        AvroWriter<V1Data> v1DataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(V1Data.class));
        AvroWriter<V2Data> v2DataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(V2Data.class));
        AvroWriter<IncompatibleData> incompatibleDataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(IncompatibleData.class));
        int total = 20;
        int batch = 5;
        int incompatible = 3;
        for (int i = 0; i < total; ++i) {
            if (i / batch % 2 == 0) {
                byte[] content = v1DataAvroWriter.write(new V1Data(i));
                p.newMessage(Schema.NATIVE_AVRO(v1SchemaAvroNative))
                        .value(content).sendAsync();
            } else {
                byte[] content = v2DataAvroWriter.write(new V2Data(i, i + total));
                p.newMessage(Schema.NATIVE_AVRO(v2SchemaAvroNative))
                        .value(content).sendAsync();
            }
            if ((i + 1) % incompatible == 0) {
                Schema<IncompatibleData> incompatibleSchema = Schema.AVRO(IncompatibleData.class);
                byte[] incompatibleSchemaBytes = incompatibleSchema.getSchemaInfo().getSchema();
                org.apache.avro.Schema incompatibleSchemaAvroNative = new Parser().parse(new ByteArrayInputStream(incompatibleSchemaBytes));
                byte[] content = incompatibleDataAvroWriter.write(new IncompatibleData(-i, -i));
                try {
                    p.newMessage(Schema.NATIVE_AVRO(incompatibleSchemaAvroNative))
                            .value(content).send();
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof IncompatibleSchemaException, e.getMessage());
                }
            }
        }
        p.flush();
    
        for (int i = 0; i < total; ++i) {
            byte[] raw = c.receive().getData();
            if (i / batch % 2 == 0) {
                AvroReader<V1Data> reader = new AvroReader<>(v1SchemaAvroNative);
                V1Data value = reader.read(raw);
                Assert.assertEquals(value.i, i);
            } else {
                AvroReader<V2Data> reader = new AvroReader<>(v2SchemaAvroNative);
                V2Data value = reader.read(raw);
                Assert.assertEquals(value, new V2Data(i, i + total));
            }
        }
        c.close();
    }

    @Test
    public void newProducerWithMultipleSchemaDisabled() throws Exception {
        String topic = NAMESPACE + "/schema-test";
        AvroWriter<V1Data> v1DataAvroWriter = new AvroWriter<>(
                ReflectData.AllowNull.get().getSchema(V1Data.class));
        try (Producer<byte[]> p = pulsarClient.newProducer()
                .topic(topic)
                .enableMultiSchema(false).create()) {
            Assert.assertThrows(InvalidMessageException.class,
                    () -> p.newMessage(Schema.AUTO_PRODUCE_BYTES(Schema.AVRO(V1Data.class)))
                            .value(v1DataAvroWriter.write(new V1Data(0))).send());
        }
    }

    @Test
    public void newConsumerWithSchemaOnNewTopic() throws Exception {
        String topic = NAMESPACE + "/schema-test";

        try (Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                .topic(topic).subscriptionName("sub1").subscribe();
             Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create()) {
            V1Data toSend = new V1Data(1);
            p.send(toSend);
            Assert.assertEquals(toSend, c.receive().getValue());
        }
    }

    @Test
    public void newConsumerWithSchemaOnExistingTopicWithoutSchema() {
        String topic = NAMESPACE + "/schema-test";

        try (Producer<byte[]> p = pulsarClient.newProducer().topic(topic).create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                     .topic(topic).subscriptionName("sub1").subscribe()) {
            Assert.fail("Shouldn't be able to consume with a schema from a topic which has no schema set");
        } catch (PulsarClientException e) {
            Assert.assertTrue(e instanceof IncompatibleSchemaException);
        }
    }

    @Test
    public void newConsumerWithSchemaTopicHasSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                     .topic(topic).subscriptionName("sub1").subscribe()) {
            V1Data toSend = new V1Data(1);
            p.send(toSend);
            Assert.assertEquals(toSend, c.receive().getValue());
        }
    }

    @Test
    public void newBytesConsumerWithTopicWithSchema() throws Exception {
        String topic = NAMESPACE + "/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create();
             Consumer<byte[]> c = pulsarClient.newConsumer().topic(topic).subscriptionName("sub1").subscribe()) {
            p.send(new V1Data(1));
            Assert.assertTrue(c.receive().getValue().length > 0);
        }
    }

    @Test
    public void getSchemaVersionFromMessagesBatchingDisabled() throws Exception {
        getSchemaVersionFromMessages(false);
    }

    @Test
    public void getSchemaVersionFromMessagesBatchingEnabled() throws Exception {
        getSchemaVersionFromMessages(true);
    }

    private void getSchemaVersionFromMessages(boolean batching) throws Exception {
        String topic = NAMESPACE + "/schema-test";

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic)
                .enableBatching(batching)
                .create();
             Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                     .topic(topic)
                     .subscriptionName("sub1")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .subscribe()) {

            p.send(new V1Data(1));
            Message<V1Data> data = c.receive();
            assertNotNull(data.getSchemaVersion());
            assertEquals(data.getValue(), new V1Data(1));
        }
    }

    @Test(dataProvider = "batchingModes")
    public void testAutoConsume(boolean batching) throws Exception {
        String topic = NAMESPACE + "/schema-test-auto-consume-" + batching;

        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic)
                .enableBatching(batching)
                .create();
             Consumer<GenericRecord> c = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                     .topic(topic)
                     .subscriptionName("sub1")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .subscribe()) {

            int numMessages = 10;

            for (int i = 0; i < numMessages; i++) {
                p.sendAsync(new V1Data(i));
            }
            p.flush();

            for (int i = 0; i < numMessages; i++) {
                Message<GenericRecord> data = c.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getField("i"), i);
                MessageImpl impl = (MessageImpl) data;

                org.apache.avro.Schema avroSchema = (org.apache.avro.Schema) impl.getSchemaInternal().getNativeSchema().get();
                assertNotNull(avroSchema);

                org.apache.avro.Schema avroSchema2 = (org.apache.avro.Schema) data.getReaderSchema().get().getNativeSchema().get();
                assertNotNull(avroSchema2);
            }

        }
    }

    @Test(dataProvider = "batchingModesAndValueEncodingType")
    public void testAutoKeyValueConsume(boolean batching, KeyValueEncodingType keyValueEncodingType) throws Exception {
        String topic = NAMESPACE + "/schema-test-auto-keyvalue-consume-" + batching+"-"+keyValueEncodingType;

        Schema<KeyValue<V1Data, V1Data>> pojoSchema = Schema.KeyValue(
                Schema.AVRO(V1Data.class),
                Schema.AVRO(V1Data.class),
                keyValueEncodingType);

        try (Consumer<KeyValue<GenericRecord, V1Data>> c3before = pulsarClient.newConsumer(
                // this consumer is the same as 'c3' Consumer below, but it subscribes to the
                // topic before that the Producer writes messages and set the Schema
                // so the Consumer starts on a non existing topic (that will be autocreated)
                // without a schema
                // in fact a KeyValue schema with a AutoConsumeSchema component
                // is to be treated like an AutoConsumeSchema because it downloads
                // automatically the schema when needed
                Schema.KeyValue(
                        Schema.AUTO_CONSUME(),
                        Schema.AVRO(V1Data.class),
                        keyValueEncodingType))
                .topic(topic)
                .subscriptionName("sub3b")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();) {

            List<SchemaInfo> allSchemas = getPulsar().getAdminClient().schemas().getAllSchemas(topic);
            // verify that the Consumer did not set a schema on the topic
            assertTrue(allSchemas.isEmpty());

            try (Producer<KeyValue<V1Data, V1Data>> p = pulsarClient.newProducer(pojoSchema)
                    .topic(topic)
                    .enableBatching(batching)
                    .create();
                 Consumer<GenericRecord> c0 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                         .topic(topic)
                         .subscriptionName("sub0")
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe();
                 Consumer<KeyValue<GenericRecord, GenericRecord>> c1 = pulsarClient.newConsumer(
                         Schema.KeyValue(
                                 Schema.AUTO_CONSUME(),
                                 Schema.AUTO_CONSUME(),
                                 keyValueEncodingType))
                         .topic(topic)
                         .subscriptionName("sub1")
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe();
                 Consumer<KeyValue<V1Data, V1Data>> c2 = pulsarClient.newConsumer(
                         Schema.KeyValue(
                                 Schema.AVRO(V1Data.class),
                                 Schema.AVRO(V1Data.class),
                                 keyValueEncodingType))
                         .topic(topic)
                         .subscriptionName("sub2")
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe();
                 Consumer<KeyValue<GenericRecord, V1Data>> c3 = pulsarClient.newConsumer(
                         Schema.KeyValue(
                                 Schema.AUTO_CONSUME(),
                                 Schema.AVRO(V1Data.class),
                                 keyValueEncodingType))
                         .topic(topic)
                         .subscriptionName("sub3")
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe();
                 Consumer<KeyValue<V1Data, GenericRecord>> c4 = pulsarClient.newConsumer(
                         Schema.KeyValue(
                                 Schema.AVRO(V1Data.class),
                                 Schema.AUTO_CONSUME(),
                                 keyValueEncodingType))
                         .topic(topic)
                         .subscriptionName("sub4")
                         .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                         .subscribe()
            ) {

                int numMessages = 10;

                for (int i = 0; i < numMessages; i++) {
                    p.sendAsync(new KeyValue<>(new V1Data(i * 100), new V1Data(i * 1000)));
                }
                p.flush();

                // verify c0
                for (int i = 0; i < numMessages; i++) {
                    Message<GenericRecord> wrapper = c0.receive();
                    KeyValue<GenericRecord, GenericRecord> data = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
                    assertNotNull(wrapper.getSchemaVersion());
                    assertEquals(data.getKey().getField("i"), i * 100);
                    assertEquals(data.getValue().getField("i"), i * 1000);
                    c0.acknowledge(wrapper);
                    Schema<?> schema = wrapper.getReaderSchema().get();
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) schema;
                    assertEquals(SchemaType.AVRO, keyValueSchema.getKeySchema().getSchemaInfo().getType());
                    assertEquals(SchemaType.AVRO, keyValueSchema.getValueSchema().getSchemaInfo().getType());
                    assertNotNull(schema.getSchemaInfo());
                }
                // verify c1
                for (int i = 0; i < numMessages; i++) {
                    Message<KeyValue<GenericRecord, GenericRecord>> data = c1.receive();
                    assertNotNull(data.getSchemaVersion());
                    assertEquals(data.getValue().getKey().getField("i"), i * 100);
                    assertEquals(data.getValue().getValue().getField("i"), i * 1000);
                    c1.acknowledge(data);
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) data.getReaderSchema().get();
                    assertNotNull(keyValueSchema.getKeySchema());
                    assertNotNull(keyValueSchema.getValueSchema());
                }

                // verify c2
                for (int i = 0; i < numMessages; i++) {
                    Message<KeyValue<V1Data, V1Data>> data = c2.receive();
                    assertNotNull(data.getSchemaVersion());
                    assertEquals(data.getValue().getKey().i, i * 100);
                    assertEquals(data.getValue().getValue().i, i * 1000);
                    c2.acknowledge(data);
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) data.getReaderSchema().get();
                    assertNotNull(keyValueSchema.getKeySchema());
                    assertNotNull(keyValueSchema.getValueSchema());
                }

                // verify c3
                for (int i = 0; i < numMessages; i++) {
                    Message<KeyValue<GenericRecord, V1Data>> data = c3.receive();
                    assertNotNull(data.getSchemaVersion());
                    assertEquals(data.getValue().getKey().getField("i"), i * 100);
                    assertEquals(data.getValue().getValue().i, i * 1000);
                    c3.acknowledge(data);
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) data.getReaderSchema().get();
                    assertNotNull(keyValueSchema.getKeySchema());
                    assertNotNull(keyValueSchema.getValueSchema());
                }

                // verify c3before
                for (int i = 0; i < numMessages; i++) {
                    Message<KeyValue<GenericRecord, V1Data>> data = c3before.receive();
                    assertNotNull(data.getSchemaVersion());
                    assertEquals(data.getValue().getKey().getField("i"), i * 100);
                    assertEquals(data.getValue().getValue().i, i * 1000);
                    c3before.acknowledge(data);
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) data.getReaderSchema().get();
                    assertNotNull(keyValueSchema.getKeySchema());
                    assertNotNull(keyValueSchema.getValueSchema());
                }

                // verify c4
                for (int i = 0; i < numMessages; i++) {
                    Message<KeyValue<V1Data, GenericRecord>> data = c4.receive();
                    assertNotNull(data.getSchemaVersion());
                    assertEquals(data.getValue().getKey().i, i * 100);
                    assertEquals(data.getValue().getValue().getField("i"), i * 1000);
                    c4.acknowledge(data);
                }
            }
        }

        // new schema version

        Schema<KeyValue<V2Data, V2Data>> pojoSchemaV2 = Schema.KeyValue(
                Schema.AVRO(V2Data.class),
                Schema.AVRO(V2Data.class),
                keyValueEncodingType);

        try (Consumer<KeyValue<GenericRecord, V2Data>> c3before = pulsarClient.newConsumer(
                Schema.KeyValue(
                        Schema.AUTO_CONSUME(),
                        Schema.AVRO(V2Data.class),
                        keyValueEncodingType))
                .topic(topic)
                .subscriptionName("sub3b")
                .subscribe();
                Producer<KeyValue<V2Data, V2Data>> p = pulsarClient.newProducer(pojoSchemaV2)
                .topic(topic)
                .enableBatching(batching)
                .create();
             Consumer<GenericRecord> c0 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                     .topic(topic)
                     .subscriptionName("sub0")
                     .subscribe();
             Consumer<KeyValue<GenericRecord, GenericRecord>> c1 = pulsarClient.newConsumer(
                     Schema.KeyValue(
                             Schema.AUTO_CONSUME(),
                             Schema.AUTO_CONSUME(),
                             keyValueEncodingType))
                     .topic(topic)
                     .subscriptionName("sub1")
                     .subscribe();
             Consumer<KeyValue<V2Data, V2Data>> c2 = pulsarClient.newConsumer(
                     Schema.KeyValue(
                             Schema.AVRO(V2Data.class),
                             Schema.AVRO(V2Data.class),
                             keyValueEncodingType))
                     .topic(topic)
                     .subscriptionName("sub2")
                     .subscribe();
             Consumer<KeyValue<GenericRecord, V2Data>> c3 = pulsarClient.newConsumer(
                     Schema.KeyValue(
                             Schema.AUTO_CONSUME(),
                             Schema.AVRO(V2Data.class),
                             keyValueEncodingType))
                     .topic(topic)
                     .subscriptionName("sub3")
                     .subscribe();
             Consumer<KeyValue<V2Data, GenericRecord>> c4 = pulsarClient.newConsumer(
                     Schema.KeyValue(
                             Schema.AVRO(V2Data.class),
                             Schema.AUTO_CONSUME(),
                             keyValueEncodingType))
                     .topic(topic)
                     .subscriptionName("sub4")
                     .subscribe()
        ) {

            int numMessages = 10;

            for (int i = 0; i < numMessages; i++) {
                p.sendAsync(new KeyValue<>(new V2Data(i * 100, i), new V2Data(i * 1000, i * 20)));
            }
            p.flush();

            // verify c0
            for (int i = 0; i < numMessages; i++) {
                Message<GenericRecord> wrapper = c0.receive();
                KeyValue<GenericRecord, GenericRecord> data = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
                assertNotNull(wrapper.getSchemaVersion());
                assertEquals(data.getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getField("i"), i * 1000);
                assertEquals(data.getKey().getField("j"), i);
                assertEquals(data.getValue().getField("j"), i * 20);
            }
            // verify c1
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<GenericRecord, GenericRecord>> data = c1.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getValue().getField("i"), i * 1000);
                assertEquals(data.getValue().getKey().getField("j"), i);
                assertEquals(data.getValue().getValue().getField("j"), i * 20);
            }

            // verify c2
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<V2Data, V2Data>> data = c2.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().i, i * 100);
                assertEquals(data.getValue().getValue().i, i * 1000);
                assertEquals(data.getValue().getKey().j, (Integer) i);
                assertEquals(data.getValue().getValue().j, (Integer) (i * 20));
            }

            // verify c3
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<GenericRecord, V2Data>> data = c3.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getValue().i, i * 1000);
                assertEquals(data.getValue().getKey().getField("j"), (Integer) i);
                assertEquals(data.getValue().getValue().j, (Integer) (i * 20));
            }

            // verify c3before
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<GenericRecord, V2Data>> data = c3before.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getValue().i, i * 1000);
                assertEquals(data.getValue().getKey().getField("j"), (Integer) i);
                assertEquals(data.getValue().getValue().j, (Integer) (i * 20));
            }

            // verify c4
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<V2Data, GenericRecord>> data = c4.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().i, i * 100);
                assertEquals(data.getValue().getValue().getField("i"), i * 1000);
                assertEquals(data.getValue().getKey().j, (Integer) i);
                assertEquals(data.getValue().getValue().getField("j"), i * 20);
            }
        }

    }

    @Test
    public void testAutoKeyValueConsumeGenericObject() throws Exception {
        String topic = NAMESPACE + "/schema-test-auto-keyvalue-consume-"+ UUID.randomUUID();

        Schema<KeyValue<V1Data, V1Data>> pojoSchema = Schema.KeyValue(
                Schema.AVRO(V1Data.class),
                Schema.AVRO(V1Data.class),
                KeyValueEncodingType.SEPARATED);

        try (Producer<KeyValue<V1Data, V1Data>> p = pulsarClient.newProducer(pojoSchema)
                .topic(topic)
                .create();
             Consumer<GenericRecord> c0 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                     .topic(topic)
                     .subscriptionName("sub0")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .subscribe();
        ) {

            int numMessages = 10;

            for (int i = 0; i < numMessages; i++) {
                p.sendAsync(new KeyValue<>(new V1Data(i * 100), new V1Data(i * 1000)));
            }
            p.flush();

            // verify c0
            for (int i = 0; i < numMessages; i++) {
                Message<GenericRecord> wrapper = c0.receive();
                KeyValue<GenericRecord, GenericRecord> data = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
                assertNotNull(wrapper.getSchemaVersion());
                assertEquals(data.getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getField("i"), i * 1000);
                c0.acknowledge(wrapper);
                KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) wrapper.getReaderSchema().get();
                assertNotNull(keyValueSchema.getKeySchema());
                assertNotNull(keyValueSchema.getValueSchema());
                assertTrue(keyValueSchema.getKeySchema().getSchemaInfo().getSchemaDefinition().contains("V1Data"));
                assertTrue(keyValueSchema.getValueSchema().getSchemaInfo().getSchemaDefinition().contains("V1Data"));
                assertTrue(keyValueSchema.getKeySchema().getNativeSchema().isPresent());
                assertTrue(keyValueSchema.getValueSchema().getNativeSchema().isPresent());
            }


            // new schema version

            Schema<KeyValue<V2Data, V2Data>> pojoSchemaV2 = Schema.KeyValue(
                    Schema.AVRO(V2Data.class),
                    Schema.AVRO(V2Data.class),
                    KeyValueEncodingType.SEPARATED);

            try (Producer<KeyValue<V2Data, V2Data>> p2 = pulsarClient.newProducer(pojoSchemaV2)
                    .topic(topic)
                    .create();
            ) {

                for (int i = 0; i < numMessages; i++) {
                    p2.sendAsync(new KeyValue<>(new V2Data(i * 100, i), new V2Data(i * 1000, i * 20)));
                }
                p2.flush();

                // verify c0
                for (int i = 0; i < numMessages; i++) {
                    Message<GenericRecord> wrapper = c0.receive();
                    KeyValue<GenericRecord, GenericRecord> data = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
                    assertNotNull(wrapper.getSchemaVersion());
                    assertEquals(data.getKey().getField("i"), i * 100);
                    assertEquals(data.getValue().getField("i"), i * 1000);
                    assertEquals(data.getKey().getField("j"), i);
                    assertEquals(data.getValue().getField("j"), i * 20);
                    KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) wrapper.getReaderSchema().get();
                    assertNotNull(keyValueSchema.getKeySchema());
                    assertNotNull(keyValueSchema.getValueSchema());
                    assertTrue(keyValueSchema.getKeySchema().getSchemaInfo().getSchemaDefinition().contains("V2Data"));
                    assertTrue(keyValueSchema.getValueSchema().getSchemaInfo().getSchemaDefinition().contains("V2Data"));
                    assertTrue(keyValueSchema.getKeySchema().getNativeSchema().isPresent());
                    assertTrue(keyValueSchema.getValueSchema().getNativeSchema().isPresent());
                }
            }
        }
    }

    @Test
    public void testGetSchemaByVersion() throws PulsarClientException, PulsarAdminException, ExecutionException, InterruptedException {
        final String topic = String.format("persistent://%s/testGetSchemaByVersion", NAMESPACE);

        @Cleanup
        PulsarClientImpl httpProtocolClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        PulsarClientImpl binaryProtocolClient = (PulsarClientImpl) pulsarClient;

        @Cleanup
        Producer p1 = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic)
                .create();

        @Cleanup
        Producer p2 = pulsarClient.newProducer(Schema.AVRO(V2Data.class))
                .topic(topic)
                .create();

        LookupService httpLookupService = httpProtocolClient.getLookup();
        LookupService binaryLookupService = binaryProtocolClient.getLookup();
        Assert.assertTrue(httpLookupService instanceof HttpLookupService);
        Assert.assertTrue(binaryLookupService instanceof BinaryProtoLookupService);
        Assert.assertEquals(admin.schemas().getAllSchemas(topic).size(), 2);
        Assert.assertTrue(httpLookupService.getSchema(TopicName.get(topic), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());
        Assert.assertTrue(httpLookupService.getSchema(TopicName.get(topic), ByteBuffer.allocate(8).putLong(1).array()).get().isPresent());
        Assert.assertTrue(binaryLookupService.getSchema(TopicName.get(topic), ByteBuffer.allocate(8).putLong(0).array()).get().isPresent());
        Assert.assertTrue(binaryLookupService.getSchema(TopicName.get(topic), ByteBuffer.allocate(8).putLong(1).array()).get().isPresent());
    }

    @Test
    public void testGetNativeSchemaWithAutoConsumeWithMultiVersion() throws Exception {
        final String topic = String.format("persistent://%s/testGetSchemaWithMultiVersion", NAMESPACE);

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test")
                .topic(topic)
                .subscribe();

        @Cleanup
        Producer<V1Data> v1DataProducer = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic)
                .create();

        @Cleanup
        Producer<V2Data> v2DataProducer = pulsarClient.newProducer(Schema.AVRO(V2Data.class))
                .topic(topic)
                .create();

        Assert.assertEquals(admin.schemas().getAllSchemas(topic).size(), 2);

        v1DataProducer.send(new V1Data());
        v2DataProducer.send(new V2Data());

        Message<?> messageV1 = consumer.receive();
        Schema<?> schemaV1 = messageV1.getReaderSchema().get();
        Message<?> messageV2 = consumer.receive();
        Schema<?> schemaV2 = messageV2.getReaderSchema().get();
        log.info("schemaV1 {} {}", schemaV1.getSchemaInfo(), schemaV1.getNativeSchema());
        log.info("schemaV2 {} {}", schemaV2.getSchemaInfo(), schemaV2.getNativeSchema());
        assertTrue(schemaV1.getSchemaInfo().getSchemaDefinition().contains("V1Data"));
        assertTrue(schemaV2.getSchemaInfo().getSchemaDefinition().contains("V2Data"));
        org.apache.avro.Schema avroSchemaV1 = (org.apache.avro.Schema) schemaV1.getNativeSchema().get();
        org.apache.avro.Schema avroSchemaV2 = (org.apache.avro.Schema) schemaV2.getNativeSchema().get();
        assertNotEquals(avroSchemaV1.toString(false), avroSchemaV2.toString(false));
        assertTrue(avroSchemaV1.toString(false).contains("V1Data"));
        assertTrue(avroSchemaV2.toString(false).contains("V2Data"));
    }

    @Test(dataProvider = "topicDomain")
    public void testAutoCreatedSchema(String domain) throws Exception {
        final String topic1 = domain + NAMESPACE + "/testAutoCreatedSchema-1";
        final String topic2 = domain + NAMESPACE + "/testAutoCreatedSchema-2";

        pulsarClient.newProducer(Schema.BYTES).topic(topic1).create().close();
        try {
            // BYTES schema is treated as no schema
            admin.schemas().getSchemaInfo(topic1);
            fail("The schema of topic1 should not exist");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 404);
        }
        pulsarClient.newProducer(Schema.STRING).topic(topic1).create().close();
        // topic1's schema becomes STRING now
        Assert.assertEquals(admin.schemas().getSchemaInfo(topic1).getType(), SchemaType.STRING);

        pulsarClient.newConsumer(Schema.BYTES).topic(topic2).subscriptionName("sub").subscribe().close();
        try {
            // BYTES schema is treated as no schema
            admin.schemas().getSchemaInfo(topic2);
            fail("The schema of topic2 should not exist");
        } catch (PulsarAdminException e) {
            Assert.assertEquals(e.getStatusCode(), 404);
        }
        pulsarClient.newConsumer(Schema.STRING).topic(topic2).subscriptionName("sub").subscribe().close();
        // topic2's schema becomes STRING now.
        Assert.assertEquals(admin.schemas().getSchemaInfo(topic2).getType(), SchemaType.STRING);
    }

    @DataProvider(name = "keyEncodingType")
    public static Object[] keyEncodingType() {
        return new Object[] { KeyValueEncodingType.SEPARATED, KeyValueEncodingType.INLINE };
    }

    @Test(dataProvider = "keyEncodingType")
    public void testAutoKeyValueConsumeGenericObjectNullValues(KeyValueEncodingType encodingType) throws Exception {
        String topic = NAMESPACE + "/schema-test-auto-keyvalue-" + encodingType + "-null-value-consume-" + UUID.randomUUID();

        Schema<KeyValue<V1Data, V1Data>> pojoSchema = Schema.KeyValue(
                Schema.AVRO(V1Data.class),
                Schema.AVRO(V1Data.class),
                encodingType);

        try (Producer<KeyValue<V1Data, V1Data>> p = pulsarClient.newProducer(pojoSchema)
                .topic(topic)
                .create();
             Consumer<GenericRecord> c0 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                     .topic(topic)
                     .subscriptionName("sub0")
                     .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                     .subscribe();
        ) {

            p.send(new KeyValue<>(new V1Data(1), new V1Data(2)));
            p.send(new KeyValue<>(new V1Data(1), null));
            p.send(new KeyValue<>(null, new V1Data(2)));
            p.send(new KeyValue<>(null, null));

            Message<GenericRecord> wrapper = c0.receive();
            assertEquals(encodingType, ((KeyValueSchemaImpl) wrapper.getReaderSchema().get()).getKeyValueEncodingType());
            KeyValue<GenericRecord, GenericRecord> data1 = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
            assertEquals(1, data1.getKey().getField("i"));
            assertEquals(2, data1.getValue().getField("i"));

            wrapper = c0.receive();
            KeyValue<GenericRecord, GenericRecord> data2 = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
            assertEquals(1, data2.getKey().getField("i"));
            assertNull(data2.getValue());

            wrapper = c0.receive();
            KeyValue<GenericRecord, GenericRecord> data3 = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
            assertNull(data3.getKey());
            assertEquals(2, data3.getValue().getField("i"));

            wrapper = c0.receive();
            KeyValue<GenericRecord, GenericRecord> data4 = (KeyValue<GenericRecord, GenericRecord>) wrapper.getValue().getNativeObject();
            assertNull(data4.getKey());
            assertNull(data4.getValue());


        }
    }

    @Test
    public void testConsumeAvroMessagesWithoutSchema() throws Exception {
        if (schemaValidationEnforced) {
            return;
        }
        final String topic = NAMESPACE + "/test-consume-avro-messages-without-schema-" + UUID.randomUUID();
        final Schema<V1Data> schema = Schema.AVRO(V1Data.class);
        final Consumer<V1Data> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int numMessages = 5;
        for (int i = 0; i < numMessages; i++) {
            producer.send(schema.encode(new V1Data(i)));
        }

        for (int i = 0; i < numMessages; i++) {
            final Message<V1Data> msg = consumer.receive(3, TimeUnit.SECONDS);
            assertNotNull(msg);
            log.info("Received {} from {}", msg.getValue().i, topic);
            assertEquals(msg.getValue().i, i);
            assertEquals(msg.getReaderSchema().orElse(Schema.BYTES).getSchemaInfo(), schema.getSchemaInfo());
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
    }
}
