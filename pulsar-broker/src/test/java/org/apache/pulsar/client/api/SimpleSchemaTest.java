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
package org.apache.pulsar.client.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.avro.reflect.ReflectData;
import org.apache.avro.Schema.Parser;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException.IncompatibleSchemaException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidMessageException;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.BinaryProtoLookupService;
import org.apache.pulsar.client.impl.HttpLookupService;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SimpleSchemaTest extends ProducerConsumerBase {

    @DataProvider(name = "batchingModes")
    public static Object[][] batchingModes() {
        return new Object[][] {
            { true },
            { false }
        };
    }

    @DataProvider(name = "schemaValidationModes")
    public static Object[][] schemaValidationModes() {
        return new Object[][] { { true }, { false } };
    }

    private final boolean schemaValidationEnforced;

    @Factory(dataProvider = "schemaValidationModes")
    public SimpleSchemaTest(boolean schemaValidationEnforced) {
        this.schemaValidationEnforced = schemaValidationEnforced;
    }


    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setSchemaValidationEnforced(schemaValidationEnforced);
        this.isTcpLookup = true;
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testString() throws Exception {
        try (Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1")
                .subscriptionName("my-subscriber-name").subscribe();
             Producer<String> producer = pulsarClient.newProducer(Schema.STRING)
                .topic("persistent://my-property/my-ns/my-topic1").create()) {
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
        String topic = "my-property/my-ns/schema-test";
        try (Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class))
                .topic(topic).create()) {
            p.send(new V1Data(0));
        }
    }

    @Test
    public void newProducerTopicExistsWithoutSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
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
        String topic = "my-property/my-ns/schema-test";
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
        String topic = "my-property/my-ns/schema-test";

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
    }

    @Test
    public void newProducerForMessageSchemaOnTopicWithMultiVersionSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
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
            Assert.assertNull(msg1Value.j);
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
                Assert.assertEquals(msg3.getSchemaVersion(), SchemaVersion.Empty.bytes());
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
        String topic = "my-property/my-ns/schema-test";
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
            Assert.assertNull(msg1Value.j);
            Assert.assertEquals(msg1.getSchemaVersion(), new LongSchemaVersion(0).bytes());

            Message<V2Data> msg2 = c.receive();
            Assert.assertEquals(data2, msg2.getValue());
            Assert.assertEquals(msg2.getSchemaVersion(), new LongSchemaVersion(1).bytes());

            Message<V2Data> msg3 = c.receive();
            V2Data msg3Value = msg3.getValue();
            Assert.assertEquals(data3.i, msg3Value.i);
            Assert.assertNull(msg3Value.j);
            Assert.assertEquals(msg3.getSchemaVersion(), new LongSchemaVersion(0).bytes());
        }
    }

    @Test
    public void newProducerForMessageSchemaOnTopicInitialWithNoSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";
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
        Assert.assertEquals(allSchemas, Arrays.asList(v1Schema.getSchemaInfo(),
                                                      v2Schema.getSchemaInfo()));
    }

    @Test
    public void newProducerForMessageSchemaWithBatch() throws Exception {
        String topic = "my-property/my-ns/schema-test";
        Consumer<V2Data> c = pulsarClient.newConsumer(Schema.AVRO(V2Data.class))
                                         .topic(topic)
                                         .subscriptionName("sub1").subscribe();
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
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof IncompatibleSchemaException, e.getMessage());
                }
            }
        }
        p.flush();
        for (int i = 0; i < total; ++i) {
            V2Data value = c.receive().getValue();
            if (i / batch % 2 == 0) {
                Assert.assertNull(value.j);
                Assert.assertEquals(value.i, i);
            } else {
                Assert.assertEquals(value, new V2Data(i, i + total));
            }
        }
        c.close();
    }

    @Test
    public void newProducerWithMultipleSchemaDisabled() throws Exception {
        String topic = "my-property/my-ns/schema-test";
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
        String topic = "my-property/my-ns/schema-test";

        try (Consumer<V1Data> c = pulsarClient.newConsumer(Schema.AVRO(V1Data.class))
                .topic(topic).subscriptionName("sub1").subscribe();
             Producer<V1Data> p = pulsarClient.newProducer(Schema.AVRO(V1Data.class)).topic(topic).create()) {
            V1Data toSend = new V1Data(1);
            p.send(toSend);
            Assert.assertEquals(toSend, c.receive().getValue());
        }
    }

    @Test
    public void newConsumerWithSchemaOnExistingTopicWithoutSchema() throws Exception {
        String topic = "my-property/my-ns/schema-test";

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
        String topic = "my-property/my-ns/schema-test";

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
        String topic = "my-property/my-ns/schema-test";

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
        String topic = "my-property/my-ns/schema-test";

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
        String topic = "my-property/my-ns/schema-test-auto-consume-" + batching;

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
            }
        }
    }

    @Test(dataProvider = "batchingModes")
    public void testAutoKeyValueConsume(boolean batching) throws Exception {
        String topic = "my-property/my-ns/schema-test-auto-keyvalue-consume-" + batching;

        Schema<KeyValue<V1Data, V1Data>> pojoSchema = Schema.KeyValue(
            Schema.AVRO(V1Data.class),
            Schema.AVRO(V1Data.class),
            KeyValueEncodingType.SEPARATED);

        try (Producer<KeyValue<V1Data, V1Data>> p = pulsarClient.newProducer(pojoSchema)
             .topic(topic)
             .enableBatching(batching)
             .create();
             Consumer<KeyValue<GenericRecord, GenericRecord>> c1 = pulsarClient.newConsumer(
                 Schema.KeyValue(
                     Schema.AUTO_CONSUME(),
                     Schema.AUTO_CONSUME(),
                     KeyValueEncodingType.SEPARATED))
             .topic(topic)
             .subscriptionName("sub1")
             .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
             .subscribe();
             Consumer<KeyValue<V1Data, V1Data>> c2 = pulsarClient.newConsumer(
                 Schema.KeyValue(
                     Schema.AVRO(V1Data.class),
                     Schema.AVRO(V1Data.class),
                     KeyValueEncodingType.SEPARATED))
             .topic(topic)
             .subscriptionName("sub2")
             .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
             .subscribe();
             Consumer<KeyValue<GenericRecord, V1Data>> c3 = pulsarClient.newConsumer(
                 Schema.KeyValue(
                     Schema.AUTO_CONSUME(),
                     Schema.AVRO(V1Data.class),
                     KeyValueEncodingType.SEPARATED))
             .topic(topic)
             .subscriptionName("sub3")
             .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
             .subscribe();
             Consumer<KeyValue<V1Data, GenericRecord>> c4 = pulsarClient.newConsumer(
                 Schema.KeyValue(
                     Schema.AVRO(V1Data.class),
                     Schema.AUTO_CONSUME(),
                     KeyValueEncodingType.SEPARATED))
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

            // verify c1
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<GenericRecord, GenericRecord>> data = c1.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getValue().getField("i"), i * 1000);
            }

            // verify c2
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<V1Data, V1Data>> data = c2.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().i, i * 100);
                assertEquals(data.getValue().getValue().i, i * 1000);
            }

            // verify c3
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<GenericRecord, V1Data>> data = c3.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().getField("i"), i * 100);
                assertEquals(data.getValue().getValue().i, i * 1000);
            }

            // verify c4
            for (int i = 0; i < numMessages; i++) {
                Message<KeyValue<V1Data, GenericRecord>> data = c4.receive();
                assertNotNull(data.getSchemaVersion());
                assertEquals(data.getValue().getKey().i, i * 100);
                assertEquals(data.getValue().getValue().getField("i"), i * 1000);
            }
        }
    }

    @Test
    public void testGetSchemaByVersion() throws PulsarClientException, PulsarAdminException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-property/my-ns/testGetSchemaByVersion";

        PulsarClientImpl httpProtocolClient = (PulsarClientImpl) PulsarClient.builder().serviceUrl(brokerUrl.toString()).build();
        PulsarClientImpl binaryProtocolClient = (PulsarClientImpl) pulsarClient;

        pulsarClient.newProducer(Schema.AVRO(V1Data.class))
            .topic(topic)
            .create();

        pulsarClient.newProducer(Schema.AVRO(V2Data.class))
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
}
