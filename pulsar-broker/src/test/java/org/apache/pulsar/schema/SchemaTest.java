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
package org.apache.pulsar.schema;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.schema.compatibility.SchemaCompatibilityCheckTest.randomName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema.Parser;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.BookkeeperSchemaStorage;
import org.apache.pulsar.broker.service.schema.SchemaRegistry;
import org.apache.pulsar.broker.service.schema.SchemaRegistryServiceImpl;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonRecord;
import org.apache.pulsar.client.impl.schema.writer.AvroWriter;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.FutureUtil;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "schema")
public class SchemaTest extends MockedPulsarServiceBaseTest {

    private static final String CLUSTER_NAME = "test";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMultiTopicSetSchemaProvider() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicOne = "test-multi-version-schema-one";
        final String topicTwo = "test-multi-version-schema-two";
        final String fqtnOne = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicOne
        ).toString();

        final String fqtnTwo = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicTwo
        ).toString();


        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.topics().createPartitionedTopic(fqtnOne, 3);
        admin.topics().createPartitionedTopic(fqtnTwo, 3);

        admin.schemas().createSchema(fqtnOne, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonOne.class).build()).getSchemaInfo());

        admin.schemas().createSchema(fqtnOne, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        Schema<Schemas.PersonTwo> personTwoSchema = Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build());
        admin.schemas().createSchema(fqtnTwo, personTwoSchema.getSchemaInfo());

        Producer<Schemas.PersonTwo> producer = pulsarClient.newProducer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .topic(fqtnOne)
                .create();

        Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
        personTwo.setId(1);
        personTwo.setName("Tom");


        Consumer<Schemas.PersonTwo> consumer = pulsarClient.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test")
                .topic(fqtnOne, fqtnTwo)
                .subscribe();

        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test2")
                .topic(fqtnOne, fqtnTwo)
                .subscribe();

        producer.send(personTwo);

        Message<Schemas.PersonTwo> message = consumer.receive();
        Schemas.PersonTwo personConsume = message.getValue();
        assertEquals(personConsume.getName(), "Tom");
        assertEquals(personConsume.getId(), 1);
        Schema<?> schema = message.getReaderSchema().get();
        log.info("the-schema {}", schema);
        assertEquals(personTwoSchema.getSchemaInfo(), schema.getSchemaInfo());
        org.apache.avro.Schema nativeSchema = (org.apache.avro.Schema) schema.getNativeSchema().get();
        log.info("nativeSchema-schema {}", nativeSchema);
        assertNotNull(nativeSchema);

        // verify that with AUTO_CONSUME we can access the original schema
        // and the Native AVRO schema
        Message<?> message2 = consumer2.receive();
        Schema<?> schema2 = message2.getReaderSchema().get();
        log.info("the-schema {}", schema2);
        assertEquals(personTwoSchema.getSchemaInfo(), schema2.getSchemaInfo());
        org.apache.avro.Schema nativeSchema2 = (org.apache.avro.Schema) schema.getNativeSchema().get();
        log.info("nativeSchema-schema {}", nativeSchema2);
        assertNotNull(nativeSchema2);

        producer.close();
        consumer.close();
    }

    @Test
    public void testMultiTopicSetSchemaProviderWithKeyValue() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicOne = "test-multi-version-schema-one";
        final String topicTwo = "test-multi-version-schema-two";
        final String fqtnOne = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicOne
        ).toString();

        final String fqtnTwo = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicTwo
        ).toString();


        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.topics().createPartitionedTopic(fqtnOne, 3);
        admin.topics().createPartitionedTopic(fqtnTwo, 3);

        Schema<Schemas.PersonOne> schemaOne =  Schema.AVRO(
                SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonOne.class).build());
        admin.schemas().createSchema(fqtnOne, Schema.KeyValue(Schema.STRING, schemaOne).getSchemaInfo());

        Schema<Schemas.PersonTwo> schemaTwo = Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build());
        admin.schemas().createSchema(fqtnOne, Schema.KeyValue(Schema.STRING, schemaTwo).getSchemaInfo());

        Schema<Schemas.PersonTwo> personTwoSchema = Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build());
        admin.schemas().createSchema(fqtnTwo, Schema.KeyValue(Schema.STRING, schemaTwo).getSchemaInfo());

        Producer<KeyValue<String, Schemas.PersonTwo>> producer = pulsarClient.newProducer(Schema.KeyValue(Schema.STRING, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build())))
                .topic(fqtnOne)
                .create();

        Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
        personTwo.setId(1);
        personTwo.setName("Tom");


        Consumer<KeyValue<String, Schemas.PersonTwo>> consumer = pulsarClient.newConsumer(Schema.KeyValue(Schema.STRING, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build())))
                .subscriptionName("test")
                .topic(fqtnOne, fqtnTwo)
                .subscribe();

        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test2")
                .topic(fqtnOne, fqtnTwo)
                .subscribe();

        producer.send(new KeyValue<>("foo", personTwo));

        Message<KeyValue<String, Schemas.PersonTwo>> message = consumer.receive();
        assertEquals("foo", message.getValue().getKey());
        Schemas.PersonTwo personConsume = message.getValue().getValue();
        assertEquals(personConsume.getName(), "Tom");
        assertEquals(personConsume.getId(), 1);
        KeyValueSchemaImpl schema = (KeyValueSchemaImpl) message.getReaderSchema().get();
        log.info("the-schema {}", schema);
        assertEquals(personTwoSchema.getSchemaInfo(), schema.getValueSchema().getSchemaInfo());
        org.apache.avro.Schema nativeSchema = (org.apache.avro.Schema) schema.getValueSchema().getNativeSchema().get();
        log.info("nativeSchema-schema {}", nativeSchema);
        assertNotNull(nativeSchema);

        // verify that with AUTO_CONSUME we can access the original schema
        // and the Native AVRO schema
        Message<?> message2 = consumer2.receive();
        KeyValueSchemaImpl schema2 = (KeyValueSchemaImpl) message2.getReaderSchema().get();
        log.info("the-schema {}", schema2);
        assertEquals(personTwoSchema.getSchemaInfo(), schema2.getValueSchema().getSchemaInfo());
        org.apache.avro.Schema nativeSchema2 = (org.apache.avro.Schema) schema.getValueSchema().getNativeSchema().get();
        log.info("nativeSchema-schema {}", nativeSchema2);
        assertNotNull(nativeSchema2);

        producer.close();
        consumer.close();
    }

    @Test
    public void testSendAvroAndJsonPrimitiveSchema() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicOne = "test-multi-version-schema-one";
        final String fqtnOne = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicOne
        ).toString();


        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.namespaces().setSchemaCompatibilityStrategy(tenant + "/" + namespace,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        Producer<byte[]> producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(fqtnOne)
                .create();

        final Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME()).topic(fqtnOne)
                .subscriptionName("sub")
                .subscribe();

        int producerAvroIntegerValue = 1;
        byte[] producerAvroBytesValue = "testProducerAvroBytes".getBytes();
        producer.newMessage(Schema.AVRO(Integer.class)).value(1).send();
        producer.newMessage(Schema.AVRO(byte[].class)).value(producerAvroBytesValue).send();

        int producerJsonIntegerValue = 2;
        byte[] producerJsonBytesValue = "testProducerJsonBytes".getBytes();
        producer.newMessage(Schema.JSON(Integer.class)).value(producerJsonIntegerValue).send();
        producer.newMessage(Schema.JSON(byte[].class)).value(producerJsonBytesValue).send();

        // AVRO schema with primitive class can consume
        assertEquals(consumer.receive().getValue().getNativeObject(), producerAvroIntegerValue);
        assertArrayEquals((byte[]) consumer.receive().getValue().getNativeObject(), producerAvroBytesValue);

        // JSON schema with primitive class can consume
        assertEquals(consumer.receive().getValue().getNativeObject(), producerJsonIntegerValue);
        assertArrayEquals((byte[])  consumer.receive().getValue().getNativeObject(), producerJsonBytesValue);
}

    @Test
    public void testJSONSchemaDeserialize() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-bytes-schema";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);
        admin.schemas().createSchema(topic, Schema.JSON(Schemas.BytesRecord.class).getSchemaInfo());

        Producer<Schemas.BytesRecord> producer = pulsarClient
                .newProducer(Schema.JSON(Schemas.BytesRecord.class))
                .topic(topic)
                .create();

        Schemas.BytesRecord bytesRecord = new Schemas.BytesRecord();
        bytesRecord.setId(1);
        bytesRecord.setName("Tom");
        bytesRecord.setAddress("test".getBytes());

        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        Consumer<Schemas.BytesRecord> consumer1 = pulsarClient.newConsumer(Schema.JSON(Schemas.BytesRecord.class))
                .subscriptionName("test-sub1")
                .topic(topic)
                .subscribe();

        producer.send(bytesRecord);

        Message<GenericRecord> message = consumer.receive();
        Message<Schemas.BytesRecord> message1 = consumer1.receive();

        assertEquals(message.getValue().getField("address").getClass(),
                message1.getValue().getAddress().getClass());

        Schema<?> schema = message.getReaderSchema().get();
        Schema<?> schema1 = message1.getReaderSchema().get();
        log.info("schema {}", schema);
        log.info("schema1 {}", schema1);
        assertEquals(schema.getSchemaInfo(), schema1.getSchemaInfo());

        producer.close();
        consumer.close();
        consumer1.close();
    }

    @Test
    public void testStringSchema() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-string-schema";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);

        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        // use GenericRecord even for primitive types
        // it will be a PrimitiveRecord
        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test-sub3")
                .topic(topic)
                .subscribe();

        producer.send("foo");

        Message<String> message = consumer.receive();
        Message<GenericRecord> message2 = consumer2.receive();
        assertEquals(SchemaType.STRING, message.getReaderSchema().get().getSchemaInfo().getType());
        assertEquals(SchemaType.STRING, message2.getReaderSchema().get().getSchemaInfo().getType());

        assertEquals("foo", message.getValue());
        assertEquals(message2.getValue().getClass().getName(), "org.apache.pulsar.client.impl.schema.GenericObjectWrapper");
        assertEquals(SchemaType.STRING, message2.getValue().getSchemaType());
        assertEquals("foo", message2.getValue().getNativeObject());

        producer.close();
        consumer.close();
        consumer2.close();
    }

    @Test
    public void testUseAutoConsumeWithBytesSchemaTopic() throws Exception {
        testUseAutoConsumeWithSchemalessTopic(SchemaType.BYTES);
    }

    @Test
    public void testUseAutoConsumeWithNoneSchemaTopic() throws Exception {
        testUseAutoConsumeWithSchemalessTopic(SchemaType.NONE);
    }

    private void testUseAutoConsumeWithSchemalessTopic(SchemaType schema) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-schemaless";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);

        // set schema
        SchemaInfo schemaInfo = SchemaInfoImpl
                .builder()
                .schema(new byte[0])
                .name("dummySchema")
                .type(schema)
                .build();
        admin.schemas().createSchema(topic, schemaInfo);

        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(topic)
                .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        // use GenericRecord even for primitive types
        // it will be a GenericObjectWrapper
        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test-sub3")
                .topic(topic)
                .subscribe();

        producer.send("foo".getBytes(StandardCharsets.UTF_8));

        Message<byte[]> message = consumer.receive();
        Message<GenericRecord> message2 = consumer2.receive();
        if (schema == SchemaType.BYTES) {
            assertEquals(schema, message.getReaderSchema().get().getSchemaInfo().getType());
            assertEquals(schema, message2.getReaderSchema().get().getSchemaInfo().getType());
        } else if (schema == SchemaType.NONE) {
            // schema NONE is always reported as BYTES
            assertEquals(SchemaType.BYTES, message.getReaderSchema().get().getSchemaInfo().getType());
            assertEquals(SchemaType.BYTES, message2.getReaderSchema().get().getSchemaInfo().getType());
        } else {
            fail();
        }

        assertEquals("foo".getBytes(StandardCharsets.UTF_8), message.getValue());
        assertEquals(message2.getValue().getClass().getName(), "org.apache.pulsar.client.impl.schema.GenericObjectWrapper");
        assertEquals(SchemaType.BYTES, message2.getValue().getSchemaType());
        assertEquals("foo".getBytes(StandardCharsets.UTF_8), message2.getValue().getNativeObject());

        producer.close();
        consumer.close();
        consumer2.close();
    }

    @Test
    public void testKeyValueSchemaINLINE() throws Exception {
        testKeyValueSchema(KeyValueEncodingType.INLINE);
    }

    @Test
    public void testKeyValueSchemaSEPARATED() throws Exception {
        testKeyValueSchema(KeyValueEncodingType.SEPARATED);
    }

    private void testKeyValueSchema(KeyValueEncodingType keyValueEncodingType) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-kv-schema-" + randomName(16);

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);

        Producer<KeyValue<String, Integer>> producer = pulsarClient
                .newProducer(Schema.KeyValue(Schema.STRING, Schema.INT32, keyValueEncodingType))
                .topic(topic)
                .create();

        Consumer<KeyValue<String, Integer>> consumer = pulsarClient.newConsumer(Schema.KeyValue(Schema.STRING, Schema.INT32, keyValueEncodingType))
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME()) // keyValueEncodingType autodetected
                .subscriptionName("test-sub2")
                .topic(topic)
                .subscribe();

        producer.send(new KeyValue<>("foo", 123));

        Message<KeyValue<String, Integer>> message = consumer.receive();
        Message<GenericRecord> message2 = consumer2.receive();
        assertEquals(message.getValue(), message2.getValue().getNativeObject());

        if (keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            // with "SEPARATED encoding the routing key is the key of the KeyValue
            assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), message.getKeyBytes());
            assertArrayEquals("foo".getBytes(StandardCharsets.UTF_8), message2.getKeyBytes());
        } else {
            assertNull(message.getKey());
            assertNull(message2.getKey());
        }

        producer.close();
        consumer.close();
        consumer2.close();
    }

    @Test
    public void testKeyValueSchemaWithStructsINLINE() throws Exception {
        testKeyValueSchemaWithStructs(KeyValueEncodingType.INLINE);
    }

    @Test
    public void testKeyValueSchemaWithStructsSEPARATED() throws Exception {
        testKeyValueSchemaWithStructs(KeyValueEncodingType.SEPARATED);
    }

    private void testKeyValueSchemaWithStructs(KeyValueEncodingType keyValueEncodingType) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-kv-schema-" + randomName(16);

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);

        Producer<KeyValue<Schemas.PersonOne, Schemas.PersonTwo>> producer = pulsarClient
                .newProducer(Schema.KeyValue(Schema.AVRO(Schemas.PersonOne.class), Schema.AVRO(Schemas.PersonTwo.class), keyValueEncodingType))
                .topic(topic)
                .create();

        Consumer<KeyValue<Schemas.PersonOne, Schemas.PersonTwo>> consumer = pulsarClient.newConsumer(Schema.KeyValue(Schema.AVRO(Schemas.PersonOne.class), Schema.AVRO(Schemas.PersonTwo.class), keyValueEncodingType))
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        Consumer<GenericRecord> consumer2 = pulsarClient.newConsumer(Schema.AUTO_CONSUME()) // keyValueEncodingType autodetected
                .subscriptionName("test-sub2")
                .topic(topic)
                .subscribe();

        Schemas.PersonOne key = new Schemas.PersonOne(8787);
        Schemas.PersonTwo value = new Schemas.PersonTwo(323, "foo");
        producer.send(new KeyValue<>(key, value));

        Message<KeyValue<Schemas.PersonOne, Schemas.PersonTwo>> message = consumer.receive();
        Message<GenericRecord> message2 = consumer2.receive();
        log.info("message: {},{}", message.getValue(), message.getValue().getClass());
        log.info("message2: {},{}", message2.getValue().getNativeObject(), message2.getValue().getNativeObject().getClass());
        KeyValue<GenericRecord, GenericRecord> keyValue2 = (KeyValue<GenericRecord, GenericRecord>) message2.getValue().getNativeObject();
        assertEquals(message.getValue().getKey().id, keyValue2.getKey().getField("id"));
        assertEquals(message.getValue().getValue().id, keyValue2.getValue().getField("id"));
        assertEquals(message.getValue().getValue().name, keyValue2.getValue().getField("name"));

        Schema<?> schema = message.getReaderSchema().get();
        Schema<?> schemaFromGenericRecord = message.getReaderSchema().get();
        KeyValueSchemaImpl keyValueSchema = (KeyValueSchemaImpl) schema;
        KeyValueSchemaImpl keyValueSchemaFromGenericRecord = (KeyValueSchemaImpl) schemaFromGenericRecord;
        assertEquals(keyValueSchema.getSchemaInfo(), keyValueSchemaFromGenericRecord.getSchemaInfo());

        if (keyValueEncodingType == KeyValueEncodingType.SEPARATED) {
            // with "SEPARATED encoding the routing key is the key of the KeyValue
            assertNotNull(message.getKeyBytes());
            assertNotNull(message2.getKeyBytes());
        } else {
            assertNull(message.getKey());
            assertNull(message2.getKey());
        }

        producer.close();
        consumer.close();
        consumer2.close();
    }

    @Test
    public void testIsUsingAvroSchemaParser() {
        for (SchemaType value : SchemaType.values()) {
            if (value == SchemaType.AVRO || value == SchemaType.JSON || value == SchemaType.PROTOBUF) {
                assertTrue(SchemaRegistryServiceImpl.isUsingAvroSchemaParser(value));
            } else {
                assertFalse(SchemaRegistryServiceImpl.isUsingAvroSchemaParser(value));
            }
        }
    }

    @Test
    public void testNullKeyValueProperty() throws PulsarAdminException, PulsarClientException {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();
        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        final Map<String, String> map = new HashMap<>();
        map.put("key", null);
        map.put(null, "value"); // null key is not allowed for JSON, it's only for test here

        final Schema<Integer> integerSchema = Schema.JSON(Integer.class);
        ((SchemaInfoImpl) integerSchema.getSchemaInfo()).setProperties(map);

        final Consumer<Integer> consumer = pulsarClient.newConsumer(integerSchema).topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.close();
    }

    @Test
    public void testDeleteTopicAndSchema() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-delete-topic-and-schema";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        @Cleanup
        Producer<Schemas.PersonOne> p1 = pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topic)
                .create();

        @Cleanup
        Producer<Schemas.PersonThree> p2 = pulsarClient.newProducer(Schema.JSON(Schemas.PersonThree.class))
                .topic(topic)
                .create();

        List<CompletableFuture<SchemaRegistry.SchemaAndMetadata>> schemaFutures =
                this.getPulsar().getSchemaRegistryService().getAllSchemas(TopicName.get(topic).getSchemaName()).get();
        FutureUtil.waitForAll(schemaFutures).get();
        List<SchemaRegistry.SchemaAndMetadata> schemas = schemaFutures.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                return null;
            }
        }).collect(Collectors.toList());

        assertEquals(schemas.size(), 2);
        for (SchemaRegistry.SchemaAndMetadata schema : schemas) {
            assertNotNull(schema);
        }

        List<Long> ledgers = ((BookkeeperSchemaStorage)this.getPulsar().getSchemaStorage())
                .getSchemaLedgerList(TopicName.get(topic).getSchemaName());
        assertEquals(ledgers.size(), 2);
        admin.topics().delete(topic, true, true);
        assertEquals(this.getPulsar().getSchemaRegistryService()
                .trimDeletedSchemaAndGetList(TopicName.get(topic).getSchemaName()).get().size(), 0);

        for (Long ledger : ledgers) {
            try {
                getPulsar().getBookKeeperClient().openLedger(ledger, BookKeeper.DigestType.CRC32, new byte[]{});
                fail();
            } catch (BKException.BKNoSuchLedgerExistsException ignore) {
            }
        }
    }

    @Test
    public void testDeleteTopicAndSchemaForV1() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String cluster = CLUSTER_NAME;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicOne = "not-partitioned-topic";
        final String topic2 = "persistent://" + tenant + "/" + cluster + "/" + namespace + "/partitioned-topic";

        // persistent, not-partitioned v1/topic
        final String topic1 = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                cluster,
                namespace,
                topicOne).toString();

        // persistent, partitioned v1/topic
        admin.topics().createPartitionedTopic(topic2, 1);

        @Cleanup
        Producer<Schemas.PersonOne> p1_1 = pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topic1)
                .create();

        @Cleanup
        Producer<Schemas.PersonThree> p1_2 = pulsarClient.newProducer(Schema.JSON(Schemas.PersonThree.class))
                .topic(topic1)
                .create();
        @Cleanup
        Producer<Schemas.PersonThree> p2_1 = pulsarClient.newProducer(Schema.JSON(Schemas.PersonThree.class))
                .topic(topic2)
                .create();

        List<CompletableFuture<SchemaRegistry.SchemaAndMetadata>> schemaFutures1 =
                this.getPulsar().getSchemaRegistryService().getAllSchemas(TopicName.get(topic1).getSchemaName()).get();
        FutureUtil.waitForAll(schemaFutures1).get();
        List<SchemaRegistry.SchemaAndMetadata> schemas1 = schemaFutures1.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                return null;
            }
        }).collect(Collectors.toList());
        assertEquals(schemas1.size(), 2);
        for (SchemaRegistry.SchemaAndMetadata schema : schemas1) {
            assertNotNull(schema);
        }

        List<CompletableFuture<SchemaRegistry.SchemaAndMetadata>> schemaFutures2 =
                this.getPulsar().getSchemaRegistryService().getAllSchemas(TopicName.get(topic2).getSchemaName()).get();
        FutureUtil.waitForAll(schemaFutures2).get();
        List<SchemaRegistry.SchemaAndMetadata> schemas2 = schemaFutures2.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception e) {
                return null;
            }
        }).collect(Collectors.toList());
        assertEquals(schemas2.size(), 1);
        for (SchemaRegistry.SchemaAndMetadata schema : schemas2) {
            assertNotNull(schema);
        }

        // not-force and not-delete-schema when delete topic
        try {
            admin.topics().delete(topic1, false, false);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Topic has active producers/subscriptions"));
        }
        assertEquals(this.getPulsar().getSchemaRegistryService()
                .trimDeletedSchemaAndGetList(TopicName.get(topic1).getSchemaName()).get().size(), 2);
        try {
            admin.topics().deletePartitionedTopic(topic2, false, false);
            fail();
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Topic has active producers/subscriptions"));
        }
        assertEquals(this.getPulsar().getSchemaRegistryService()
                .trimDeletedSchemaAndGetList(TopicName.get(topic2).getSchemaName()).get().size(), 1);

        // force and delete-schema when delete topic
        admin.topics().delete(topic1, true, true);
        assertEquals(this.getPulsar().getSchemaRegistryService()
                .trimDeletedSchemaAndGetList(TopicName.get(topic1).getSchemaName()).get().size(), 0);
        admin.topics().deletePartitionedTopic(topic2, true, true);
        assertEquals(this.getPulsar().getSchemaRegistryService()
                .trimDeletedSchemaAndGetList(TopicName.get(topic2).getSchemaName()).get().size(), 0);
    }

    @Test
    public void testProducerMultipleSchemaMessages() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "auto_schema_test";

        String ns = tenant + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));
        admin.namespaces().setSchemaCompatibilityStrategy(ns, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String topic = TopicName.get(TopicDomain.persistent.value(), tenant, namespace, topicName).toString();
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(topic)
                .create();

        producer.newMessage(Schema.STRING).value("test").send();
        producer.newMessage(Schema.JSON(Schemas.PersonThree.class)).value(new Schemas.PersonThree(0, "ran")).send();
        producer.newMessage(Schema.AVRO(Schemas.PersonThree.class)).value(new Schemas.PersonThree(0, "ran")).send();
        producer.newMessage(Schema.AVRO(Schemas.PersonOne.class)).value(new Schemas.PersonOne(0)).send();
        producer.newMessage(Schema.JSON(Schemas.PersonThree.class)).value(new Schemas.PersonThree(1, "tang")).send();
        producer.newMessage(Schema.BYTES).value("test".getBytes(StandardCharsets.UTF_8)).send();
        producer.newMessage(Schema.BYTES).value("test".getBytes(StandardCharsets.UTF_8)).send();
        producer.newMessage(Schema.BOOL).value(true).send();
        
        Schema<Schemas.PersonThree> personThreeSchema = Schema.AVRO(Schemas.PersonThree.class);
        byte[] personThreeSchemaBytes = personThreeSchema.getSchemaInfo().getSchema();
        org.apache.avro.Schema personThreeSchemaAvroNative = new Parser().parse(new ByteArrayInputStream(personThreeSchemaBytes));
        AvroWriter<Schemas.PersonThree> writer = new AvroWriter<>(personThreeSchemaAvroNative);
        byte[] content = writer.write(new Schemas.PersonThree(0, "ran"));
        producer.newMessage(Schema.NATIVE_AVRO(personThreeSchemaAvroNative)).value(content).send();

        List<SchemaInfo> allSchemas = admin.schemas().getAllSchemas(topic);
        allSchemas.forEach(schemaInfo -> {
            ((SchemaInfoImpl)schemaInfo).setTimestamp(0);
        });
        Assert.assertEquals(allSchemas.size(), 5);
        Assert.assertEquals(allSchemas.get(0), Schema.STRING.getSchemaInfo());
        Assert.assertEquals(allSchemas.get(1), Schema.JSON(Schemas.PersonThree.class).getSchemaInfo());
        Assert.assertEquals(allSchemas.get(2), Schema.AVRO(Schemas.PersonThree.class).getSchemaInfo());
        Assert.assertEquals(allSchemas.get(3), Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        Assert.assertEquals(allSchemas.get(4), Schema.BOOL.getSchemaInfo());
    }

    @Test
    public void testNullKey() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-schema-" + randomName(16);

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);

        Producer<String> producer = pulsarClient
                .newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        producer.send("foo");

        Message<String> message = consumer.receive();
        assertNull(message.getKey());
        assertEquals("foo", message.getValue());
    }

    @Test
    public void testConsumeMultipleSchemaMessages() throws Exception {
        final String namespace = "test-namespace-" + randomName(16);
        String ns = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));
        admin.namespaces().setSchemaCompatibilityStrategy(ns, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String autoProducerTopic = getTopicName(ns, "auto_produce_topic");
        Producer<byte[]> autoProducer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES())
                .topic(autoProducerTopic)
                .create();

        AtomicInteger totalMsgCnt = new AtomicInteger(0);
        generateDataByDifferentSchema(ns, "bytes_schema", Schema.BYTES, "bytes value".getBytes(),
                autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "string_schema", Schema.STRING, "string value",
                autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "bool_schema", Schema.BOOL, true,
                autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "json_one_schema", Schema.JSON(Schemas.PersonOne.class),
                new Schemas.PersonOne(1), autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "json_three_schema", Schema.JSON(Schemas.PersonThree.class),
                new Schemas.PersonThree(3, "ran"), autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "json_four_schema", Schema.JSON(Schemas.PersonFour.class),
                new Schemas.PersonFour(4, "tang", 18), autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "avro_one_schema", Schema.AVRO(Schemas.PersonOne.class),
                new Schemas.PersonOne(10), autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "k_one_v_three_schema_separate",
                Schema.KeyValue(Schema.JSON(Schemas.PersonOne.class),
                        Schema.JSON(Schemas.PersonThree.class), KeyValueEncodingType.SEPARATED),
                new KeyValue<>(new Schemas.PersonOne(1), new Schemas.PersonThree(3, "kv-separate")),
                autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "k_one_v_four_schema_inline",
                Schema.KeyValue(Schema.JSON(Schemas.PersonOne.class),
                        Schema.JSON(Schemas.PersonFour.class), KeyValueEncodingType.INLINE),
                new KeyValue<>(new Schemas.PersonOne(10), new Schemas.PersonFour(30, "kv-inline", 20)),
                autoProducer, totalMsgCnt);
        generateDataByDifferentSchema(ns, "k_int_v_three_schema_separate",
                Schema.KeyValue(Schema.INT32, Schema.JSON(Schemas.PersonThree.class), KeyValueEncodingType.SEPARATED),
                new KeyValue<>(100, new Schemas.PersonThree(40, "kv-separate")),
                autoProducer, totalMsgCnt);

        Consumer<GenericRecord> autoConsumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(autoProducerTopic)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<GenericRecord> message;
        for (int i = 0; i < totalMsgCnt.get(); i++) {
            message = autoConsumer.receive(5, TimeUnit.SECONDS);
            if (message == null) {
                Assert.fail("Failed to receive multiple schema message.");
            }
            log.info("auto consumer get native object class: {}, value: {}",
                    message.getValue().getNativeObject().getClass(), message.getValue().getNativeObject());
            checkSchemaForAutoSchema(message);
        }
    }

    private String getTopicName(String ns, String baseTopic) {
        return ns + "/" + baseTopic;
    }

    private void generateDataByDifferentSchema(String ns,
                                               String baseTopic,
                                               Schema schema,
                                               Object data,
                                               Producer<?> autoProducer,
                                               AtomicInteger totalMsgCnt) throws PulsarClientException {
        String topic = getTopicName(ns, baseTopic);
        Producer<Object> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .create();
        producer.newMessage().value(data).property("baseTopic", baseTopic).send();
        totalMsgCnt.incrementAndGet();

        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(topic)
                .subscriptionName("test")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Message<GenericRecord> message = consumer.receive(5, TimeUnit.SECONDS);
        if (message == null) {
            Assert.fail("Failed to receive message for topic " + topic);
        }
        if (!message.getReaderSchema().isPresent()) {
            Assert.fail("Failed to get reader schema for topic " + topic);
        }
        message.getValue();

        TypedMessageBuilder messageBuilder = autoProducer
                .newMessage(Schema.AUTO_PRODUCE_BYTES(message.getReaderSchema().get()))
                .value(message.getData())
                .properties(message.getProperties());
        if (message.getKeyBytes() != null) {
            messageBuilder.keyBytes(message.getKeyBytes());
        }
        messageBuilder.send();

        producer.close();
        consumer.close();
    }

    private void checkSchemaForAutoSchema(Message<GenericRecord> message) {
        if (!message.getReaderSchema().isPresent()) {
            Assert.fail("Failed to get reader schema for auto consume multiple schema topic.");
        }
        Object nativeObject = message.getValue().getNativeObject();
        String baseTopic = message.getProperty("baseTopic");
        JsonNode jsonNode;
        KeyValue<?, ?> kv;
        switch (baseTopic) {
            case "bytes_schema":
                Assert.assertEquals(new String((byte[]) nativeObject), "bytes value");
                break;
            case "string_schema":
                Assert.assertEquals((String) nativeObject, "string value");
                break;
            case "bool_schema":
                Assert.assertEquals(nativeObject, Boolean.TRUE);
                break;
            case "json_one_schema":
                jsonNode = (JsonNode) nativeObject;
                Assert.assertEquals(jsonNode.get("id").intValue(), 1);
                break;
            case "json_three_schema":
                jsonNode = (JsonNode) nativeObject;
                Assert.assertEquals(jsonNode.get("id").intValue(), 3);
                Assert.assertEquals(jsonNode.get("name").textValue(), "ran");
                break;
            case "json_four_schema":
                jsonNode = (JsonNode) nativeObject;
                Assert.assertEquals(jsonNode.get("id").intValue(), 4);
                Assert.assertEquals(jsonNode.get("name").textValue(), "tang");
                Assert.assertEquals(jsonNode.get("age").intValue(), 18);
                break;
            case "avro_one_schema":
                org.apache.avro.generic.GenericRecord genericRecord =
                        (org.apache.avro.generic.GenericRecord) nativeObject;
                Assert.assertEquals(genericRecord.get("id"), 10);
                break;
            case "k_one_v_three_schema_separate":
                kv = (KeyValue<GenericRecord, GenericRecord>) nativeObject;
                jsonNode = ((GenericJsonRecord) kv.getKey()).getJsonNode();
                Assert.assertEquals(jsonNode.get("id").intValue(), 1);
                jsonNode = ((GenericJsonRecord) kv.getValue()).getJsonNode();
                Assert.assertEquals(jsonNode.get("id").intValue(), 3);
                Assert.assertEquals(jsonNode.get("name").textValue(), "kv-separate");
                break;
            case "k_one_v_four_schema_inline":
                kv = (KeyValue<GenericRecord, GenericRecord>) nativeObject;
                jsonNode = ((GenericJsonRecord) kv.getKey()).getJsonNode();
                Assert.assertEquals(jsonNode.get("id").intValue(), 10);
                jsonNode = ((GenericJsonRecord) kv.getValue()).getJsonNode();
                Assert.assertEquals(jsonNode.get("id").intValue(), 30);
                Assert.assertEquals(jsonNode.get("name").textValue(), "kv-inline");
                Assert.assertEquals(jsonNode.get("age").intValue(), 20);
                break;
            case "k_int_v_three_schema_separate":
                kv = (KeyValue<Integer, GenericRecord>) nativeObject;
                Assert.assertEquals(kv.getKey(), 100);
                jsonNode = ((GenericJsonRecord) kv.getValue()).getJsonNode();
                Assert.assertEquals(jsonNode.get("id").intValue(), 40);
                Assert.assertEquals(jsonNode.get("name").textValue(), "kv-separate");
                break;
            default:
                // nothing to do
        }
    }

    @Test
    public void testAvroSchemaWithHttpLookup() throws Exception {
        stopBroker();
        isTcpLookup = false;
        setup();
        testIncompatibleSchema();
    }

    @Test
    public void testAvroSchemaWithTcpLookup() throws Exception {
        stopBroker();
        isTcpLookup = true;
        setup();
        testIncompatibleSchema();
    }

    private void testIncompatibleSchema() throws Exception {
        final String namespace = "test-namespace-" + randomName(16);
        String ns = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));

        final String autoProducerTopic = getTopicName(ns, "testEmptySchema");

        @Cleanup
        Consumer<User> consumer = pulsarClient
                .newConsumer(Schema.AVRO(User.class))
                .topic(autoProducerTopic)
                .subscriptionType(SubscriptionType.Shared)
                .subscriptionName("sub-1")
                .subscribe();

        @Cleanup
        Producer<User> userProducer = pulsarClient
                .newProducer(Schema.AVRO(User.class))
                .topic(autoProducerTopic)
                .enableBatching(false)
                .create();

        @Cleanup
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(autoProducerTopic)
                .enableBatching(false)
                .create();

        User test = new User("test");
        userProducer.send(test);
        producer.send("test".getBytes(StandardCharsets.UTF_8));
        Message<User> message1 = consumer.receive();
        Assert.assertEquals(test, message1.getValue());
        Message<User> message2 = consumer.receive();
        try {
            message2.getValue();
        } catch (SchemaSerializationException e) {
            final String schemaString =
                    new String(Schema.AVRO(User.class).getSchemaInfo().getSchema(), StandardCharsets.UTF_8);
            Assert.assertTrue(e.getMessage().contains(schemaString));
            Assert.assertTrue(e.getMessage().contains("payload (4 bytes)"));
        }
    }

    /**
     * This test just ensure that schema check still keeps the original logic: if there has any producer, but no schema
     * was registered, the new consumer could not register new schema.
     * TODO: I think this design should be improved: if a producer used "AUTO_PRODUCE_BYTES" schema, we should allow
     *       the new consumer to register new schema. But before we can solve this problem, we need to modify
     *       "CmdProducer" to let the Broker know that the Producer uses a schema of type "AUTO_PRODUCE_BYTES".
     */
    @Test
    public void testAutoProduceAndSpecifiedConsumer() throws Exception {
        final String namespace = PUBLIC_TENANT + "/ns_" + randomName(16);
        admin.namespaces().createNamespace(namespace, Sets.newHashSet(CLUSTER_NAME));
        final String topicName = "persistent://" + namespace + "/tp_" + randomName(16);
        admin.topics().createNonPartitionedTopic(topicName);

        Producer producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topicName).create();
        try {
            pulsarClient.newConsumer(Schema.STRING).topic(topicName).subscriptionName("sub1").subscribe();
            fail("Should throw ex: Topic does not have schema to check");
        } catch (Exception ex){
            assertTrue(ex.getMessage().contains("Topic does not have schema to check"));
        }

        // Cleanup.
        producer.close();
        admin.topics().delete(topicName);
    }

    @Test
    public void testCreateSchemaInParallel() throws Exception {
        final String namespace = "test-namespace-" + randomName(16);
        String ns = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(ns, Sets.newHashSet(CLUSTER_NAME));

        final String topic = getTopicName(ns, "testCreateSchemaInParallel");
        ExecutorService executor = Executors.newFixedThreadPool(16);
        List<CompletableFuture<Producer<Schemas.PersonOne>>> producers = new ArrayList<>(16);
        CountDownLatch latch = new CountDownLatch(16);
        for (int i = 0; i < 16; i++) {
            executor.execute(() -> {
                producers.add(pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                        .topic(topic).createAsync());
                latch.countDown();
            });
        }
        latch.await();
        FutureUtil.waitForAll(producers).join();
        assertEquals(admin.schemas().getAllSchemas(topic).size(), 1);
        producers.clear();

        List<CompletableFuture<Producer<Schemas.PersonThree>>> producers2 = new ArrayList<>(16);
        CountDownLatch latch2 = new CountDownLatch(16);
        for (int i = 0; i < 16; i++) {
            executor.execute(() -> {
                producers2.add(pulsarClient.newProducer(Schema.AVRO(Schemas.PersonThree.class))
                        .topic(topic).createAsync());
                latch2.countDown();
            });
        }
        latch2.await();
        FutureUtil.waitForAll(producers2).join();
        assertEquals(admin.schemas().getAllSchemas(topic).size(), 2);
        producers.forEach(p -> {
            try {
                p.join().close();
            } catch (Exception ignore) {
            }
        });
        producers2.forEach(p -> {
            try {
                p.join().close();
            } catch (Exception ignore) {
            }
        });
        producers.clear();
        producers2.clear();
        executor.shutdownNow();
    }

    @EqualsAndHashCode
    static class User implements Serializable {
        private String name;
        public User() {}
        public User(String name) {
            this.name = name;
        }
    }

}
