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
package org.apache.pulsar.schema;

import static org.apache.pulsar.schema.MockExternalJsonSchema.MOCK_KEY_SCHEMA_DATA;
import static org.apache.pulsar.schema.MockExternalJsonSchema.MOCK_SCHEMA_DATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Test get partitioned topic schema.
 */
@Test(groups = "schema")
public class ExternalSchemaTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 1000 * 5)
    public void testMockExternalSchema() throws Exception {
        final String topic = "external-schema-topic";
        MockExternalJsonSchema<Schemas.PersonFour> externalJsonSchema =
                new MockExternalJsonSchema<>(Schemas.PersonFour.class);
        @Cleanup
        Consumer<Schemas.PersonFour> consumer = pulsarClient.newConsumer(externalJsonSchema)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        @Cleanup
        Producer<Schemas.PersonFour> producer = pulsarClient.newProducer(externalJsonSchema)
                .topic(topic)
                .enableBatching(false)
                .create();

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            Schemas.PersonFour person = new Schemas.PersonFour();
            person.setId(i);
            person.setName("user-" + i);
            person.setAge(18);
            producer.newMessage().value(person).send();
        }

        for (int i = 0; i < messageCount; i++) {
            Message<Schemas.PersonFour> message = consumer.receive();
            assertTrue(message.getSchemaId().isPresent());
            assertEquals(message.getSchemaId().get(), MockExternalJsonSchema.MOCK_SCHEMA_ID);
            assertEquals(message.getData(), MOCK_SCHEMA_DATA);
            assertNull(message.getValue());
            Assert.assertNotNull(message);
        }
    }

    @Test(timeOut = 1000 * 5)
    public void testConflictKvSchema() throws Exception {
        var externalJsonSchema = new MockExternalJsonSchema<>(Schemas.PersonFour.class);
        try {
            Schema.KeyValue(externalJsonSchema, Schema.JSON(Schemas.PersonFour.class), KeyValueEncodingType.SEPARATED);
            fail("should fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("External schema cannot be used with other Pulsar struct schema types"));
        }

        try {
            Schema.KeyValue(Schema.JSON(Schemas.PersonFour.class), externalJsonSchema);
            fail("should fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("External schema cannot be used with other Pulsar struct schema types"));
        }

        Schema.KeyValue(Schema.JSON(Schemas.PersonFour.class), Schema.STRING);
        Schema.KeyValue(Schema.STRING, Schema.JSON(Schemas.PersonFour.class));
    }

    @DataProvider(name = "provideKeyValueEncodingType")
    public Object[][] provideKeyValueEncodingType() {
        return new Object[][]{
                {KeyValueEncodingType.SEPARATED},
                {KeyValueEncodingType.INLINE}
        };
    }

    @Test(dataProvider = "provideKeyValueEncodingType", timeOut = 1000 * 5)
    public void testExternalKeyValueSchema(KeyValueEncodingType encodingType) throws Exception {
        var keySchema = new MockExternalJsonSchema<>(Schemas.PersonFour.class, true);
        var valueSchema = new MockExternalJsonSchema<>(Schemas.PersonFour.class);
        var keyValueSchema = Schema.KeyValue(keySchema, valueSchema, encodingType);

        final String topic = "testExternalKeyValueSchema-" + encodingType.name();
        @Cleanup
        Consumer<KeyValue<Schemas.PersonFour, Schemas.PersonFour>> consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        @Cleanup
        Producer<KeyValue<Schemas.PersonFour, Schemas.PersonFour>> producer = pulsarClient.newProducer(keyValueSchema)
                .topic(topic)
                .enableBatching(false)
                .create();

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            var person = new Schemas.PersonFour();
            person.setId(i);
            person.setName("user-" + i);
            person.setAge(18);
            producer.newMessage().value(new KeyValue<>(person, person)).send();
        }

        for (int i = 0; i < messageCount; i++) {
            var message = consumer.receive();
            assertTrue(message.getSchemaId().isPresent());
            assertEquals(message.getSchemaId().get(), KeyValue.generateKVSchemaId(
                    MockExternalJsonSchema.MOCK_KEY_SCHEMA_ID, MockExternalJsonSchema.MOCK_SCHEMA_ID));

            if (KeyValueEncodingType.INLINE.equals(encodingType)) {
                ByteBuf buf = Unpooled.wrappedBuffer(message.getData());
                assertEquals(buf.readInt(), MOCK_KEY_SCHEMA_DATA.length);
                byte[] data = new byte[MOCK_KEY_SCHEMA_DATA.length];
                buf.readBytes(data);
                assertEquals(data, MOCK_KEY_SCHEMA_DATA);
                assertEquals(buf.readInt(), MOCK_SCHEMA_DATA.length);
                data = new byte[MOCK_SCHEMA_DATA.length];
                buf.readBytes(data);
                assertEquals(data, MOCK_SCHEMA_DATA);
                assertEquals(buf.readableBytes(), 0);
            } else {
                assertEquals(message.getData(), MOCK_SCHEMA_DATA);
                assertEquals(message.getKeyBytes(), MOCK_KEY_SCHEMA_DATA);
            }

            assertNotNull(message.getValue());
            assertNull(message.getValue().getKey());
            assertNull(message.getValue().getValue());
        }
    }

    @Test(dataProvider = "provideKeyValueEncodingType", timeOut = 1000 * 5)
    public void testExternalSchemaWithPrimitiveSchema(KeyValueEncodingType encodingType) throws Exception {
        var keySchema = Schema.STRING;
        var valueSchema = new MockExternalJsonSchema<>(Schemas.PersonFour.class);
        var keyValueSchema = Schema.KeyValue(keySchema, valueSchema, encodingType);

        final String topic = "testExternalSchemaWithPrimitiveSchema-" + encodingType.name();
        @Cleanup
        Consumer<KeyValue<String, Schemas.PersonFour>> consumer = pulsarClient.newConsumer(keyValueSchema)
                .topic(topic)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        @Cleanup
        Producer<KeyValue<String, Schemas.PersonFour>> producer = pulsarClient.newProducer(keyValueSchema)
                .topic(topic)
                .enableBatching(false)
                .create();

        int messageCount = 10;
        for (int i = 0; i < messageCount; i++) {
            var person = new Schemas.PersonFour();
            person.setId(i);
            person.setName("user-" + i);
            person.setAge(18);
            producer.newMessage().value(new KeyValue<>("index-" + i, person)).send();
        }

        for (int i = 0; i < messageCount; i++) {
            var message = consumer.receive();
            assertTrue(message.getSchemaId().isPresent());
            assertEquals(message.getSchemaId().get(), KeyValue.generateKVSchemaId(
                    new byte[0], MockExternalJsonSchema.MOCK_SCHEMA_ID));
            var keyBytes = ("index-" + i).getBytes();

            if (KeyValueEncodingType.INLINE.equals(encodingType)) {
                ByteBuf buf = Unpooled.wrappedBuffer(message.getData());
                assertEquals(buf.readInt(), keyBytes.length);
                byte[] data = new byte[keyBytes.length];
                buf.readBytes(data);
                assertEquals(data, keyBytes);
                assertEquals(buf.readInt(), MOCK_SCHEMA_DATA.length);
                data = new byte[MOCK_SCHEMA_DATA.length];
                buf.readBytes(data);
                assertEquals(data, MOCK_SCHEMA_DATA);
                assertEquals(buf.readableBytes(), 0);
            } else {
                assertEquals(message.getData(), MOCK_SCHEMA_DATA);
                assertEquals(message.getKeyBytes(), keyBytes);
            }

            assertNotNull(message.getValue());
            assertEquals(message.getValue().getKey(), "index-" + i);
            assertNull(message.getValue().getValue());
        }
    }

}
