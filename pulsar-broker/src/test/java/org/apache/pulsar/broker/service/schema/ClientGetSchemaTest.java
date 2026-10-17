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
package org.apache.pulsar.broker.service.schema;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.schema.Schemas;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ClientGetSchemaTest extends SharedPulsarBaseTest {

    private static class MyClass {
        public String name;
        public int age;
    }

    @DataProvider(name = "serviceUrl")
    public Object[] serviceUrls() {
        return new Object[] {
                stringSupplier(() -> getBrokerServiceUrl()),
                stringSupplier(() -> getWebServiceUrl())
        };
    }

    private static Supplier<String> stringSupplier(Supplier<String> supplier) {
        return supplier;
    }

    @Test(dataProvider = "serviceUrl")
    public void testGetSchema(Supplier<String> serviceUrl) throws Exception {
        String topicBytes = newTopicName();
        String topicString = newTopicName();
        String topicJson = newTopicName();
        String topicAvro = newTopicName();

        // Create topics with different schema types
        @Cleanup Producer<byte[]> pBytes = pulsarClient.newProducer(Schema.BYTES).topic(topicBytes).create();
        @Cleanup Producer<String> pString = pulsarClient.newProducer(Schema.STRING).topic(topicString).create();
        @Cleanup Producer<MyClass> pAvro =
                pulsarClient.newProducer(Schema.AVRO(MyClass.class)).topic(topicAvro).create();
        @Cleanup Producer<MyClass> pJson =
                pulsarClient.newProducer(Schema.JSON(MyClass.class)).topic(topicJson).create();

        @Cleanup
        PulsarClientImpl client = (PulsarClientImpl) PulsarClient.builder().serviceUrl(serviceUrl.get()).build();

        assertEquals(client.getSchema("non-existing-topic").join(), Optional.empty());
        assertEquals(client.getSchema(topicBytes).join(), Optional.empty());
        assertEquals(client.getSchema(topicString).join(), Optional.of(Schema.STRING.getSchemaInfo()));
        assertEquals(client.getSchema(topicJson).join(), Optional.of(Schema.JSON(MyClass.class).getSchemaInfo()));
        assertEquals(client.getSchema(topicAvro).join(), Optional.of(Schema.AVRO(MyClass.class).getSchemaInfo()));
    }

    /**
     * It validates if schema ledger is deleted or non recoverable then it will clean up schema storage for the topic
     * and make the topic available.
     */
    @Test
    public void testSchemaFailure() throws Exception {
        final String topicOne = newTopicName();

        // (1) create topic with schema
        Producer<Schemas.PersonTwo> producer = pulsarClient
                .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull(false)
                        .withSupportSchemaVersioning(true).withPojo(Schemas.PersonTwo.class).build()))
                .topic(topicOne).create();

        producer.close();

        String key = TopicName.get(topicOne).getSchemaName();
        BookkeeperSchemaStorage schemaStorage = (BookkeeperSchemaStorage) getSchemaStorage();
        long schemaLedgerId = schemaStorage.getSchemaLedgerList(key).get(0);

        // (2) break schema locator by deleting schema-ledger
        schemaStorage.getBookKeeper().deleteLedger(schemaLedgerId);

        admin.topics().unload(topicOne);

        // (3) create topic again: broker should handle broken schema and load the topic successfully
        producer = pulsarClient
                .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull(false)
                        .withSupportSchemaVersioning(true).withPojo(Schemas.PersonTwo.class).build()))
                .topic(topicOne).create();

        assertNotEquals(schemaLedgerId, schemaStorage.getSchemaLedgerList(key).get(0));

        Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
        personTwo.setId(1);
        personTwo.setName("Tom");

        Consumer<Schemas.PersonTwo> consumer = pulsarClient
                .newConsumer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull(false)
                        .withSupportSchemaVersioning(true).withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test").topic(topicOne).subscribe();

        producer.send(personTwo);

        Schemas.PersonTwo personConsume = consumer.receive().getValue();
        assertEquals("Tom", personConsume.getName());
        assertEquals(1, personConsume.getId());

        producer.close();
        consumer.close();
    }

    @Test
    public void testAddProducerOnDeletedSchemaLedgerTopic() throws Exception {
        final String topicOne = newTopicName();

        boolean origValue = getConfig().isSchemaLedgerForceRecovery();
        getConfig().setSchemaLedgerForceRecovery(true);
        try {
            // (1) create topic with schema
            Producer<Schemas.PersonTwo> producer = pulsarClient
                    .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull(false)
                            .withSupportSchemaVersioning(true).withPojo(Schemas.PersonTwo.class).build()))
                    .topic(topicOne).create();

            producer.close();

            String key = TopicName.get(topicOne).getSchemaName();
            BookkeeperSchemaStorage schemaStorage = (BookkeeperSchemaStorage) getSchemaStorage();
            long schemaLedgerId = schemaStorage.getSchemaLedgerList(key).get(0);

            // (2) break schema locator by deleting schema-ledger
            schemaStorage.getBookKeeper().deleteLedger(schemaLedgerId);

            admin.topics().unload(topicOne);

            @Cleanup
            Producer<byte[]> producerWithoutSchema = pulsarClient.newProducer().topic(topicOne).create();

            assertNotNull(producerWithoutSchema);
        } finally {
            getConfig().setSchemaLedgerForceRecovery(origValue);
        }
    }
}
