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
package org.apache.pulsar.schema.compatibility;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.schema.Schemas;
import org.testng.annotations.Test;


public class SchemaTypeCompatibilityCheckTest extends SharedPulsarBaseTest {

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @Test
    public void testSchemaCompatibilityStrategyInBrokerLevel() throws PulsarClientException {
        // the shared broker reads this setting when a topic loads, so it applies to the topic created below
        SchemaCompatibilityStrategy brokerStrategy = getConfig().getSchemaCompatibilityStrategy();
        getConfig().setSchemaCompatibilityStrategy(SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        try {
            String topicName = TopicName.get(
                    TopicDomain.persistent.value(),
                    NamespaceName.get(getNamespace()),
                    "testSchemaCompatibilityStrategyInBrokerLevel"
            ).toString();

            @Cleanup
            Producer<?> producer = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                    withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                    .topic(topicName)
                    .create();

            ProducerBuilder<Schemas.PersonThree> producerBuilder = pulsarClient.newProducer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                            .withPojo(Schemas.PersonThree.class).build()))
                    .topic(topicName);

            Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class,
                    producerBuilder::create);
            assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));
        } finally {
            getConfig().setSchemaCompatibilityStrategy(brokerStrategy);
        }
    }

    @Test
    public void structTypeProducerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "structTypeProducerProducerUndefinedCompatible"
        ).toString();

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeProducerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "structTypeProducerConsumerUndefinedCompatible"
        ).toString();

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeConsumerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "structTypeConsumerProducerUndefinedCompatible"
        ).toString();

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                    .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeConsumerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "structTypeConsumerConsumerUndefinedCompatible"
        ).toString();

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "1")
                .subscribe();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2");

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void primitiveTypeProducerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "primitiveTypeProducerProducerUndefinedCompatible"
        ).toString();

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeProducerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "primitiveTypeProducerConsumerUndefinedCompatible"
        ).toString();

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeConsumerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "primitiveTypeConsumerProducerUndefinedCompatible"
        ).toString();

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeConsumerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "primitiveTypeConsumerConsumerUndefinedCompatible"
        ).toString();

        @Cleanup
        Consumer<?> consumer = pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "1")
                .subscribe();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2");

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void testAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        final String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "testAlwaysCompatible" + UUID.randomUUID().toString()
        ).toString();
        Schema<?>[] schemas = new Schema[] {
                Schema.AVRO(Schemas.PersonOne.class),
                Schema.AVRO(Schemas.PersonFour.class),
                Schema.JSON(Schemas.PersonOne.class),
                Schema.JSON(Schemas.PersonFour.class),
                Schema.INT8,
                Schema.INT16,
                Schema.INT32,
                Schema.INT64,
                Schema.DATE,
                Schema.BOOL,
                Schema.DOUBLE,
                Schema.STRING,
                Schema.BYTES,
                Schema.FLOAT,
                Schema.INSTANT,
                Schema.BYTEBUFFER,
                Schema.TIME,
                Schema.TIMESTAMP,
                Schema.LOCAL_DATE,
                Schema.LOCAL_DATE_TIME,
                Schema.LOCAL_TIME
        };

        for (Schema<?> schema : schemas) {
            Producer<?> p = pulsarClient.newProducer(schema)
                    .topic(topicName)
                    .create();
            p.close();
        }

        for (Schema<?> schema : schemas) {
            Consumer<?> c = pulsarClient.newConsumer(schema)
                    .topic(topicName)
                    .subscriptionName(UUID.randomUUID().toString())
                    .subscribe();
            c.close();
        }

        List<SchemaInfo> schemasOfTopic = admin.schemas().getAllSchemas(topicName);

        // bytes[] schema and bytebuffer schema does not upload schema info to the schema registry
        assertEquals(schemasOfTopic.size(), schemas.length - 2);

        // Try to upload the schema again.
        for (Schema<?> schema : schemas) {
            Producer<?> p = pulsarClient.newProducer(schema)
                    .topic(topicName)
                    .create();
            p.close();
        }

        assertEquals(schemasOfTopic.size(), schemas.length - 2);
    }

}
