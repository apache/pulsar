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
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Topic level schema compatibility tests on the shared broker, each in a namespace of its own.
 * {@link SchemaTypeCompatibilityCheckOnTopicLevelOwnBrokerTest} has the ones that disable the broker's topic policies.
 */
public class SchemaTypeCompatibilityCheckOnTopicLevelTest extends SharedPulsarBaseTest {

    @Override
    protected boolean checkClientHandlesClosed() {
        return true;
    }

    @Test
    public void testSetAlwaysInCompatibleStrategyOnTopicLevelAndCheckAlwaysInCompatible()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "testSetAlwaysInCompatibleStrategyOnTopicLevelAndCheckAlwaysInCompatible"
        ).toString();

        getPulsar().getAdminClient().topics().createNonPartitionedTopic(topicName);
        getPulsar().getAdminClient().topicPolicies().setSchemaCompatibilityStrategy(topicName,
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        Awaitility.await()
                .untilAsserted(() -> assertEquals(
                        getPulsar().getAdminClient().topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        ProducerBuilder<Schemas.PersonThree> producerBuilder = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));
    }

    @Test
    public void testSetAlwaysCompatibleOnNamespaceLevelAndCheckAlwaysInCompatible()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "testSetAlwaysCompatibleOnNamespaceLevelAndCheckAlwaysInCompatible"
        ).toString();

        getPulsar().getAdminClient().topics().createNonPartitionedTopic(topicName);
        getPulsar().getAdminClient().topicPolicies().setSchemaCompatibilityStrategy(topicName,
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        Awaitility.await()
                .untilAsserted(() -> assertEquals(
                        getPulsar().getAdminClient().topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        ProducerBuilder<Schemas.PersonThree> producerBuilder = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName);

        Throwable t =
                expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));
    }

    @Test
    public void testDefaultConfig()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "testDefaultConfig"
        ).toString();

        getPulsar().getAdminClient().topics().createNonPartitionedTopic(topicName);

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        @Cleanup
        Producer<?> producer2 = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName).create();
    }
    @SuppressWarnings("deprecation")

    @Test
    public void testUpdateSchemaCompatibilityStrategyRepeatedly()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        assertEquals(getConfig().getSchemaCompatibilityStrategy(), SchemaCompatibilityStrategy.FULL);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                NamespaceName.get(getNamespace()),
                "testUpdateSchemaCompatibilityStrategyRepeatedly"
        ).toString();

        getPulsar().getAdminClient().topics().createNonPartitionedTopic(topicName);

        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.FULL));
        Awaitility.await().untilAsserted(
                () -> assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(getNamespace())));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                        SchemaCompatibilityStrategy.UNDEFINED));

        @Cleanup
        Producer<?> producer = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Set SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE to schema_auto_update_compatibility_strategy on
        // namespace level.
        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(getNamespace(),
                SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(getNamespace()),
                        SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                        SchemaCompatibilityStrategy.UNDEFINED));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));

        ProducerBuilder<Schemas.PersonThree> producerBuilder =
                pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().
                                withAlwaysAllowNull(true).withPojo(Schemas.PersonThree.class).build()))
                        .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));

        // Set SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE to schema_compatibility_strategy on namespace level.
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(getNamespace()),
                        SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
        @Cleanup
        Producer<?> producer2 = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Set SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE on topic level.
        admin.topicPolicies().setSchemaCompatibilityStrategy(topicName,
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));
        producerBuilder = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName);
        t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));

        // Remove schema compatibility strategy on topic level.
        admin.topicPolicies().removeSchemaCompatibilityStrategy(topicName);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
        @Cleanup
        Producer<?> producer3 = pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Remove schema_compatibility_strategy on namespace level.
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(), SchemaCompatibilityStrategy.UNDEFINED);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(getNamespace()),
                        SchemaCompatibilityStrategy.UNDEFINED));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));
        producerBuilder = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName);
        t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));
    }
}
