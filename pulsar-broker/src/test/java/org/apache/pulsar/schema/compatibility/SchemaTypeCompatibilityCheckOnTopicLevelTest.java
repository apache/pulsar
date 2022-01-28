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
package org.apache.pulsar.schema.compatibility;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.collect.Sets;
import java.util.Collections;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaAutoUpdateCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.schema.Schemas;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaTypeCompatibilityCheckOnTopicLevelTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER_NAME = "test";
    private static final String PUBLIC_TENANT = "public";
    private static final String namespace = "test-namespace";
    private static final String namespaceName = PUBLIC_TENANT + "/" + namespace;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);

        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress())
                .build());

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSetAlwaysInCompatibleStrategyOnTopicLevelAndCheckAlwaysInCompatible()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testSetAlwaysInCompatibleStrategyOnTopicLevelAndCheckAlwaysInCompatible"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);
        pulsar.getAdminClient().topicPolicies().setSchemaCompatibilityStrategy(topicName,
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        Awaitility.await()
                .untilAsserted(() -> assertEquals(
                        pulsar.getAdminClient().topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
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
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testSetAlwaysCompatibleOnNamespaceLevelAndCheckAlwaysInCompatible"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);
        pulsar.getAdminClient().topicPolicies().setSchemaCompatibilityStrategy(topicName,
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        Awaitility.await()
                .untilAsserted(() -> assertEquals(
                        pulsar.getAdminClient().topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE));

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
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
    public void testDisableTopicPoliciesAndSetAlwaysInCompatibleOnNamespaceLevel()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);

        admin.namespaces()
                .setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testDisableTopicPoliciesAndSetAlwaysInCompatibleOnNamespaceLevel"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
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
    public void testDisableTopicPoliciesWithDefaultConfig()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testDisableTopicPoliciesWithDefaultConfig"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName).create();
    }

    @Test
    public void testDefaultConfig()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testDefaultConfig"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                        withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName).create();
    }

    @Test
    public void testUpdateSchemaCompatibilityStrategyRepeatedly()
            throws PulsarClientException, PulsarServerException, PulsarAdminException {
        assertEquals(conf.getSchemaCompatibilityStrategy(), SchemaCompatibilityStrategy.FULL);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testUpdateSchemaCompatibilityStrategyRepeatedly"
        ).toString();

        pulsar.getAdminClient().topics().createNonPartitionedTopic(topicName);

        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.FULL));
        Awaitility.await().untilAsserted(
                () -> assertNull(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespaceName)));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName),
                        SchemaCompatibilityStrategy.UNDEFINED));

        pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Set SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE to schema_auto_update_compatibility_strategy on
        // namespace level.
        admin.namespaces().setSchemaAutoUpdateCompatibilityStrategy(namespaceName,
                SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespaceName),
                        SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName),
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
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaAutoUpdateCompatibilityStrategy(namespaceName),
                        SchemaAutoUpdateCompatibilityStrategy.AutoUpdateDisabled));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.topicPolicies().getSchemaCompatibilityStrategy(topicName, true),
                        SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE));
        pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Set SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE on topic level.
        admin.topicPolicies().setSchemaCompatibilityStrategy(topicName,SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
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
        pulsarClient.newProducer(
                        Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull(true)
                                .withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName).create();

        // Remove schema_compatibility_strategy on namespace level.
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);
        Awaitility.await().untilAsserted(
                () -> assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName),
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
