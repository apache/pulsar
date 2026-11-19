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

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.collect.Sets;
import java.util.Collections;
import org.apache.pulsar.broker.BrokerTestUtil;
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
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.schema.Schemas;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Schema compatibility tests that disable the broker's topic level policies, so they run on a broker of their own,
 * each in a namespace of its own. {@link SchemaTypeCompatibilityCheckOnTopicLevelTest} has the tests that share one.
 */
public class SchemaTypeCompatibilityCheckOnTopicLevelOwnBrokerTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER_NAME = "test";
    private static final String PUBLIC_TENANT = "public";
    private String namespace;
    private String namespaceName;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress())
                .build());

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
    }

    @BeforeMethod
    public void createNamespace() throws Exception {
        namespace = BrokerTestUtil.newUniqueName("test-namespace");
        namespaceName = PUBLIC_TENANT + "/" + namespace;
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
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
}
