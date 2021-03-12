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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.schema.Schemas;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SchemaTypeCompatibilityCheckTest extends MockedPulsarServiceBaseTest {
    private final static String CLUSTER_NAME = "test";
    private final static String PUBLIC_TENANT = "public";
    private final static String namespace = "test-namespace";
    private final static String namespaceName = PUBLIC_TENANT + "/" + namespace;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(pulsar.getBrokerServiceUrl()));

        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Collections.singleton(CLUSTER_NAME));
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));

    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void structTypeProducerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerProducerUndefinedCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeProducerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerConsumerUndefinedCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeConsumerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerProducerUndefinedCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                    .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeConsumerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerConsumerUndefinedCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
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
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeProducerProducerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerProducerAlwaysCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .create();
    }

    @Test
    public void structTypeProducerConsumerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerConsumerAlwaysCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();
    }

    @Test
    public void structTypeConsumerProducerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerProducerAlwaysCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        pulsarClient.newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .create();
    }

    @Test
    public void structTypeConsumerConsumerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerConsumerAlwaysCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "1")
                .subscribe();

        pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2")
                .subscribe();
    }

    @Test
    public void primitiveTypeProducerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeProducerProducerUndefinedCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeProducerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeProducerConsumerUndefinedCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeConsumerProducerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeConsumerProducerUndefinedCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        ProducerBuilder producerBuilder = pulsarClient.newProducer(Schema.STRING)
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeConsumerConsumerUndefinedCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.UNDEFINED);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeConsumerConsumerUndefinedCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.INT32)
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
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveTypeProducerProducerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeProducerProducerAlwaysCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
    }

    @Test
    public void primitiveTypeProducerConsumerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeProducerConsumerAlwaysCompatible"
        ).toString();

        pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2")
                .subscribe();
    }

    @Test
    public void primitiveTypeConsumerProducerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeConsumerProducerAlwaysCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName)
                .subscribe();

        pulsarClient.newProducer(Schema.STRING)
                .topic(topicName)
                .create();
    }

    @Test
    public void primitiveTypeConsumerConsumerAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeConsumerConsumerAlwaysCompatible"
        ).toString();

        pulsarClient.newConsumer(Schema.INT32)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "1")
                .subscribe();

        pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2")
                .subscribe();
    }

}
