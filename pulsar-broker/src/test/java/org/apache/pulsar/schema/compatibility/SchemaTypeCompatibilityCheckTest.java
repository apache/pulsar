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
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.schema.Schemas;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.*;


public class SchemaTypeCompatibilityCheckTest extends MockedPulsarServiceBaseTest {
    private final static String CLUSTER_NAME = "test";
    private final static String PUBLIC_TENANT = "public";
    private final String namespace = "test-namespace";
    private final String namespaceName = PUBLIC_TENANT + "/" + namespace;

    @BeforeMethod
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

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void structTypeProducerProducer() throws Exception {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerProducer"
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
    public void structTypeProducerConsumer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeProducerConsumer"
        ).toString();

        pulsarClient.newProducer(Schema.JSON(Schemas.PersonOne.class))
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2");

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type JSON, new schema type AVRO"));
    }

    @Test
    public void structTypeConsumerProducer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerProducer"
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
    public void structTypeConsumerConsumer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeConsumerConsumer"
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
    public void structTypeAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "structTypeAlwaysCompatible"
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
    public void primitiveTypeProducerProducer() throws Exception {
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveTypeProducerProducer"
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
    public void primitiveProducerConsumer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveProducerConsumer"
        ).toString();

        pulsarClient.newProducer(Schema.INT32)
                .topic(topicName)
                .create();

        ConsumerBuilder consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
                .topic(topicName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscriptionName(subName + "2");

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, consumerBuilder::subscribe);
        assertTrue(t.getMessage().endsWith("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void primitiveConsumerProducer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveConsumerProducer"
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
    public void primitiveConsumerConsumer() throws Exception {
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveConsumerConsumer"
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
    public void primitiveAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        final String subName = "my-sub";
        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "primitiveAlwaysCompatible"
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
