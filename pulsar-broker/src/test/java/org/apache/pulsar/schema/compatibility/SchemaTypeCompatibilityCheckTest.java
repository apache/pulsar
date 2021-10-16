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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.schema.Schemas;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SchemaTypeCompatibilityCheckTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER_NAME = "test";
    private static final String PUBLIC_TENANT = "public";
    private static final String namespace = "test-namespace";
    private static final String namespaceName = PUBLIC_TENANT + "/" + namespace;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));

    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSchemaCompatibilityStrategyInBrokerLevel() throws PulsarClientException {
        conf.setSchemaCompatibilityStrategy(SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);

        String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
                "testSchemaCompatibilityStrategyInBrokerLevel"
        ).toString();

        pulsarClient.newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonOne>builder().
                withAlwaysAllowNull(true).withPojo(Schemas.PersonOne.class).build()))
                .topic(topicName)
                .create();

        ProducerBuilder<Schemas.PersonThree> producerBuilder = pulsarClient.newProducer(Schema.AVRO(SchemaDefinition
                .<Schemas.PersonThree>builder().withAlwaysAllowNull(true).withPojo(Schemas.PersonThree.class).build()))
                .topic(topicName);

        Throwable t = expectThrows(PulsarClientException.IncompatibleSchemaException.class, producerBuilder::create);
        assertTrue(t.getMessage().contains("org.apache.avro.SchemaValidationException: Unable to read schema"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type JSON, new schema type AVRO"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
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
        assertTrue(t.getMessage().contains("Incompatible schema: exists schema type INT32, new schema type STRING"));
    }

    @Test
    public void testAlwaysCompatible() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        final String topicName = TopicName.get(
                TopicDomain.persistent.value(),
                PUBLIC_TENANT,
                namespace,
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
