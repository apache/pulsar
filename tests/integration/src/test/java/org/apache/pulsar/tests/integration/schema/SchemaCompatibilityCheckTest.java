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
package org.apache.pulsar.tests.integration.schema;

import com.google.common.collect.Sets;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.junit.Assert.assertEquals;

@Slf4j
public class SchemaCompatibilityCheckTest extends PulsarTestSuite {

    private PulsarClient client;
    private PulsarAdmin admin;

    @DataProvider(name = "CanReadLastSchemaCompatibilityStrategy")
    public Object[] canReadLastSchemaCompatibilityStrategy() {
        return new Object[] {
                SchemaCompatibilityStrategy.BACKWARD,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FORWARD,
                SchemaCompatibilityStrategy.FULL
        };
    }

    @DataProvider(name = "ReadAllCheckSchemaCompatibilityStrategy")
    public Object[] readAllCheckSchemaCompatibilityStrategy() {
        return new Object[] {
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FULL_TRANSITIVE
        };
    }

    @DataProvider(name = "AllCheckSchemaCompatibilityStrategy")
    public Object[] allCheckSchemaCompatibilityStrategy() {
        return new Object[] {
                SchemaCompatibilityStrategy.BACKWARD,
                SchemaCompatibilityStrategy.FORWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FORWARD,
                SchemaCompatibilityStrategy.FULL,
                SchemaCompatibilityStrategy.BACKWARD_TRANSITIVE,
                SchemaCompatibilityStrategy.FULL_TRANSITIVE
        };
    }

    @BeforeMethod
    public void setup() throws Exception {
        this.client = PulsarClient.builder()
                .serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .build();
        this.admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsarCluster.getHttpServiceUrl())
                .build();
    }

    @Test(dataProvider =  "CanReadLastSchemaCompatibilityStrategy")
    public void testConsumerCompatibilityCheckCanReadLastTest(SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-consumer-compatibility";

            String namespace = "test-namespace-" + randomName(16);
            String fqtn = TopicName.get(
                    TopicDomain.persistent.value(),
                    tenant,
                    namespace,
                    topic
            ).toString();

            NamespaceName namespaceName = NamespaceName.get(tenant, namespace);

            admin.namespaces().createNamespace(
                    tenant + "/" + namespace,
                    Sets.newHashSet(pulsarCluster.getClusterName())
            );

            admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
            admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
            admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                    .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

            Consumer<Schemas.PersonThree> consumerThree = client.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonThree.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();

            Producer<Schemas.PersonOne> producerOne = client
                    .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                    .topic(fqtn)
                    .create();


            Schemas.PersonOne personOne = new Schemas.PersonOne();
            personOne.id = 1;

            producerOne.send(personOne);
            Message<Schemas.PersonThree> message = null;

            try {
                message = consumerThree.receive();
                message.getValue();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SchemaSerializationException);
                consumerThree.acknowledge(message);
            }

            Producer<Schemas.PersonTwo> producerTwo = client
                    .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonTwo.class).build()))
                    .topic(fqtn)
                    .create();

            Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
            personTwo.id = 1;
            personTwo.name = "Jerry";
            producerTwo.send(personTwo);

            message = consumerThree.receive();
            Schemas.PersonThree personThree = message.getValue();
            consumerThree.acknowledge(message);

            assertEquals(personThree.id, 1);
            assertEquals(personThree.name, "Jerry");

            consumerThree.close();
            producerOne.close();
            producerTwo.close();
    }

    @Test(dataProvider = "ReadAllCheckSchemaCompatibilityStrategy")
    public void testConsumerCompatibilityReadAllCheckTest(SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-consumer-compatibility";
        String namespace = "test-namespace-" + randomName(16);
        String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(pulsarCluster.getClusterName())
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());
        Consumer<Schemas.PersonThree> consumerThree = null;
        try {
            consumerThree = client.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonThree.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();

        } catch (Exception e) {
            consumerThree.close();
            Assert.assertTrue(e.getMessage().contains("Unable to read schema"));
        }
    }

    @Test(dataProvider =  "AllCheckSchemaCompatibilityStrategy")
    public void testIsAutoUpdateSchema(SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-consumer-compatibility";

        String namespace = "test-namespace-" + randomName(16);
        String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(pulsarCluster.getClusterName())
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), false);
        ProducerBuilder<Schemas.PersonTwo> producerThreeBuilder = client
                .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .topic(fqtn);
        try {
            producerThreeBuilder.create();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Don't allow auto update schema."));
        }

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), true);

        Producer<Schemas.PersonTwo> producer = producerThreeBuilder.create();
        Consumer<Schemas.PersonTwo> consumerTwo = client.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test")
                .topic(fqtn)
                .subscribe();
        producer.send(new Schemas.PersonTwo(2, "Lucy"));
        Message<Schemas.PersonTwo> message = consumerTwo.receive();

        Schemas.PersonTwo personTwo = consumerTwo.receive().getValue();
        consumerTwo.acknowledge(message);

        assertEquals(personTwo.id, 1);
        assertEquals(personTwo.name, "Lucy");

        consumerTwo.close();
        producer.close();
    }

    @Test(dataProvider = "AllCheckSchemaCompatibilityStrategy")
    public void testProducerSendWithOldSchemaAndConsumerCanRead(SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-consumer-compatibility";
        String namespace = "test-namespace-" + randomName(16);
        String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(pulsarCluster.getClusterName())
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        Producer<Schemas.PersonOne> producerOne = client
                .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(fqtn)
                .create();


        Schemas.PersonOne personOne = new Schemas.PersonOne();
        personOne.id = 10;

        Consumer<Schemas.PersonOne> consumerTwo = client.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonOne.class).build()))
                .subscriptionName("test")
                .topic(fqtn)
                .subscribe();

        producerOne.send(personOne);

        Message<Schemas.PersonOne> message = consumerTwo.receive();

        personOne = message.getValue();

        assertEquals(10, personOne.id);

        consumerTwo.close();
        producerOne.close();

    }

    @Test(dataProvider =  "CanReadLastSchemaCompatibilityStrategy")
    public void testConsumerWithNotCompatibilitySchema(SchemaCompatibilityStrategy schemaCompatibilityStrategy) throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-consumer-compatibility";

        String namespace = "test-namespace-" + randomName(16);
        String fqtn = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topic
        ).toString();

        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(pulsarCluster.getClusterName())
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        Consumer<Schemas.PersonFour> consumerFour = null;
        try {
            consumerFour = client.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonFour>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonFour.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();
        } catch (Exception e) {
            consumerFour.close();
            Assert.assertTrue(e.getMessage().contains("Unable to read schema"));
        }

    }
}
