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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.schema.Schemas;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "schema")
public class SchemaCompatibilityCheckTest extends MockedPulsarServiceBaseTest {
    private static final String CLUSTER_NAME = "test";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfo tenantInfo = TenantInfo.builder()
                .allowedClusters(Collections.singleton(CLUSTER_NAME))
                .build();
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

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
                    Sets.newHashSet(CLUSTER_NAME)
            );

            admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
            admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
            admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                    .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

            Consumer<Schemas.PersonThree> consumerThree = pulsarClient.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonThree.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();

            Producer<Schemas.PersonOne> producerOne = pulsarClient
                    .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                    .topic(fqtn)
                    .create();


            Schemas.PersonOne personOne = new Schemas.PersonOne();
            personOne.setId(1);

            producerOne.send(personOne);
            Message<Schemas.PersonThree> message = null;

            try {
                message = consumerThree.receive();
                message.getValue();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof SchemaSerializationException);
                consumerThree.acknowledge(message);
            }

            Producer<Schemas.PersonTwo> producerTwo = pulsarClient
                    .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonTwo.class).build()))
                    .topic(fqtn)
                    .create();

            Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
            personTwo.setId(1);
            personTwo.setName("Jerry");
            producerTwo.send(personTwo);

            message = consumerThree.receive();
            Schemas.PersonThree personThree = message.getValue();
            consumerThree.acknowledge(message);

            assertEquals(personThree.getId(), 1);
            assertEquals(personThree.getName(), "Jerry");

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
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());
        try {
            pulsarClient.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonThree>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonThree.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();

        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to read schema"));
        }
    }

    @Test(dataProvider = "AllCheckSchemaCompatibilityStrategy")
    public void testBrokerAllowAutoUpdateSchemaDisabled(SchemaCompatibilityStrategy schemaCompatibilityStrategy)
            throws Exception {

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
                Sets.newHashSet(CLUSTER_NAME)
        );

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName.toString()),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());


        pulsar.getConfig().setAllowAutoUpdateSchemaEnabled(false);

        ProducerBuilder<Schemas.PersonTwo> producerThreeBuilder = pulsarClient
                .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                                (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .topic(fqtn);
        try {
            producerThreeBuilder.create();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Schema not found and schema auto updating is disabled."));
        }

        pulsar.getConfig().setAllowAutoUpdateSchemaEnabled(true);
        Policies policies = admin.namespaces().getPolicies(namespaceName.toString());
        Assert.assertTrue(policies.is_allow_auto_update_schema);

        ConsumerBuilder<Schemas.PersonTwo> comsumerBuilder = pulsarClient.newConsumer(Schema.AVRO(
                        SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                                        (false).withSupportSchemaVersioning(true).
                                withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test")
                .topic(fqtn);

        Producer<Schemas.PersonTwo> producer = producerThreeBuilder.create();
        Consumer<Schemas.PersonTwo> consumerTwo = comsumerBuilder.subscribe();

        producer.send(new Schemas.PersonTwo(2, "Lucy"));
        Message<Schemas.PersonTwo> message = consumerTwo.receive();

        Schemas.PersonTwo personTwo = message.getValue();
        consumerTwo.acknowledge(message);

        assertEquals(personTwo.getId(), 2);
        assertEquals(personTwo.getName(), "Lucy");

        producer.close();
        consumerTwo.close();

        pulsar.getConfig().setAllowAutoUpdateSchemaEnabled(false);

        producer = producerThreeBuilder.create();
        consumerTwo = comsumerBuilder.subscribe();

        producer.send(new Schemas.PersonTwo(2, "Lucy"));
        message = consumerTwo.receive();

        personTwo = message.getValue();
        consumerTwo.acknowledge(message);

        assertEquals(personTwo.getId(), 2);
        assertEquals(personTwo.getName(), "Lucy");

        consumerTwo.close();
        producer.close();
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
                Sets.newHashSet(CLUSTER_NAME)
        );

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName.toString()),
                SchemaCompatibilityStrategy.UNDEFINED);

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), false);
        ProducerBuilder<Schemas.PersonTwo> producerThreeBuilder = pulsarClient
                .newProducer(Schema.AVRO(SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .topic(fqtn);
        try {
            producerThreeBuilder.create();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Schema not found and schema auto updating is disabled."));
        }

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), true);
        ConsumerBuilder<Schemas.PersonTwo> comsumerBuilder = pulsarClient.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test")
                .topic(fqtn);

        Producer<Schemas.PersonTwo> producer = producerThreeBuilder.create();
        Consumer<Schemas.PersonTwo> consumerTwo = comsumerBuilder.subscribe();

        producer.send(new Schemas.PersonTwo(2, "Lucy"));
        Message<Schemas.PersonTwo> message = consumerTwo.receive();

        Schemas.PersonTwo personTwo = message.getValue();
        consumerTwo.acknowledge(message);

        assertEquals(personTwo.getId(), 2);
        assertEquals(personTwo.getName(), "Lucy");

        producer.close();
        consumerTwo.close();

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), false);

        producer = producerThreeBuilder.create();
        consumerTwo = comsumerBuilder.subscribe();

        producer.send(new Schemas.PersonTwo(2, "Lucy"));
        message = consumerTwo.receive();

        personTwo = message.getValue();
        consumerTwo.acknowledge(message);

        assertEquals(personTwo.getId(), 2);
        assertEquals(personTwo.getName(), "Lucy");

        consumerTwo.close();
        producer.close();
    }

    @Test
    public void testSchemaComparison() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "test-schema-comparison";

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
                Sets.newHashSet(CLUSTER_NAME)
        );

        assertEquals(admin.namespaces().getSchemaCompatibilityStrategy(namespaceName.toString()),
                SchemaCompatibilityStrategy.UNDEFINED);
        byte[] changeSchemaBytes = (new String(Schema.AVRO(Schemas.PersonOne.class)
                .getSchemaInfo().getSchema(), UTF_8) + "\n   \n   \n").getBytes();
        SchemaInfo schemaInfo = SchemaInfo.builder().type(SchemaType.AVRO).schema(changeSchemaBytes).build();
        admin.schemas().createSchema(fqtn, schemaInfo);

        admin.namespaces().setIsAllowAutoUpdateSchema(namespaceName.toString(), false);
        ProducerBuilder<Schemas.PersonOne> producerOneBuilder = pulsarClient
                .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(fqtn);
        producerOneBuilder.create().close();

        assertEquals(changeSchemaBytes, admin.schemas().getSchemaInfo(fqtn).getSchema());

        ProducerBuilder<Schemas.PersonThree> producerThreeBuilder = pulsarClient
                .newProducer(Schema.AVRO(Schemas.PersonThree.class))
                .topic(fqtn);

        try {
            producerThreeBuilder.create();
            fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Schema not found and schema auto updating is disabled."));
        }
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
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        Producer<Schemas.PersonOne> producerOne = pulsarClient
                .newProducer(Schema.AVRO(Schemas.PersonOne.class))
                .topic(fqtn)
                .create();


        Schemas.PersonOne personOne = new Schemas.PersonOne(10);

        Consumer<Schemas.PersonOne> consumerOne = pulsarClient.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonOne.class).build()))
                .subscriptionName("test")
                .topic(fqtn)
                .subscribe();

        producerOne.send(personOne);
        Message<Schemas.PersonOne> message = consumerOne.receive();
        personOne = message.getValue();

        assertEquals(personOne.getId(), 10);

        consumerOne.close();
        producerOne.close();
    }

    @Test
    public void testSchemaLedgerAutoRelease() throws Exception {
        String namespaceName = PUBLIC_TENANT + "/default";
        String topicName = "persistent://" + namespaceName + "/tp";
        admin.namespaces().createNamespace(namespaceName, Sets.newHashSet(CLUSTER_NAME));
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName, SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        // Update schema 100 times.
        for (int i = 0; i < 100; i++){
            Schema schema = Schema.JSON(SchemaDefinition.builder()
                    .withJsonDef(String.format("{\n"
                            + "    \"type\": \"record\",\n"
                            + "    \"name\": \"Test_Pojo\",\n"
                            + "    \"namespace\": \"org.apache.pulsar.schema.compatibility\",\n"
                            + "    \"fields\": [{\n"
                            + "        \"name\": \"prop_%s\",\n"
                            + "        \"type\": [\"null\", \"string\"],\n"
                            + "        \"default\": null\n"
                            + "    }]\n"
                            + "}", i))
                    .build());
            Producer producer = pulsarClient
                    .newProducer(schema)
                    .topic(topicName)
                    .create();
            producer.close();
        }
        // The other ledgers are about 5.
        Assert.assertTrue(mockBookKeeper.getLedgerMap().values().stream()
                .filter(ledger -> !ledger.isFenced())
                .collect(Collectors.toList()).size() < 20);
        admin.topics().delete(topicName, true);
    }

    @Test
    public void testAutoProduceSchemaAlwaysCompatible() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String topic = "topic" + randomName(16);

        String namespace = "test-namespace-" + randomName(16);
        String topicName = TopicName.get(
                TopicDomain.persistent.value(), tenant, namespace, topic).toString();
        NamespaceName namespaceName = NamespaceName.get(tenant, namespace);
        admin.namespaces().createNamespace(tenant + "/" + namespace, Sets.newHashSet(CLUSTER_NAME));

        // set ALWAYS_COMPATIBLE
        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);

        Producer producer = pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topicName).create();
        // should not fail
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING).subscriptionName("my-sub").topic(topicName).subscribe();

        producer.close();
        consumer.close();
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
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.namespaces().setSchemaCompatibilityStrategy(namespaceName.toString(), schemaCompatibilityStrategy);
        admin.schemas().createSchema(fqtn, Schema.AVRO(Schemas.PersonOne.class).getSchemaInfo());
        admin.schemas().createSchema(fqtn, Schema.AVRO(SchemaDefinition.builder()
                .withAlwaysAllowNull(false).withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        try {
            pulsarClient.newConsumer(Schema.AVRO(
                    SchemaDefinition.<Schemas.PersonFour>builder().withAlwaysAllowNull
                            (false).withSupportSchemaVersioning(true).
                            withPojo(Schemas.PersonFour.class).build()))
                    .subscriptionName("test")
                    .topic(fqtn)
                    .subscribe();
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Unable to read schema"));
        }

    }
    public static String randomName(int numChars) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numChars; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        return sb.toString();
    }
}
