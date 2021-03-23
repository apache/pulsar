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
package org.apache.pulsar.schema;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.schema.compatibility.SchemaCompatibilityCheckTest.randomName;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.schema.SchemaRegistryServiceImpl;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "schema")
public class SchemaTest extends MockedPulsarServiceBaseTest {

    private final static String CLUSTER_NAME = "test";

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        // Setup namespaces
        admin.clusters().createCluster(CLUSTER_NAME, new ClusterData(pulsar.getBrokerServiceUrl()));
        TenantInfo tenantInfo = new TenantInfo();
        tenantInfo.setAllowedClusters(Collections.singleton(CLUSTER_NAME));
        admin.tenants().createTenant(PUBLIC_TENANT, tenantInfo);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testMultiTopicSetSchemaProvider() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicOne = "test-multi-version-schema-one";
        final String topicTwo = "test-multi-version-schema-two";
        final String fqtnOne = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicOne
        ).toString();

        final String fqtnTwo = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicTwo
        ).toString();


        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME)
        );

        admin.topics().createPartitionedTopic(fqtnOne, 3);
        admin.topics().createPartitionedTopic(fqtnTwo, 3);

        admin.schemas().createSchema(fqtnOne, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonOne>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonOne.class).build()).getSchemaInfo());

        admin.schemas().createSchema(fqtnOne, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        admin.schemas().createSchema(fqtnTwo, Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()).getSchemaInfo());

        Producer<Schemas.PersonTwo> producer = pulsarClient.newProducer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .topic(fqtnOne)
                .create();

        Schemas.PersonTwo personTwo = new Schemas.PersonTwo();
        personTwo.setId(1);
        personTwo.setName("Tom");


        Consumer<Schemas.PersonTwo> consumer = pulsarClient.newConsumer(Schema.AVRO(
                SchemaDefinition.<Schemas.PersonTwo>builder().withAlwaysAllowNull
                        (false).withSupportSchemaVersioning(true).
                        withPojo(Schemas.PersonTwo.class).build()))
                .subscriptionName("test")
                .topic(fqtnOne, fqtnTwo)
                .subscribe();

        producer.send(personTwo);

        Schemas.PersonTwo personConsume = consumer.receive().getValue();
        assertEquals(personConsume.getName(), "Tom");
        assertEquals(personConsume.getId(), 1);

        producer.close();
        consumer.close();
    }

    @Test
    public void testBytesSchemaDeserialize() throws Exception {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test-bytes-schema";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();

        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        admin.topics().createPartitionedTopic(topic, 2);
        admin.schemas().createSchema(topic, Schema.JSON(Schemas.BytesRecord.class).getSchemaInfo());

        Producer<Schemas.BytesRecord> producer = pulsarClient
                .newProducer(Schema.JSON(Schemas.BytesRecord.class))
                .topic(topic)
                .create();

        Schemas.BytesRecord bytesRecord = new Schemas.BytesRecord();
        bytesRecord.setId(1);
        bytesRecord.setName("Tom");
        bytesRecord.setAddress("test".getBytes());

        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .subscriptionName("test-sub")
                .topic(topic)
                .subscribe();

        Consumer<Schemas.BytesRecord> consumer1 = pulsarClient.newConsumer(Schema.JSON(Schemas.BytesRecord.class))
                .subscriptionName("test-sub1")
                .topic(topic)
                .subscribe();

        producer.send(bytesRecord);

        Message<GenericRecord> message = consumer.receive();
        Message<Schemas.BytesRecord> message1 = consumer1.receive();

        assertEquals(message.getValue().getField("address").getClass(),
                message1.getValue().getAddress().getClass());

        producer.close();
        consumer.close();
        consumer1.close();
    }

    @Test
    public void testIsUsingAvroSchemaParser() {
        for (SchemaType value : SchemaType.values()) {
            if (value == SchemaType.AVRO || value == SchemaType.JSON || value == SchemaType.PROTOBUF) {
                assertTrue(SchemaRegistryServiceImpl.isUsingAvroSchemaParser(value));
            } else {
                assertFalse(SchemaRegistryServiceImpl.isUsingAvroSchemaParser(value));
            }
        }
    }

    @Test
    public void testNullKeyValueProperty() throws PulsarAdminException, PulsarClientException {
        final String tenant = PUBLIC_TENANT;
        final String namespace = "test-namespace-" + randomName(16);
        final String topicName = "test";

        final String topic = TopicName.get(
                TopicDomain.persistent.value(),
                tenant,
                namespace,
                topicName).toString();
        admin.namespaces().createNamespace(
                tenant + "/" + namespace,
                Sets.newHashSet(CLUSTER_NAME));

        final Map<String, String> map = new HashMap<>();
        map.put("key", null);
        map.put(null, "value"); // null key is not allowed for JSON, it's only for test here
        Schema.INT32.getSchemaInfo().setProperties(map);

        final Consumer<Integer> consumer = pulsarClient.newConsumer(Schema.INT32).topic(topic)
                .subscriptionName("sub")
                .subscribe();
        consumer.close();
    }
}
