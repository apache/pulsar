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

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.schema.compatibility.SchemaCompatibilityCheckTest.randomName;
import static org.junit.Assert.assertEquals;

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

    @AfterMethod
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
        assertEquals("Tom", personConsume.getName());
        assertEquals(1, personConsume.getId());

        producer.close();
        consumer.close();
    }
}
