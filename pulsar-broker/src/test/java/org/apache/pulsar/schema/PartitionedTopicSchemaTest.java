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
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test get partitioned topic schema.
 */
@Test(groups = "schema")
public class PartitionedTopicSchemaTest extends MockedPulsarServiceBaseTest {

    private static final String PARTITIONED_TOPIC = "public/default/partitioned-schema-topic";
    private static final int MESSAGE_COUNT_PER_PARTITION  = 12;
    private static final int TOPIC_PARTITION = 3;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        isTcpLookup = true;
        super.internalSetup();

        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("my-property/my-ns");
        admin.namespaces().setNamespaceReplicationClusters("my-property/my-ns", Sets.newHashSet("test"));

        // so that clients can test short names
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
        admin.topics().createPartitionedTopic(PARTITIONED_TOPIC, TOPIC_PARTITION);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void test() throws Exception {
        Consumer<GenericRecord> consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(PARTITIONED_TOPIC)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();
        consumer.close();

        @Cleanup
        Producer<Schemas.PersonFour> producer = pulsarClient.newProducer(Schema.JSON(Schemas.PersonFour.class))
                .topic(PARTITIONED_TOPIC)
                .enableBatching(false)
                .roundRobinRouterBatchingPartitionSwitchFrequency(1)
                .create();

        for (int i = 0; i < MESSAGE_COUNT_PER_PARTITION * TOPIC_PARTITION; i++) {
            Schemas.PersonFour person = new Schemas.PersonFour();
            person.setId(i);
            person.setName("user-" + i);
            person.setAge(18);
            producer.newMessage().value(person).send();
        }

        consumer = pulsarClient.newConsumer(Schema.AUTO_CONSUME())
                .topic(TopicName.get(PARTITIONED_TOPIC).getPartition(1).toString())
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("test")
                .subscribe();

        int receiveMsgCount = 0;
        for (int i = 0; i < MESSAGE_COUNT_PER_PARTITION; i++) {
            Message<GenericRecord> message = consumer.receive();
            Assert.assertNotNull(message);
            receiveMsgCount++;
        }
        Assert.assertEquals(MESSAGE_COUNT_PER_PARTITION, receiveMsgCount);
    }

}
