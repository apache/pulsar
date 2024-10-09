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
package org.apache.pulsar.broker.service;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InmemoryTopicPoliciesServiceServiceTest extends MockedPulsarServiceBaseTest {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setTopicPoliciesServiceClassName(InmemoryTopicPoliciesService.class.getName());
        conf.setSystemTopicEnabled(false); // verify topic policies don't rely on system topics
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    // Shadow replicator is created by the topic policies update, this test verifies the listener can be triggered
    @Test
    public void testShadowReplicator() throws Exception {
        final var sourceTopic = TopicName.get("test-shadow-replicator").toString();
        final var shadowTopic = sourceTopic + "-shadow";

        admin.topics().createNonPartitionedTopic(sourceTopic);
        admin.topics().createShadowTopic(shadowTopic, sourceTopic);
        admin.topics().setShadowTopics(sourceTopic, Lists.newArrayList(shadowTopic));

        @Cleanup final var producer = pulsarClient.newProducer(Schema.STRING).topic(sourceTopic).create();
        @Cleanup final var consumer = pulsarClient.newConsumer(Schema.STRING).topic(shadowTopic)
                .subscriptionName("sub").subscribe();
        producer.send("msg");
        final var msg = consumer.receive(5, TimeUnit.SECONDS);
        Assert.assertNotNull(msg);
        Assert.assertEquals(msg.getValue(), "msg");

        final var persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(sourceTopic).get()
                .orElseThrow();
        Assert.assertEquals(TopicPolicyTestUtils.getTopicPolicies(persistentTopic).getShadowTopics(), List.of(shadowTopic));
        Assert.assertEquals(persistentTopic.getShadowReplicators().size(), 1);
    }

    @Test
    public void testTopicPoliciesAdmin() throws Exception {
        final var topic = "test-topic-policies-admin";
        admin.topics().createNonPartitionedTopic(topic);

        Assert.assertNull(admin.topicPolicies().getCompactionThreshold(topic));
        admin.topicPolicies().setCompactionThreshold(topic, 1000);
        Assert.assertEquals(admin.topicPolicies().getCompactionThreshold(topic).intValue(), 1000);
        // Sleep here because "Directory not empty error" might occur if deleting the topic immediately
        Thread.sleep(1000);
        final var topicPoliciesService = (InmemoryTopicPoliciesService) pulsar.getTopicPoliciesService();
        Assert.assertTrue(topicPoliciesService.containsKey(TopicName.get(topic)));
        admin.topics().delete(topic);
        Assert.assertFalse(topicPoliciesService.containsKey(TopicName.get(topic)));
    }
}
