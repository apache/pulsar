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
package org.apache.pulsar.broker.service.persistent;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import lombok.Cleanup;

import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ReplicatedSubscriptionConfigTest extends ProducerConsumerBase {

    @Override
    @BeforeClass
    public void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void createReplicatedSubscription() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("createReplicatedSubscription");

        @Cleanup
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub1")
                .replicateSubscriptionState(true)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub1").isReplicated());

        admin.topics().unload(topic);

        // Check that subscription is still marked replicated after reloading
        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub1").isReplicated());
    }

    @Test
    public void upgradeToReplicatedSubscription() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("upgradeToReplicatedSubscription");

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }

    @Test
    public void upgradeToReplicatedSubscriptionAfterRestart() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(true);
        String topic = BrokerTestUtil.newUniqueName("upgradeToReplicatedSubscriptionAfterRestart");

        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(false)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();

        admin.topics().unload(topic);

        consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        stats = admin.topics().getStats(topic);
        assertTrue(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }

    @Test
    public void testDisableReplicatedSubscriptions() throws Exception {
        this.conf.setEnableReplicatedSubscriptions(false);
        String topic = BrokerTestUtil.newUniqueName("disableReplicatedSubscriptions");
        Consumer<String> consumer = pulsarClient.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("sub")
                .replicateSubscriptionState(true)
                .subscribe();

        TopicStats stats = admin.topics().getStats(topic);
        assertFalse(stats.getSubscriptions().get("sub").isReplicated());
        consumer.close();
    }
}
