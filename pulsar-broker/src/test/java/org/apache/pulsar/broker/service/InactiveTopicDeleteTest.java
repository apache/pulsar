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
package org.apache.pulsar.broker.service;


import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Consumer;

import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InactiveTopicDeleteTest extends BrokerTestBase {

    protected void setup() throws Exception {
        // No-op
    }

    protected void cleanup() throws Exception {
        // No-op
    }

    @Test
    public void testDeleteWhenNoSubscriptions() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoSubscriptions";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("sub")
            .subscribe();

        consumer.close();
        producer.close();

        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        admin.topics().deleteSubscription(topic, "sub");
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
    }

    @Test
    public void testDeleteWhenNoBacklogs() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testDeleteWhenNoBacklogs";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
            .topic(topic)
            .subscriptionName("sub")
            .subscribe();

        for (int i = 0; i < 10; i++) {
            producer.send("Pulsar".getBytes());
        }

        consumer.close();
        producer.close();

        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        admin.topics().skipAllMessages(topic, "sub");
        Thread.sleep(2000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
    }

    @Test
    public void testMaxInactiveDuration() throws Exception {
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(5);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testMaxInactiveDuration";

        Producer<byte[]> producer = pulsarClient.newProducer()
            .topic(topic)
            .create();

        producer.close();
        Thread.sleep(2000);
        Assert.assertTrue(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        Thread.sleep(4000);
        Assert.assertFalse(admin.topics().getList("prop/ns-abc")
            .contains(topic));

        super.internalCleanup();
    }
}
