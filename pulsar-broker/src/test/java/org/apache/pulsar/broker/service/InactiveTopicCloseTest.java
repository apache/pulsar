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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InactiveTopicCloseTest extends BrokerTestBase {

    @BeforeMethod
    protected void setup() throws Exception {
        //No-op
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testCloseInactiveTopicKeepsDataAndEvictsFromCache() throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testCloseInactive";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        producer.send("hello".getBytes());
        consumer.close();
        producer.close();

        // Drop the only subscription so the topic qualifies as inactive in delete_when_no_subscriptions mode.
        admin.topics().deleteSubscription(topic, "sub");

        // Topic should be evicted from broker cache (closed) but still present in metadata.
        Awaitility.await().untilAsserted(() ->
                assertFalse(pulsar.getBrokerService().getTopicReference(topic).isPresent()));
        assertTrue(admin.topics().getList("prop/ns-abc").contains(topic));

        // Data is preserved: a fresh consumer on a new subscription reading from earliest must see the message.
        Consumer<byte[]> reReader = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub2")
                .subscriptionInitialPosition(org.apache.pulsar.client.api.SubscriptionInitialPosition.Earliest)
                .subscribe();
        org.apache.pulsar.client.api.Message<byte[]> msg = reReader.receive(10, java.util.concurrent.TimeUnit.SECONDS);
        org.testng.Assert.assertNotNull(msg);
        org.testng.Assert.assertEquals(new String(msg.getValue()), "hello");
        reReader.close();
    }

    @Test
    public void testMutualExclusionWithDeleteInactive() throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        try {
            super.baseSetup();
            org.testng.Assert.fail("expected broker startup to fail");
        } catch (Exception e) {
            Throwable cause = e;
            boolean found = false;
            while (cause != null) {
                if (cause instanceof IllegalArgumentException
                        && cause.getMessage() != null
                        && cause.getMessage().contains("mutually exclusive")) {
                    found = true;
                    break;
                }
                cause = cause.getCause();
            }
            assertTrue(found, "expected IllegalArgumentException about mutual exclusion, got: " + e);
        }
    }

    @Test
    public void testActiveTopicIsNotClosed() throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(1);
        super.baseSetup();

        final String topic = "persistent://prop/ns-abc/testActiveNotClosed";
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        try {
            // Wait past the inactivity window; topic must remain loaded because a subscription exists.
            Thread.sleep(3000);
            assertTrue(pulsar.getBrokerService().getTopicReference(topic).isPresent());
        } finally {
            consumer.close();
            producer.close();
        }
    }
}
