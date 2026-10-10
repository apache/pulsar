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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class InactiveTopicCloseTest extends BrokerTestBase {

    private static final String NAMESPACE = "prop/ns-abc";

    @BeforeMethod
    protected void setup() throws Exception {
        //No-op
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /** Enable close mode with the only supported delete mode and a 1s inactivity window. */
    private void setupCloseMode(InactiveTopicDeleteMode mode) throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(mode);
        conf.setBrokerDeleteInactiveTopicsFrequencySeconds(1);
        conf.setBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds(1);
        super.baseSetup();
    }

    private void assertStartupFailsWith(String expectedMessageFragment) throws Exception {
        try {
            super.baseSetup();
            fail("expected broker startup to fail with: " + expectedMessageFragment);
        } catch (Exception e) {
            Throwable cause = e;
            while (cause != null) {
                if (cause instanceof IllegalArgumentException
                        && cause.getMessage() != null
                        && cause.getMessage().contains(expectedMessageFragment)) {
                    return;
                }
                cause = cause.getCause();
            }
            fail("expected IllegalArgumentException containing '" + expectedMessageFragment + "', got: " + e);
        }
    }

    @Test
    public void testCloseInactiveTopicKeepsDataAndEvictsFromCache() throws Exception {
        setupCloseMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);

        final String topic = "persistent://" + NAMESPACE + "/testCloseInactive";

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
        assertTrue(admin.topics().getList(NAMESPACE).contains(topic));

        assertMessagePreserved(topic, "sub2", "hello");
    }

    @Test
    public void testMutualExclusionWithDeleteInactive() throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(true);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        assertStartupFailsWith("mutually exclusive");
    }

    /**
     * Close mode only supports delete_when_no_subscriptions: under delete_when_subscriptions_caught_up a topic
     * with connected but caught-up consumers is inactive, so closing it would unload and reload it forever.
     */
    @Test
    public void testCloseModeRejectsCaughtUpDeleteMode() throws Exception {
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerCloseInactiveTopicsEnabled(true);
        conf.setBrokerDeleteInactiveTopicsMode(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
        assertStartupFailsWith("only supports brokerDeleteInactiveTopicsMode");
    }

    @Test
    public void testActiveTopicIsNotClosed() throws Exception {
        setupCloseMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);

        final String topic = "persistent://" + NAMESPACE + "/testActiveNotClosed";
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

    /**
     * The broker-level close switch must win over a namespace-level deleteWhileInactive policy, otherwise
     * enabling close mode would still silently delete data in namespaces that set that policy.
     */
    @Test
    public void testCloseWinsOverNamespaceDeleteWhileInactivePolicy() throws Exception {
        setupCloseMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        admin.namespaces().setInactiveTopicPolicies(NAMESPACE, new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_no_subscriptions, 1, true));

        final String topic = "persistent://" + NAMESPACE + "/testCloseWinsOverDeletePolicy";

        Producer<byte[]> producer = pulsarClient.newProducer().topic(topic).create();
        producer.send("hello".getBytes());
        producer.close();

        // No subscription was ever created, so the topic is inactive right away.
        Awaitility.await().untilAsserted(() ->
                assertFalse(pulsar.getBrokerService().getTopicReference(topic).isPresent()));

        // Closed, not deleted: still in metadata and the data is still readable.
        assertTrue(admin.topics().getList(NAMESPACE).contains(topic),
                "topic was deleted despite brokerCloseInactiveTopicsEnabled");
        assertMessagePreserved(topic, "sub", "hello");
    }

    /**
     * Startup validation only covers the broker-level mode. A namespace policy can still switch the effective
     * mode to delete_when_subscriptions_caught_up at runtime, and checkGC must then skip the topic entirely
     * rather than close it out from under its connected consumers.
     */
    @Test
    public void testCloseSkippedWhenNamespacePolicyOverridesDeleteMode() throws Exception {
        setupCloseMode(InactiveTopicDeleteMode.delete_when_no_subscriptions);
        admin.namespaces().setInactiveTopicPolicies(NAMESPACE, new InactiveTopicPolicies(
                InactiveTopicDeleteMode.delete_when_subscriptions_caught_up, 1, false));

        final String topic = "persistent://" + NAMESPACE + "/testCaughtUpModeSkipsClose";

        // A consumer with no backlog and no producer is "inactive" under delete_when_subscriptions_caught_up.
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub")
                .subscribe();
        try {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).orElseThrow();
            // Precondition: the namespace policy really did override the mode, and under that mode the topic
            // looks inactive - i.e. without the guard in checkGC it would be closed out from under the consumer.
            assertEquals(persistentTopic.getInactiveTopicPolicies().getInactiveTopicDeleteMode(),
                    InactiveTopicDeleteMode.delete_when_subscriptions_caught_up);
            assertFalse(persistentTopic.isActive(InactiveTopicDeleteMode.delete_when_subscriptions_caught_up),
                    "precondition: topic must look inactive under the overridden mode");

            Thread.sleep(3000);
            // Identity, not mere presence: a closed topic is reloaded within milliseconds by the consumer
            // reconnecting, so "a topic is cached" would still hold in the unload/reload loop this guards
            // against. Only the very same instance proves checkGC left the topic alone.
            assertSame(pulsar.getBrokerService().getTopicReference(topic).orElseThrow(), persistentTopic,
                    "topic was closed and reloaded under an unsupported delete mode");
            assertTrue(admin.topics().getList(NAMESPACE).contains(topic));
        } finally {
            consumer.close();
        }
    }

    private void assertMessagePreserved(String topic, String subscription, String expected) throws Exception {
        Consumer<byte[]> reReader = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();
        try {
            Message<byte[]> msg = reReader.receive(10, TimeUnit.SECONDS);
            assertNotNull(msg);
            assertEquals(new String(msg.getValue()), expected);
        } finally {
            reReader.close();
        }
    }
}
