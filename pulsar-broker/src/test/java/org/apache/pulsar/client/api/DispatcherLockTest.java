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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import static org.apache.pulsar.client.api.KeySharedPolicy.KeySharedPolicySticky;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.impl.ClientBuilderImpl;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.api.proto.CommandPing;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-api")
public class DispatcherLockTest extends ProducerConsumerBase {

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    /**
     * The method is used to verify that the Broker will not leave an orphan consumer in the scenario below:
     * 1. Register "consumer-1"
     *   - "consumer-1" will be maintained by the Subscription.
     *   - "consumer-1" will be maintained by the Dispatcher.
     * 2. The connection of "consumer-1" has something wrong. We call this connection "connection-1"
     * 3. Try to register "consumer-2"
     *   - "consumer-2" will be maintained by the Subscription. At this time, there are two consumers under this
     *     subscription.
     *   - This will trigger a connection check task for connection-1, we call this task "CheckConnectionLiveness".
     *     This task will be executed in another thread, which means it will release the lock `Synchronized(dispatcher)`
     *   - "consumer-2" was not maintained by the Dispatcher yet.
     * 4. "CheckConnectionLiveness" will kick out "consumer-1" after 5 seconds, then "consumer-2" will be maintained
     *    by the Dispatcher.
     * (Highlight) Race condition: if the connection of "consumer-2" went to a wrong state before step 4,
     *   "consumer-2" maintained by the Subscription and not maintained by the Dispatcher. Would the scenario below
     *   will happen?
     *   1. "connection-2" closed.
     *   2. Remove "consumer-2" from the Subscription.
     *   3. Try to remove "consumer-2" from the Dispatcher, but there are no consumers under this Dispatcher. To remove
     *      nothing.
     *   4. "CheckConnectionLiveness" is finished; put "consumer-2" into the Dispatcher.
     *   5. At this moment, the consumer's state of Subscription and Dispatcher are not consistent. There is an orphan
     *      consumer under the Dispatcher.
     */
    @Test
    public void testNoOrphanConsumerIfLostDispatcherLock() throws Exception {
        final String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscription = "s1";
        admin.topics().createNonPartitionedTopic(tpName);
        admin.topics().createSubscription(tpName, subscription, MessageId.earliest);
        List<Range> ranges = Collections.singletonList(new Range(0, 65535));
        KeySharedPolicySticky sharedPolicySticky = new KeySharedPolicySticky.KeySharedPolicySticky().ranges(ranges);
        final String consumerName1 = "c1";
        final String consumerName2 = "c2";

        // Create a client that injected logic: do not answer for the command Ping
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        PulsarClient skipHealthCheckClient = InjectedClientCnxClientBuilder.create(clientBuilder,
                (conf, eventLoopGroup) -> new ClientCnx(conf, eventLoopGroup) {
                    public void handlePing(CommandPing ping) {
                        // do not response anything.
                    }
                });
        PulsarClientImpl normalClient = (PulsarClientImpl) newPulsarClient(lookupUrl.toString(), 0);

        // 1. Register "consumer-1"
        skipHealthCheckClient.newConsumer().topic(tpName).subscriptionName(subscription)
                .consumerName(consumerName1).keySharedPolicy(sharedPolicySticky)
                .subscriptionType(SubscriptionType.Key_Shared).subscribe();
        // Wait for all commands of the consumer c1 have been handled. To avoid the Broker mark the connection is active
        // after it receive anything.
        Thread.sleep(1000);

        // Try to register "consumer-2"
        normalClient.newConsumer().topic(tpName).subscriptionName(subscription)
                .consumerName(consumerName2).keySharedPolicy(sharedPolicySticky)
                .subscriptionType(SubscriptionType.Key_Shared).subscribeAsync();
        // Wait for "consumer-2" maintained by the Subscription, and the task "CheckConnectionLiveness" is in-progress.
        Thread.sleep(1000);

        // Make a race condition: close "connection-2".
        normalClient.close();

        // Verify no orphan consumers were left under the Dispatcher.
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(tpName, false).join().get();
        List<org.apache.pulsar.broker.service.Consumer>
                consumers = persistentTopic.getSubscription(subscription).getDispatcher().getConsumers();
        Awaitility.await().untilAsserted(() -> {
            log.info("consumer size: {}", consumers.size());
            assertEquals(consumers.size(), 0);
        });
    }
}
