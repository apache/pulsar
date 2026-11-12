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
package org.apache.pulsar.client.impl;

import static org.apache.pulsar.client.impl.PatternTopicsConsumerImplTest.waitTopicListWatcherCreation;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.InjectedClientCnxClientBuilder;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandWatchTopicListSuccess;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Pattern consumer tests that create a topic right after subscribing, without waiting for the consumer's topic list
 * watcher, and turn the periodic topic recheck off, so they run on a new broker per method. The broker lists a
 * namespace's topics for a new topic list watcher before it registers the watcher's listener, so a topic created in
 * between never reaches such a consumer. On the shared broker, {@code testPreciseRegexpSubscribe(true, false)} created
 * its topic in that window in 3 of 50 runs and {@code testAutoSubscribePatterConsumerFromBrokerWatcher(false)} in 2 of
 * 51 runs. {@link PatternTopicsConsumerImplTest} has the tests that share one.
 */
@Test(groups = "broker-impl")
public class PatternTopicsConsumerImplOwnBrokerTest extends ProducerConsumerBase {
    private static final long testTimeout = 90000; // 1.5 min
    private final long ackTimeOutMillis = TimeUnit.SECONDS.toMillis(2);

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        // set isTcpLookup = true, to use BinaryProtoLookupService to get topics for a pattern.
        isTcpLookup = true;
        // enabled transaction, to test pattern consumers not subscribe to transaction system topic.
        conf.setTransactionCoordinatorEnabled(true);
        conf.setSubscriptionPatternMaxLength(10000);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "delayTypesOfWatchingTopics")
    public Object[][] delayTypesOfWatchingTopics(){
        return new Object[][]{
            {true},
            {false}
        };
    }

    @Test(timeOut = testTimeout, dataProvider = "delayTypesOfWatchingTopics")
    public void testAutoSubscribePatterConsumerFromBrokerWatcher(boolean delayWatchingTopics) throws Exception {
        final String key = "AutoSubscribePatternConsumer";
        final String subscriptionName = "my-ex-subscription-" + key;
        final Pattern pattern = Pattern.compile("persistent://my-property/my-ns/pattern-topic.*");

        PulsarClient client = null;
        if (delayWatchingTopics) {
            client = createDelayWatchTopicsClient();
        } else {
            client = pulsarClient;
        }

        Consumer<byte[]> consumer = client.newConsumer()
                .topicsPattern(pattern)
                // Disable automatic discovery.
                .patternAutoDiscoveryPeriod(1000)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();

        // 1. create partition
        String topicName = "persistent://my-property/my-ns/pattern-topic-1-" + key;
        TenantInfoImpl tenantInfo = createDefaultTenantInfo();
        admin.tenants().createTenant("prop", tenantInfo);
        admin.topics().createPartitionedTopic(topicName, 4);

        // 2. verify consumer get methods. There is no need to trigger discovery, because the broker will push the
        // changes to update(CommandWatchTopicUpdate).
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 4);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
        });

        // cleanup.
        consumer.close();
        admin.topics().deletePartitionedTopic(topicName);
        if (delayWatchingTopics) {
            client.close();
        }
    }

    @DataProvider(name = "regexpConsumerArgs")
    public Object[][] regexpConsumerArgs(){
        return new Object[][]{
                {true, true},
                {true, false},
                {false, true},
                {false, false}
        };
    }

    @Test(timeOut = testTimeout, dataProvider = "regexpConsumerArgs")
    public void testPreciseRegexpSubscribe(boolean partitioned, boolean createTopicAfterWatcherStarted)
            throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://public/default/tp");
        final String subscriptionName = "s1";
        final Pattern pattern = Pattern.compile(String.format("%s$", topicName));

        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topicsPattern(pattern)
                // Disable automatic discovery.
                .patternAutoDiscoveryPeriod(1000)
                .subscriptionName(subscriptionName)
                .subscriptionType(SubscriptionType.Shared)
                .ackTimeout(ackTimeOutMillis, TimeUnit.MILLISECONDS)
                .receiverQueueSize(4)
                .subscribe();
        if (createTopicAfterWatcherStarted) {
            waitTopicListWatcherCreation(consumer);
        }

        // 1. create topic.
        if (partitioned) {
            admin.topics().createPartitionedTopic(topicName, 1);
        } else {
            admin.topics().createNonPartitionedTopic(topicName);
        }

        // 2. verify consumer can subscribe the topic.
        assertSame(pattern.pattern(), ((PatternMultiTopicsConsumerImpl<?>) consumer).getPattern().inputPattern());
        Awaitility.await().untilAsserted(() -> {
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitions().size(), 1);
            assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getConsumers().size(), 1);
            if (partitioned) {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 1);
            } else {
                assertEquals(((PatternMultiTopicsConsumerImpl<?>) consumer).getPartitionedTopics().size(), 0);
            }
        });

        // cleanup.
        consumer.close();
        if (partitioned) {
            admin.topics().deletePartitionedTopic(topicName);
        } else {
            admin.topics().delete(topicName);
        }
    }

    private PulsarClient createDelayWatchTopicsClient() throws Exception {
        ClientBuilderImpl clientBuilder = (ClientBuilderImpl) PulsarClient.builder().serviceUrl(lookupUrl.toString());
        return InjectedClientCnxClientBuilder.create(clientBuilder,
            (conf, eventLoopGroup) -> new ClientCnx(InstrumentProvider.NOOP, conf, eventLoopGroup) {
                public CompletableFuture<CommandWatchTopicListSuccess> newWatchTopicList(
                        BaseCommand command, long requestId) {
                    // Inject 2 seconds delay when sending command New Watch Topics.
                    CompletableFuture<CommandWatchTopicListSuccess> res = new CompletableFuture<>();
                    new Thread(() -> {
                        sleepSeconds(2);
                        super.newWatchTopicList(command, requestId).whenComplete((v, ex) -> {
                            if (ex != null) {
                                res.completeExceptionally(ex);
                            } else {
                                res.complete(v);
                            }
                        });
                    }).start();
                    return res;
                }
            });
    }
}
