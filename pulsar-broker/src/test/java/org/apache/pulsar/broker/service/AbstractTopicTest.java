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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.service.nonpersistent.NonPersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class AbstractTopicTest extends BrokerTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractTopicTest.class);

    @DataProvider(name = "topicAndSubscription")
    public Object[][] topicAndSubscriptionProvider() {
        Supplier<Pair<AbstractTopic, AbstractSubscription>> persistentProvider =
                createProvider(TopicDomain.persistent, PersistentSubscription.class);
        Supplier<Pair<AbstractTopic, AbstractSubscription>> nonPersistentProvider =
                createProvider(TopicDomain.non_persistent, NonPersistentSubscription.class);
        return new Object[][]{{persistentProvider}, {nonPersistentProvider}};
    }

    private Supplier<Pair<AbstractTopic, AbstractSubscription>> createProvider(
            TopicDomain topicDomain, Class<? extends AbstractSubscription> subscriptionClass) {
        return () -> {
            String topicName = topicDomain.value() + "://public/default/topic-0";
            try {
                admin.topics().createNonPartitionedTopic(topicName);
            } catch (PulsarAdminException e) {
                throw new RuntimeException("Create Topic failed", e);
            }
            Optional<Topic> topicOpt = pulsar.getBrokerService()
                    .getTopicReference(topicName);
            if (topicOpt.isEmpty()) {
                throw new RuntimeException("Topic " + topicName + " not found");
            }
            Topic topic = spy(topicOpt.get());
            AbstractSubscription subscription = mock(subscriptionClass);
            doReturn(Map.of("subscription", subscription))
                    .when(topic).getSubscriptions();
            return Pair.of((AbstractTopic) topic, subscription);
        };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.setupDefaultTenantAndNamespace();
    }

    @AfterMethod
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(dataProvider = "topicAndSubscription")
    public void testGetMsgOutCounter(Supplier<Pair<AbstractTopic, AbstractSubscription>> provider) {
        Pair<AbstractTopic, AbstractSubscription> topicAndSubscription = provider.get();
        AbstractTopic topic = topicAndSubscription.getLeft();
        AbstractSubscription subscription = topicAndSubscription.getRight();
        topic.msgOutFromRemovedSubscriptions.add(1L);
        when(subscription.getMsgOutCounter()).thenReturn(2L);
        assertEquals(topic.getMsgOutCounter(), 3L);
    }

    @Test(dataProvider = "topicAndSubscription")
    public void testGetBytesOutCounter(Supplier<Pair<AbstractTopic, AbstractSubscription>> provider) {
        Pair<AbstractTopic, AbstractSubscription> topicAndSubscription = provider.get();
        AbstractTopic topic = topicAndSubscription.getLeft();
        AbstractSubscription subscription = topicAndSubscription.getRight();
        topic.bytesOutFromRemovedSubscriptions.add(1L);
        when(subscription.getBytesOutCounter()).thenReturn(2L);
        assertEquals(topic.getBytesOutCounter(), 3L);
    }

    @Test(dataProvider = "topicAndSubscription")
    public void testOverwriteOldProducerAfterTopicClosed(
            Supplier<Pair<AbstractTopic, AbstractSubscription>> provider) throws InterruptedException {
        Pair<AbstractTopic, AbstractSubscription> topicAndSubscription = provider.get();
        AbstractTopic topic = topicAndSubscription.getLeft();
        Producer oldProducer = spy(createProducer(topic));
        ServerCnx oldCnx = spy((ServerCnx) oldProducer.getCnx());
        doReturn(oldCnx).when(oldProducer).getCnx();

        // Add old producer
        topic.addProducer(oldProducer, new CompletableFuture<>()).join();

        CountDownLatch oldCnxCheckInvokedLatch = new CountDownLatch(1);
        CountDownLatch oldCnxCheckStartLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            CompletableFuture<Optional<Boolean>> future = new CompletableFuture<>();
            CompletableFuture.runAsync(() -> {
                try {
                    oldCnxCheckInvokedLatch.countDown();
                    oldCnxCheckStartLatch.await();
                } catch (InterruptedException e) {
                    future.completeExceptionally(e);
                }
                future.complete(Optional.of(false));
            });
            return future;
        }).when(oldCnx).checkConnectionLiveness();

        // Add new producer
        Producer newProducer = createProducer(topic);
        CompletableFuture<Optional<Long>> producerEpoch =
                topic.addProducer(newProducer, new CompletableFuture<>());

        // Wait until new producer entered `AbstractTopic#tryOverwriteOldProducer`
        oldCnxCheckInvokedLatch.await();

        topic.close(true);
        // Run pending tasks to remove old producer from topic.
        ((EmbeddedChannel) oldCnx.ctx().channel()).runPendingTasks();

        // Unblock ServerCnx#checkConnectionLiveness to resume `AbstractTopic#tryOverwriteOldProducer`
        oldCnxCheckStartLatch.countDown();

        // As topic is fenced, adding new producer should fail.
        try {
            producerEpoch.join();
            fail("TopicFencedException expected");
        } catch (CompletionException e) {
            LOG.error("Failed to add producer", e);
            assertEquals(e.getCause().getClass(), BrokerServiceException.TopicFencedException.class);
        }
    }

    private Producer createProducer(Topic topic) {
        ServerCnx serverCnx = new ServerCnx(pulsar);
        new EmbeddedChannel(serverCnx);
        assertNotNull(serverCnx.ctx());
        return new Producer(topic, serverCnx, 1, "prod-name",
                "app-0", false, null, SchemaVersion.Latest, 0, false,
                ProducerAccessMode.Shared, Optional.empty(), true);
    }
}
