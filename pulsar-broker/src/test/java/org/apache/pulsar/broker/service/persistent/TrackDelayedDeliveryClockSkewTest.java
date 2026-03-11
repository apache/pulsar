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
package org.apache.pulsar.broker.service.persistent;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTracker;
import org.apache.pulsar.broker.delayed.DelayedDeliveryTrackerFactory;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.plugin.EntryFilterProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TrackDelayedDeliveryClockSkewTest {

    private PersistentTopic topicMock;
    private ManagedCursorImpl cursorMock;
    private PersistentSubscription subscriptionMock;
    private DelayedDeliveryTracker trackerMock;
    private DelayedDeliveryTrackerFactory trackerFactoryMock;

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration configMock = mock(ServiceConfiguration.class);
        doReturn(true).when(configMock).isSubscriptionRedeliveryTrackerEnabled();
        doReturn(100).when(configMock).getDispatcherMaxReadBatchSize();
        doReturn(false).when(configMock).isDispatcherDispatchMessagesInSubscriptionThread();
        doReturn(false).when(configMock).isAllowOverrideEntryFilters();
        doReturn(10).when(configMock).getDispatcherRetryBackoffInitialTimeInMs();
        doReturn(50).when(configMock).getDispatcherRetryBackoffMaxTimeInMs();

        PulsarService pulsarMock = mock(PulsarService.class);
        doReturn(configMock).when(pulsarMock).getConfiguration();

        BrokerService brokerMock = mock(BrokerService.class);
        doReturn(pulsarMock).when(brokerMock).pulsar();

        EntryFilterProvider entryFilterProvider = mock(EntryFilterProvider.class);
        doReturn(Collections.emptyList()).when(entryFilterProvider).getBrokerEntryFilters();
        doReturn(entryFilterProvider).when(brokerMock).getEntryFilterProvider();

        OrderedExecutor orderedExecutor = mock(OrderedExecutor.class);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        doReturn(executorService).when(orderedExecutor).chooseThread();
        doReturn(orderedExecutor).when(brokerMock).getTopicOrderedExecutor();

        trackerMock = mock(DelayedDeliveryTracker.class);
        when(trackerMock.addMessage(anyLong(), anyLong(), anyLong())).thenReturn(true);

        trackerFactoryMock = mock(DelayedDeliveryTrackerFactory.class);
        when(trackerFactoryMock.newTracker(org.mockito.ArgumentMatchers.any())).thenReturn(trackerMock);
        doReturn(trackerFactoryMock).when(brokerMock).getDelayedDeliveryTrackerFactory();

        HierarchyTopicPolicies topicPolicies = new HierarchyTopicPolicies();
        topicPolicies.getMaxConsumersPerSubscription().updateBrokerValue(0);

        topicMock = mock(PersistentTopic.class);
        doReturn(brokerMock).when(topicMock).getBrokerService();
        doReturn("persistent://public/default/testTopic").when(topicMock).getName();
        doReturn(topicPolicies).when(topicMock).getHierarchyTopicPolicies();
        doReturn(true).when(topicMock).isDelayedDeliveryEnabled();
        doReturn(1000L).when(topicMock).getDelayedDeliveryTickTimeMillis();

        ManagedLedgerImpl ledgerMock = mock(ManagedLedgerImpl.class);
        cursorMock = mock(ManagedCursorImpl.class);
        doReturn("testSubscription").when(cursorMock).getName();
        doReturn(ledgerMock).when(cursorMock).getManagedLedger();

        subscriptionMock = mock(PersistentSubscription.class);
        when(subscriptionMock.getTopic()).thenReturn(topicMock);
    }

    /**
     * Simulate client clock is 5 minutes behind server.
     * Client uses deliverAfter(3min), so:
     *   publishTime = clientNow (serverNow - 5min)
     *   deliverAtTime = clientNow + 3min (serverNow - 2min)
     *   relativeDelay = 3min
     *
     * Without fix: broker uses deliverAtTime directly (serverNow - 2min), already passed, delivers immediately.
     * With fix: broker recalculates as serverNow + 3min.
     */
    @Test
    public void testDeliverAfterWithClientClockBehindServer() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        long serverNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L; // client is 5 minutes behind
        long clientNow = serverNow - clockSkew;
        long delayMs = 3 * 60 * 1000L; // 3 minutes delay

        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(clientNow);
        msgMetadata.setDeliverAtTime(clientNow + delayMs);

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        long capturedDeliverAt = deliverAtCaptor.getValue();
        // The recalculated time should be approximately serverNow + 3min
        // Allow 1 second tolerance for test execution time
        long expectedMin = serverNow + delayMs - 1000;
        long expectedMax = serverNow + delayMs + 1000;
        assertTrue(capturedDeliverAt >= expectedMin && capturedDeliverAt <= expectedMax,
                String.format("Expected deliverAtTime around %d, but got %d (diff: %d ms)",
                        serverNow + delayMs, capturedDeliverAt, capturedDeliverAt - (serverNow + delayMs)));
    }

    /**
     * Simulate client clock is 5 minutes ahead of server.
     * Client uses deliverAfter(3min), so:
     *   publishTime = clientNow (serverNow + 5min)
     *   deliverAtTime = clientNow + 3min (serverNow + 8min)
     *   relativeDelay = 3min
     *
     * Without fix: broker uses deliverAtTime directly (serverNow + 8min), delivers 5 minutes too late.
     * With fix: broker recalculates as serverNow + 3min.
     */
    @Test
    public void testDeliverAfterWithClientClockAheadOfServer() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        long serverNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L; // client is 5 minutes ahead
        long clientNow = serverNow + clockSkew;
        long delayMs = 3 * 60 * 1000L;

        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(clientNow);
        msgMetadata.setDeliverAtTime(clientNow + delayMs);

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        long capturedDeliverAt = deliverAtCaptor.getValue();
        long expectedMin = serverNow + delayMs - 1000;
        long expectedMax = serverNow + delayMs + 1000;
        assertTrue(capturedDeliverAt >= expectedMin && capturedDeliverAt <= expectedMax,
                String.format("Expected deliverAtTime around %d, but got %d (diff: %d ms)",
                        serverNow + delayMs, capturedDeliverAt, capturedDeliverAt - (serverNow + delayMs)));
    }

    /**
     * When publishTime is not present in metadata, fall back to using the original deliverAtTime.
     */
    @Test
    public void testFallbackWhenNoPublishTime() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        long deliverAt = System.currentTimeMillis() + 60_000;

        MessageMetadata msgMetadata = new MessageMetadata();
        // No publishTime set
        msgMetadata.setDeliverAtTime(deliverAt);

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        // Should use original deliverAtTime as-is
        assertTrue(deliverAtCaptor.getValue() == deliverAt,
                "When publishTime is missing, should use original deliverAtTime");
    }

    /**
     * When relativeDelay <= 0 (deliverAtTime <= publishTime, abnormal data),
     * fall back to using the original deliverAtTime.
     */
    @Test
    public void testFallbackWhenRelativeDelayNegative() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        long now = System.currentTimeMillis();

        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(now);
        msgMetadata.setDeliverAtTime(now - 1000); // deliverAtTime before publishTime

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        assertTrue(deliverAtCaptor.getValue() == now - 1000,
                "When relativeDelay <= 0, should use original deliverAtTime");
    }

    /**
     * When no deliverAtTime is set (normal non-delayed message), deliverAtTime should be -1.
     */
    @Test
    public void testNoDeliverAtTime() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        // First call with a delayed message to initialize the tracker
        MessageMetadata initMsg = new MessageMetadata();
        initMsg.setPublishTime(System.currentTimeMillis());
        initMsg.setDeliverAtTime(System.currentTimeMillis() + 60_000);
        dispatcher.trackDelayedDelivery(1, 1, initMsg);

        // Now call with a normal message (no deliverAtTime)
        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(System.currentTimeMillis());
        // No deliverAtTime set

        dispatcher.trackDelayedDelivery(1, 2, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock, org.mockito.Mockito.times(2)).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        // The second call should pass -1
        long secondDeliverAt = deliverAtCaptor.getAllValues().get(1);
        assertTrue(secondDeliverAt == -1L,
                "When no deliverAtTime is set, should pass -1");
    }

    /**
     * Test with synchronized clocks (no skew) - deliverAfter should work the same.
     */
    @Test
    public void testNoClockSkew() {
        PersistentDispatcherMultipleConsumers dispatcher =
                new PersistentDispatcherMultipleConsumers(topicMock, cursorMock, subscriptionMock);

        long serverNow = System.currentTimeMillis();
        long delayMs = 60_000;

        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(serverNow); // client clock matches server
        msgMetadata.setDeliverAtTime(serverNow + delayMs);

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        long capturedDeliverAt = deliverAtCaptor.getValue();
        long expectedMin = serverNow + delayMs - 1000;
        long expectedMax = serverNow + delayMs + 1000;
        assertTrue(capturedDeliverAt >= expectedMin && capturedDeliverAt <= expectedMax,
                "With no clock skew, recalculated time should match original");
    }

    /**
     * Test the Classic dispatcher variant has the same behavior.
     */
    @Test
    public void testClassicDispatcherClockSkewRecalculation() {
        PersistentDispatcherMultipleConsumersClassic dispatcher =
                new PersistentDispatcherMultipleConsumersClassic(topicMock, cursorMock, subscriptionMock);

        long serverNow = System.currentTimeMillis();
        long clockSkew = 5 * 60 * 1000L;
        long clientNow = serverNow - clockSkew;
        long delayMs = 3 * 60 * 1000L;

        MessageMetadata msgMetadata = new MessageMetadata();
        msgMetadata.setPublishTime(clientNow);
        msgMetadata.setDeliverAtTime(clientNow + delayMs);

        dispatcher.trackDelayedDelivery(1, 1, msgMetadata);

        ArgumentCaptor<Long> deliverAtCaptor = ArgumentCaptor.forClass(Long.class);
        verify(trackerMock).addMessage(anyLong(), anyLong(), deliverAtCaptor.capture());

        long capturedDeliverAt = deliverAtCaptor.getValue();
        long expectedMin = serverNow + delayMs - 1000;
        long expectedMax = serverNow + delayMs + 1000;
        assertTrue(capturedDeliverAt >= expectedMin && capturedDeliverAt <= expectedMax,
                String.format("Classic dispatcher: expected deliverAtTime around %d, but got %d",
                        serverNow + delayMs, capturedDeliverAt));
    }
}
