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
package org.apache.pulsar.broker.loadbalance.extensions.channel;

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Deleted;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Init;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Releasing;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Assign;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Split;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Unload;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MAX_CLEAN_UP_DELAY_TIME_IN_SECS;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData.state;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.ConnectionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.Reconnected;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.expectThrows;
import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistryImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
@SuppressWarnings("unchecked")
public class ServiceUnitStateChannelTest extends MockedPulsarServiceBaseTest {

    private PulsarService pulsar1;
    private PulsarService pulsar2;
    private ServiceUnitStateChannel channel1;
    private ServiceUnitStateChannel channel2;
    private String brokerId1;
    private String brokerId2;
    private String bundle;
    private String bundle1;
    private String bundle2;
    private String bundle3;
    private String childBundle1Range;
    private String childBundle2Range;
    private String childBundle11;
    private String childBundle12;

    private String childBundle31;
    private String childBundle32;
    private PulsarTestContext additionalPulsarTestContext;

    private LoadManagerContext loadManagerContext;

    private BrokerRegistryImpl registry;

    private ExtensibleLoadManagerImpl loadManager;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setLoadBalancerDebugModeEnabled(true);
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(10);
        super.internalSetup(conf);

        admin.tenants().createTenant("pulsar", createDefaultTenantInfo());
        admin.namespaces().createNamespace("pulsar/system");
        admin.tenants().createTenant("public", createDefaultTenantInfo());
        admin.namespaces().createNamespace("public/default");

        pulsar1 = pulsar;
        registry = new BrokerRegistryImpl(pulsar);
        loadManagerContext = mock(LoadManagerContext.class);
        doReturn(mock(LoadDataStore.class)).when(loadManagerContext).brokerLoadDataStore();
        doReturn(mock(LoadDataStore.class)).when(loadManagerContext).topBundleLoadDataStore();
        loadManager = mock(ExtensibleLoadManagerImpl.class);
        additionalPulsarTestContext = createAdditionalPulsarTestContext(getDefaultConf());
        pulsar2 = additionalPulsarTestContext.getPulsarService();

        channel1 = createChannel(pulsar1);
        channel1.start();

        channel2 = createChannel(pulsar2);
        channel2.start();
        brokerId1 = (String)
                FieldUtils.readDeclaredField(channel1, "brokerId", true);
        brokerId2 = (String)
                FieldUtils.readDeclaredField(channel2, "brokerId", true);

        bundle = "public/default/0x00000000_0xffffffff";
        bundle1 = "public/default/0x00000000_0xfffffff0";
        bundle2 = "public/default/0xfffffff0_0xffffffff";
        bundle3 = "public/default3/0x00000000_0xffffffff";
        childBundle1Range = "0x7fffffff_0xffffffff";
        childBundle2Range = "0x00000000_0x7fffffff";

        childBundle11 = "public/default/" + childBundle1Range;
        childBundle12 = "public/default/" + childBundle2Range;

        childBundle31 = "public/default3/" + childBundle1Range;
        childBundle32 = "public/default3/" + childBundle2Range;
    }

    @BeforeMethod
    protected void initChannels() throws Exception {
        cleanTableViews();
        cleanOwnershipMonitorCounters(channel1);
        cleanOwnershipMonitorCounters(channel2);
        cleanOpsCounters(channel1);
        cleanOpsCounters(channel2);
        cleanMetadataState(channel1);
        cleanMetadataState(channel2);
    }


    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        channel1.close();
        channel2.close();
        if (additionalPulsarTestContext != null) {
            additionalPulsarTestContext.close();
            additionalPulsarTestContext = null;
        }
        pulsar1 = null;
        pulsar2 = null;
        super.internalCleanup();
    }

    @Test(priority = -1)
    public void channelOwnerTest() throws Exception {
        var channelOwner1 = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        var channelOwner2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(channelOwner1, channelOwner2);
        LeaderElectionService leaderElectionService1 = (LeaderElectionService) FieldUtils.readDeclaredField(
                channel1, "leaderElectionService", true);
        leaderElectionService1.close();
        waitUntilNewChannelOwner(channel2, channelOwner1);
        leaderElectionService1.start();
        waitUntilNewChannelOwner(channel1, channelOwner1);

        var newChannelOwner1 = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS);
        var newChannelOwner2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS);

        assertEquals(newChannelOwner1, newChannelOwner2);
        assertNotEquals(channelOwner1, newChannelOwner1);

        if (newChannelOwner1.equals(Optional.of(brokerId1))) {
            assertTrue(channel1.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
            assertFalse(channel2.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
        } else {
            assertFalse(channel1.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
            assertTrue(channel2.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
        }
    }

    @Test(priority = 0)
    public void channelValidationTest()
            throws ExecutionException, InterruptedException, IllegalAccessException, PulsarServerException,
            TimeoutException {
        var channel = createChannel(pulsar);
        int errorCnt = validateChannelStart(channel);
        assertEquals(6, errorCnt);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future startFuture = executor.submit(() -> {
            try {
                channel.start();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
        });
        errorCnt = validateChannelStart(channel);
        startFuture.get();
        assertTrue(errorCnt > 0);

        FieldUtils.writeDeclaredField(channel, "channelState",
                ServiceUnitStateChannelImpl.ChannelState.LeaderElectionServiceStarted, true);
        assertNotNull(channel.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get());

        Future closeFuture = executor.submit(()->{
            try {
                channel.close();
            } catch (PulsarServerException e) {
                throw new RuntimeException(e);
            }
        });
        errorCnt = validateChannelStart(channel);
        closeFuture.get();
        assertTrue(errorCnt > 0);
        errorCnt = validateChannelStart(channel);
        assertEquals(6, errorCnt);

        // check if we can close() again
        channel.close();
        errorCnt = validateChannelStart(channel);
        assertEquals(6, errorCnt);

        // close() -> start() test.
        channel.start();
        assertNotNull(channel.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get());

        // start() -> start() test.
        assertThrows(IllegalStateException.class, () -> channel.start());
        channel.close(); // cleanup
    }

    private int validateChannelStart(ServiceUnitStateChannelImpl channel)
            throws InterruptedException, TimeoutException {
        int errorCnt = 0;
        try {
            channel.isChannelOwnerAsync().get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if(e.getCause() instanceof IllegalStateException){
                errorCnt++;
            }
        }
        try {
            channel.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            channel.getOwnerAsync(bundle).get(2, TimeUnit.SECONDS).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            channel.publishAssignEventAsync(bundle, brokerId1).get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            channel.publishUnloadEventAsync(
                    new Unload(brokerId1, bundle, Optional.of(brokerId2)))
                    .get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            Split split = new Split(bundle, brokerId1, Map.of(
                    childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
            channel.publishSplitEventAsync(split)
                    .get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        return errorCnt;
    }

    @Test(priority = 1)
    public void compactionScheduleTest() {

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    try {
                        var threshold = admin.topicPolicies()
                                .getCompactionThreshold(ServiceUnitStateChannelImpl.TOPIC, false).longValue();
                        assertEquals(5 * 1024 * 1024, threshold);
                    } catch (Exception e) {
                        ;
                    }
                });
    }

    @Test(priority = 2)
    public void assignmentTest()
            throws ExecutionException, InterruptedException, IllegalAccessException, TimeoutException {
        var getOwnerRequests1 = spy(getOwnerRequests((channel1)));
        var getOwnerRequests2 = spy(getOwnerRequests((channel2)));

        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        var assigned1 = channel1.publishAssignEventAsync(bundle, brokerId1);
        var assigned2 = channel2.publishAssignEventAsync(bundle, brokerId2);
        assertNotNull(assigned1);
        assertNotNull(assigned2);
        waitUntilOwnerChanges(channel1, bundle, null);
        waitUntilOwnerChanges(channel2, bundle, null);
        String assignedAddr1 = assigned1.get(5, TimeUnit.SECONDS);
        String assignedAddr2 = assigned2.get(5, TimeUnit.SECONDS);

        assertEquals(assignedAddr1, assignedAddr2);
        assertTrue(assignedAddr1.equals(brokerId1)
                || assignedAddr1.equals(brokerId2), assignedAddr1);

        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(getOwnerRequests1.size(), 0);
        assertEquals(getOwnerRequests2.size(), 0);

        validateHandlerCounters(channel1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateHandlerCounters(channel2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateEventCounters(channel1, 1, 0, 0, 0, 0, 0);
        validateEventCounters(channel2, 1, 0, 0, 0, 0, 0);
    }

    @Test(priority = 3)
    public void assignmentTestWhenOneAssignmentFails()
            throws ExecutionException, InterruptedException, IllegalAccessException, TimeoutException {
        var getOwnerRequests1 = getOwnerRequests(channel1);
        var getOwnerRequests2 = getOwnerRequests(channel2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        var producer = (Producer<ServiceUnitStateData>) FieldUtils.readDeclaredField(channel1,
                "producer", true);
        var spyProducer = spy(producer);
        var msg = mock(TypedMessageBuilder.class);
        var future = spy(CompletableFuture.failedFuture(new RuntimeException()));
        doReturn(msg).when(spyProducer).newMessage();
        doReturn(msg).when(msg).key(any());
        doReturn(msg).when(msg).value(any());
        doReturn(future).when(msg).sendAsync();

        FieldUtils.writeDeclaredField(channel1, "producer", spyProducer, true);

        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        var owner3 = channel1.publishAssignEventAsync(bundle, brokerId1);
        var owner4 = channel2.publishAssignEventAsync(bundle, brokerId2);
        assertTrue(owner3.isCompletedExceptionally());
        assertNotNull(owner4);
        String ownerAddrOpt2 = owner4.get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddrOpt2, brokerId2);
        waitUntilNewOwner(channel1, bundle, brokerId2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        FieldUtils.writeDeclaredField(channel1, "producer", producer, true);
    }

    @Test(priority = 4)
    public void transferTest()
            throws ExecutionException, InterruptedException, TimeoutException, IllegalAccessException {

        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());


        channel1.publishAssignEventAsync(bundle, brokerId1);
        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId1));

        Unload unload = new Unload(brokerId1, bundle, Optional.of(brokerId2));
        channel1.publishUnloadEventAsync(unload);

        waitUntilNewOwner(channel1, bundle, brokerId2);
        waitUntilNewOwner(channel2, bundle, brokerId2);

        ownerAddr1 = channel1.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        ownerAddr2 = channel2.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId2));

        validateHandlerCounters(channel1, 2, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateHandlerCounters(channel2, 2, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateEventCounters(channel1, 1, 0, 0, 0, 1, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
    }

    @Test(priority = 5)
    public void transferTestWhenDestBrokerFails()
            throws ExecutionException, InterruptedException, IllegalAccessException {

        var getOwnerRequests1 = getOwnerRequests(channel1);
        var getOwnerRequests2 = getOwnerRequests(channel2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        channel1.publishAssignEventAsync(bundle, brokerId1);
        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId1));

        var producer = (Producer<ServiceUnitStateData>) FieldUtils.readDeclaredField(channel1,
                "producer", true);
        var spyProducer = spy(producer);
        var msg = mock(TypedMessageBuilder.class);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(msg).when(spyProducer).newMessage();
        doReturn(msg).when(msg).key(any());
        doReturn(msg).when(msg).value(any());
        doReturn(future).when(msg).sendAsync();
        FieldUtils.writeDeclaredField(channel2, "producer", spyProducer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        Unload unload = new Unload(brokerId1, bundle, Optional.of(brokerId2));
        channel1.publishUnloadEventAsync(unload);
        // channel1 is broken. the ownership transfer won't be complete.
        waitUntilState(channel1, bundle);
        waitUntilState(channel2, bundle);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertFalse(owner1.isDone());
        assertFalse(owner2.isDone());

        assertEquals(1, getOwnerRequests1.size());
        assertEquals(1, getOwnerRequests2.size());

        // In 10 secs, the getOwnerAsync requests(lookup requests) should time out.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner1.isCompletedExceptionally()));
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner2.isCompletedExceptionally()));

        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        // recovered, check the monitor update state : Assigned -> Owned
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId1)))
                .when(loadManager).selectAsync(any(), any());
        FieldUtils.writeDeclaredField(channel2, "producer", producer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);

        ((ServiceUnitStateChannelImpl) channel1).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        ((ServiceUnitStateChannelImpl) channel2).monitorOwnerships(
                List.of(brokerId1, brokerId2));


        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId1));

        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        validateMonitorCounters(leader,
                0,
                0,
                1,
                0,
                0,
                0,
                0);

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);

    }

    @Test(priority = 6)
    public void splitAndRetryTest() throws Exception {
        channel1.publishAssignEventAsync(bundle, brokerId1);
        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();
        assertEquals(ownerAddr1, Optional.of(brokerId1));
        assertEquals(ownerAddr2, Optional.of(brokerId1));
        assertTrue(ownerAddr1.isPresent());

        NamespaceService namespaceService = pulsar1.getNamespaceService();
        CompletableFuture<Void> future = new CompletableFuture<>();
        int badVersionExceptionCount = 3;
        AtomicInteger count = new AtomicInteger(badVersionExceptionCount);
        future.completeExceptionally(new MetadataStoreException.BadVersionException("BadVersion"));
        doAnswer(invocationOnMock -> {
            if (count.decrementAndGet() > 0) {
                return future;
            }
            // Call the real method
            reset(namespaceService);
            doReturn(CompletableFuture.completedFuture(List.of("test-topic-1", "test-topic-2")))
                    .when(namespaceService).getOwnedTopicListForNamespaceBundle(any());
            return future;
        }).when(namespaceService).updateNamespaceBundles(any(), any());
        doReturn(namespaceService).when(pulsar1).getNamespaceService();
        doReturn(CompletableFuture.completedFuture(List.of("test-topic-1", "test-topic-2")))
                .when(namespaceService).getOwnedTopicListForNamespaceBundle(any());

        // Assert child bundle ownerships in the channels.

        Split split = new Split(bundle, ownerAddr1.get(), Map.of(
                childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
        channel1.publishSplitEventAsync(split);

        waitUntilState(channel1, bundle, Deleted);
        waitUntilState(channel2, bundle, Deleted);

        validateHandlerCounters(channel1, 1, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
        validateHandlerCounters(channel2, 1, 0, 3, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0);
        validateEventCounters(channel1, 1, 0, 1, 0, 0, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
        // Verify the retry count
        verify(((ServiceUnitStateChannelImpl) channel1), times(badVersionExceptionCount + 1))
                .splitServiceUnitOnceAndRetry(any(), any(), any(), any(), any(), any(), any(), any(), anyLong(), any());



        waitUntilNewOwner(channel1, childBundle11, brokerId1);
        waitUntilNewOwner(channel1, childBundle12, brokerId1);
        waitUntilNewOwner(channel2, childBundle11, brokerId1);
        waitUntilNewOwner(channel2, childBundle12, brokerId1);
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle11).get());
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle12).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle11).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle12).get());


        // try the monitor and check the monitor moves `Deleted` -> `Init`
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 1, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 1, true);

        ((ServiceUnitStateChannelImpl) channel1).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        ((ServiceUnitStateChannelImpl) channel2).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        waitUntilState(channel1, bundle, Init);
        waitUntilState(channel2, bundle, Init);

        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        validateMonitorCounters(leader,
                0,
                1,
                0,
                0,
                0,
                0,
                0);

        cleanTableView(channel1, childBundle11);
        cleanTableView(channel2, childBundle11);
        cleanTableView(channel1, childBundle12);
        cleanTableView(channel2, childBundle12);

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 300 * 1000, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 300 * 1000, true);
    }

    @Test(priority = 7)
    public void handleMetadataSessionEventTest() throws IllegalAccessException {
        var ts = System.currentTimeMillis();
        channel1.handleMetadataSessionEvent(SessionReestablished);
        var lastMetadataSessionEvent = getLastMetadataSessionEvent(channel1);
        var lastMetadataSessionEventTimestamp = getLastMetadataSessionEventTimestamp(channel1);

        assertEquals(SessionReestablished, lastMetadataSessionEvent);
        assertThat(lastMetadataSessionEventTimestamp,
                greaterThanOrEqualTo(ts));

        ts = System.currentTimeMillis();
        channel1.handleMetadataSessionEvent(SessionLost);
        lastMetadataSessionEvent = getLastMetadataSessionEvent(channel1);
        lastMetadataSessionEventTimestamp = getLastMetadataSessionEventTimestamp(channel1);

        assertEquals(SessionLost, lastMetadataSessionEvent);
        assertThat(lastMetadataSessionEventTimestamp,
                greaterThanOrEqualTo(ts));

        ts = System.currentTimeMillis();
        channel1.handleMetadataSessionEvent(ConnectionLost);
        lastMetadataSessionEvent = getLastMetadataSessionEvent(channel1);
        lastMetadataSessionEventTimestamp = getLastMetadataSessionEventTimestamp(channel1);

        assertEquals(SessionLost, lastMetadataSessionEvent);
        assertThat(lastMetadataSessionEventTimestamp,
                lessThanOrEqualTo(ts));

        ts = System.currentTimeMillis();
        channel1.handleMetadataSessionEvent(Reconnected);
        lastMetadataSessionEvent = getLastMetadataSessionEvent(channel1);
        lastMetadataSessionEventTimestamp = getLastMetadataSessionEventTimestamp(channel1);

        assertEquals(SessionLost, lastMetadataSessionEvent);
        assertThat(lastMetadataSessionEventTimestamp,
                lessThanOrEqualTo(ts));

    }

    @Test(priority = 8)
    public void handleBrokerCreationEventTest() throws IllegalAccessException {
        var cleanupJobs = getCleanupJobs(channel1);
        String broker = "broker-1";
        var future = new CompletableFuture();
        cleanupJobs.put(broker, future);
        channel1.handleBrokerRegistrationEvent(broker, NotificationType.Created);
        assertEquals(0, cleanupJobs.size());
        assertTrue(future.isCancelled());
    }

    @Test(priority = 9)
    public void handleBrokerDeletionEventTest()
            throws IllegalAccessException, ExecutionException, InterruptedException, TimeoutException {

        var cleanupJobs1 = getCleanupJobs(channel1);
        var cleanupJobs2 = getCleanupJobs(channel2);
        var leaderCleanupJobsTmp = spy(cleanupJobs1);
        var followerCleanupJobsTmp = spy(cleanupJobs2);
        var leaderChannel = channel1;
        var followerChannel = channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(brokerId2)) {
            leaderChannel = channel2;
            followerChannel = channel1;
            var tmp = followerCleanupJobsTmp;
            followerCleanupJobsTmp = leaderCleanupJobsTmp;
            leaderCleanupJobsTmp = tmp;
        }
        final var leaderCleanupJobs = leaderCleanupJobsTmp;
        final var followerCleanupJobs = followerCleanupJobsTmp;
        FieldUtils.writeDeclaredField(leaderChannel, "cleanupJobs", leaderCleanupJobs,
                true);
        FieldUtils.writeDeclaredField(followerChannel, "cleanupJobs", followerCleanupJobs,
                true);

        var owner1 = channel1.getOwnerAsync(bundle1);
        var owner2 = channel2.getOwnerAsync(bundle2);
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId2)))
                .when(loadManager).selectAsync(any(), any());
        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        String broker = brokerId1;
        channel1.publishAssignEventAsync(bundle1, broker);
        channel2.publishAssignEventAsync(bundle2, broker);

        waitUntilNewOwner(channel1, bundle1, broker);
        waitUntilNewOwner(channel2, bundle1, broker);
        waitUntilNewOwner(channel1, bundle2, broker);
        waitUntilNewOwner(channel2, bundle2, broker);

        // Verify to transfer the ownership to the other broker.
        channel1.publishUnloadEventAsync(new Unload(broker, bundle1, Optional.of(brokerId2)));
        waitUntilNewOwner(channel1, bundle1, brokerId2);
        waitUntilNewOwner(channel2, bundle1, brokerId2);

        // test stable metadata state
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        followerChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        FieldUtils.writeDeclaredField(followerChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        leaderChannel.handleBrokerRegistrationEvent(brokerId2, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(brokerId2, NotificationType.Deleted);

        waitUntilNewOwner(channel1, bundle1, brokerId2);
        waitUntilNewOwner(channel2, bundle1, brokerId2);
        waitUntilNewOwner(channel1, bundle2, brokerId2);
        waitUntilNewOwner(channel2, bundle2, brokerId2);

        verify(leaderCleanupJobs, times(1)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                2,
                0,
                3,
                0,
                2,
                0,
                0);


        // test jittery metadata state
        channel1.publishUnloadEventAsync(new Unload(brokerId2, bundle1, Optional.of(broker)));
        channel1.publishUnloadEventAsync(new Unload(brokerId2, bundle2, Optional.of(broker)));
        waitUntilNewOwner(channel1, bundle1, broker);
        waitUntilNewOwner(channel2, bundle1, broker);
        waitUntilNewOwner(channel1, bundle2, broker);
        waitUntilNewOwner(channel2, bundle2, broker);

        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        followerChannel.handleMetadataSessionEvent(SessionReestablished);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        verify(leaderCleanupJobs, times(2)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                2,
                0,
                3,
                0,
                3,
                0,
                0);

        // broker is back online
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Created);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Created);

        verify(leaderCleanupJobs, times(2)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                2,
                0,
                3,
                0,
                3,
                0,
                1);


        // broker is offline again
        FieldUtils.writeDeclaredField(leaderChannel, "maxCleanupDelayTimeInSecs", 3, true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        verify(leaderCleanupJobs, times(3)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(1, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                2,
                0,
                3,
                0,
                4,
                0,
                1);

        // finally cleanup
        waitUntilNewOwner(channel1, bundle1, brokerId2);
        waitUntilNewOwner(channel2, bundle1, brokerId2);
        waitUntilNewOwner(channel1, bundle2, brokerId2);
        waitUntilNewOwner(channel2, bundle2, brokerId2);

        verify(leaderCleanupJobs, times(3)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                3,
                0,
                5,
                0,
                4,
                0,
                1);

        // test unstable state
        channel1.publishUnloadEventAsync(new Unload(brokerId2, bundle1, Optional.of(broker)));
        channel1.publishUnloadEventAsync(new Unload(brokerId2, bundle2, Optional.of(broker)));
        waitUntilNewOwner(channel1, bundle1, broker);
        waitUntilNewOwner(channel2, bundle1, broker);
        waitUntilNewOwner(channel1, bundle2, broker);
        waitUntilNewOwner(channel2, bundle2, broker);

        leaderChannel.handleMetadataSessionEvent(SessionLost);
        followerChannel.handleMetadataSessionEvent(SessionLost);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        verify(leaderCleanupJobs, times(3)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, leaderCleanupJobs.size());
            assertEquals(0, followerCleanupJobs.size());
        });

        validateMonitorCounters(leaderChannel,
                3,
                0,
                5,
                0,
                4,
                1,
                1);

        // clean-up
        FieldUtils.writeDeclaredField(leaderChannel, "maxCleanupDelayTimeInSecs", 3 * 60, true);
        FieldUtils.writeDeclaredField(channel1, "cleanupJobs", cleanupJobs1,
                true);
        FieldUtils.writeDeclaredField(channel2, "cleanupJobs", cleanupJobs2,
                true);
    }

    @Test(priority = 10)
    public void conflictAndCompactionTest() throws ExecutionException, InterruptedException, TimeoutException,
            IllegalAccessException, PulsarClientException, PulsarServerException {
        String bundle = String.format("%s/%s", "public/default", "0x0000000a_0xffffffff");
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);
        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        var assigned1 = channel1.publishAssignEventAsync(bundle, brokerId1);
        assertNotNull(assigned1);

        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        String assignedAddr1 = assigned1.get(5, TimeUnit.SECONDS);
        assertEquals(brokerId1, assignedAddr1);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        var assigned2 = channel2.publishAssignEventAsync(bundle, brokerId2);
        assertNotNull(assigned2);
        Exception ex = null;
        try {
            assigned2.join();
        } catch (CompletionException e) {
            ex = e;
        }
        assertNull(ex);
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(bundle).get());
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(bundle).get());

        var compactor = spy (pulsar1.getStrategicCompactor());
        Field strategicCompactorField = FieldUtils.getDeclaredField(PulsarService.class, "strategicCompactor", true);
        FieldUtils.writeField(strategicCompactorField, pulsar1, compactor, true);
        FieldUtils.writeField(strategicCompactorField, pulsar2, compactor, true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(140, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    channel1.publishAssignEventAsync(bundle, brokerId1);
                    verify(compactor, times(1))
                            .compact(eq(ServiceUnitStateChannelImpl.TOPIC), any());
                });


        var channel3 = createChannel(pulsar);
        channel3.start();
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(
                        channel3.getOwnerAsync(bundle).get(), Optional.of(brokerId1)));
        channel3.close();
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
    }

    @Test(priority = 11)
    public void ownerLookupCountTests() throws IllegalAccessException {

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Assigning, "b1", 1));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Owned, "b1", 1));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Releasing, "b1", 1));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Splitting, null, "b1", 1));
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Free, "b1", 1));
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Deleted, "b1", 1));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, null);
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        validateOwnerLookUpCounters(channel1, 2, 3, 2, 1, 1, 2, 3);

    }

    @Test(priority = 12)
    public void unloadTest()
            throws ExecutionException, InterruptedException, IllegalAccessException {

        channel1.publishAssignEventAsync(bundle, brokerId1);

        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId1));
        Unload unload = new Unload(brokerId1, bundle, Optional.empty());

        channel1.publishUnloadEventAsync(unload);

        waitUntilState(channel1, bundle, Free);
        waitUntilState(channel2, bundle, Free);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertEquals(Optional.empty(), owner1.get());
        assertEquals(Optional.empty(), owner2.get());

        channel2.publishAssignEventAsync(bundle, brokerId2);

        waitUntilNewOwner(channel1, bundle, brokerId2);
        waitUntilNewOwner(channel2, bundle, brokerId2);

        ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId2));
        Unload unload2 = new Unload(brokerId2, bundle, Optional.empty());

        channel2.publishUnloadEventAsync(unload2);

        waitUntilState(channel1, bundle, Free);
        waitUntilState(channel2, bundle, Free);

        // test monitor if Free -> Init
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 1, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 1, true);

        ((ServiceUnitStateChannelImpl) channel1).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        ((ServiceUnitStateChannelImpl) channel2).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        waitUntilState(channel1, bundle, Init);
        waitUntilState(channel2, bundle, Init);

        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        validateMonitorCounters(leader,
                0,
                1,
                0,
                0,
                0,
                0,
                0);


        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 30 * 1000, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 300 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 300 * 1000, true);
    }

    @Test(priority = 13)
    public void assignTestWhenDestBrokerProducerFails()
            throws ExecutionException, InterruptedException, IllegalAccessException {

        Unload unload = new Unload(brokerId1, bundle, Optional.empty());

        channel1.publishUnloadEventAsync(unload);

        waitUntilState(channel1, bundle, Free);
        waitUntilState(channel2, bundle, Free);

        assertEquals(Optional.empty(), channel1.getOwnerAsync(bundle).get());
        assertEquals(Optional.empty(), channel2.getOwnerAsync(bundle).get());

        var producer = (Producer<ServiceUnitStateData>) FieldUtils.readDeclaredField(channel1,
                "producer", true);
        var spyProducer = spy(producer);
        var msg = mock(TypedMessageBuilder.class);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(msg).when(spyProducer).newMessage();
        doReturn(msg).when(msg).key(any());
        doReturn(msg).when(msg).value(any());
        doReturn(future).when(msg).sendAsync();
        FieldUtils.writeDeclaredField(channel2, "producer", spyProducer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId2)))
                .when(loadManager).selectAsync(any(), any());
        channel1.publishAssignEventAsync(bundle, brokerId2);
        // channel1 is broken. the assign won't be complete.
        waitUntilState(channel1, bundle);
        waitUntilState(channel2, bundle);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertFalse(owner1.isDone());
        assertFalse(owner2.isDone());

        // In 10 secs, the getOwnerAsync requests(lookup requests) should time out.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner1.isCompletedExceptionally()));
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner2.isCompletedExceptionally()));

        // recovered, check the monitor update state : Assigned -> Owned
        FieldUtils.writeDeclaredField(channel2, "producer", producer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);

        ((ServiceUnitStateChannelImpl) channel1).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        ((ServiceUnitStateChannelImpl) channel2).monitorOwnerships(
                List.of(brokerId1, brokerId2));


        waitUntilNewOwner(channel1, bundle, brokerId2);
        waitUntilNewOwner(channel2, bundle, brokerId2);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(brokerId2));

        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        validateMonitorCounters(leader,
                0,
                0,
                1,
                0,
                0,
                0,
                0);

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);

    }

    @Test(priority = 14)
    public void splitTestWhenProducerFails()
            throws ExecutionException, InterruptedException, IllegalAccessException {


        Unload unload = new Unload(brokerId1, bundle, Optional.empty());

        channel1.publishUnloadEventAsync(unload);

        waitUntilState(channel1, bundle, Free);
        waitUntilState(channel2, bundle, Free);

        channel1.publishAssignEventAsync(bundle, brokerId1);

        waitUntilState(channel1, bundle, Owned);
        waitUntilState(channel2, bundle, Owned);

        assertEquals(brokerId1, channel1.getOwnerAsync(bundle).get().get());
        assertEquals(brokerId1, channel2.getOwnerAsync(bundle).get().get());

        var producer = (Producer<ServiceUnitStateData>) FieldUtils.readDeclaredField(channel1,
                "producer", true);
        var spyProducer = spy(producer);
        var msg = mock(TypedMessageBuilder.class);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(msg).when(spyProducer).newMessage();
        doReturn(msg).when(msg).key(any());
        doReturn(msg).when(msg).value(any());
        doReturn(future).when(msg).sendAsync();
        FieldUtils.writeDeclaredField(channel1, "producer", spyProducer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        // Assert child bundle ownerships in the channels.


        Split split = new Split(bundle, brokerId1, Map.of(
                childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
        channel2.publishSplitEventAsync(split);
        // channel1 is broken. the split won't be complete.
        waitUntilState(channel1, bundle);
        waitUntilState(channel2, bundle);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);


        // recovered, check the monitor update state : Splitting -> Owned
        FieldUtils.writeDeclaredField(channel1, "producer", producer, true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);


        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;

        waitUntilStateWithMonitor(leader, bundle, Deleted);
        waitUntilStateWithMonitor(channel1, bundle, Deleted);
        waitUntilStateWithMonitor(channel2, bundle, Deleted);

        var ownerAddr1 = channel1.getOwnerAsync(bundle);
        var ownerAddr2 = channel2.getOwnerAsync(bundle);

        assertTrue(ownerAddr1.isCompletedExceptionally());
        assertTrue(ownerAddr2.isCompletedExceptionally());


        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);

    }

    @Test(priority = 15)
    public void testIsOwner() throws IllegalAccessException {

        var owner1 = channel1.isOwner(bundle);
        var owner2 = channel2.isOwner(bundle);

        assertFalse(owner1);
        assertFalse(owner2);

        owner1 = channel1.isOwner(bundle, brokerId2);
        owner2 = channel2.isOwner(bundle, brokerId1);

        assertFalse(owner1);
        assertFalse(owner2);

        channel1.publishAssignEventAsync(bundle, brokerId1);
        owner2 = channel2.isOwner(bundle);
        assertFalse(owner2);

        waitUntilOwnerChanges(channel1, bundle, null);
        waitUntilOwnerChanges(channel2, bundle, null);

        owner1 = channel1.isOwner(bundle);
        owner2 = channel2.isOwner(bundle);

        assertTrue(owner1);
        assertFalse(owner2);

        owner1 = channel1.isOwner(bundle, brokerId1);
        owner2 = channel2.isOwner(bundle, brokerId2);

        assertTrue(owner1);
        assertFalse(owner2);

        owner1 = channel2.isOwner(bundle, brokerId1);
        owner2 = channel1.isOwner(bundle, brokerId2);

        assertTrue(owner1);
        assertFalse(owner2);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Assigning, brokerId1, 1));
        assertFalse(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Owned, brokerId1, 1));
        assertTrue(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Releasing, null, brokerId1, 1));
        assertFalse(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Splitting, null, brokerId1, 1));
        assertTrue(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Free, null, brokerId1, 1));
        assertFalse(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Deleted, null, brokerId1, 1));
        assertFalse(channel1.isOwner(bundle));

        overrideTableView(channel1, bundle, null);
        assertFalse(channel1.isOwner(bundle));
    }

    @Test(priority = 16)
    public void splitAndRetryFailureTest() throws Exception {
        channel1.publishAssignEventAsync(bundle3, brokerId1);
        waitUntilNewOwner(channel1, bundle3, brokerId1);
        waitUntilNewOwner(channel2, bundle3, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle3).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle3).get();
        assertEquals(ownerAddr1, Optional.of(brokerId1));
        assertEquals(ownerAddr2, Optional.of(brokerId1));
        assertTrue(ownerAddr1.isPresent());

        NamespaceService namespaceService = pulsar1.getNamespaceService();
        CompletableFuture<Void> future = new CompletableFuture<>();
        int badVersionExceptionCount = 10;
        AtomicInteger count = new AtomicInteger(badVersionExceptionCount);
        future.completeExceptionally(new MetadataStoreException.BadVersionException("BadVersion"));
        doAnswer(invocationOnMock -> {
            if (count.decrementAndGet() > 0) {
                return future;
            }
            // Call the real method
            reset(namespaceService);
            doReturn(CompletableFuture.completedFuture(List.of("test-topic-1", "test-topic-2")))
                    .when(namespaceService).getOwnedTopicListForNamespaceBundle(any());
            return future;
        }).when(namespaceService).updateNamespaceBundlesForPolicies(any(), any());
        doReturn(namespaceService).when(pulsar1).getNamespaceService();
        doReturn(CompletableFuture.completedFuture(List.of("test-topic-1", "test-topic-2")))
                .when(namespaceService).getOwnedTopicListForNamespaceBundle(any());

        // Assert child bundle ownerships in the channels.

        Split split = new Split(bundle3, ownerAddr1.get(), Map.of(
                childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
        channel1.publishSplitEventAsync(split);

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1 , true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1 , true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertEquals(3, count.get());
                });
        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        ((ServiceUnitStateChannelImpl) leader)
                .monitorOwnerships(List.of(brokerId1, brokerId2));
        waitUntilState(leader, bundle3, Deleted);
        waitUntilState(channel1, bundle3, Deleted);
        waitUntilState(channel2, bundle3, Deleted);


        validateHandlerCounters(channel1, 1, 0, 3, 0, 0, 0, 2, 1, 0, 0, 0, 0, 1, 0);
        validateHandlerCounters(channel2, 1, 0, 3, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0);
        validateEventCounters(channel1, 1, 0, 1, 0, 0, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);

        waitUntilNewOwner(channel1, childBundle31, brokerId1);
        waitUntilNewOwner(channel1, childBundle32, brokerId1);
        waitUntilNewOwner(channel2, childBundle31, brokerId1);
        waitUntilNewOwner(channel2, childBundle32, brokerId1);
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle31).get());
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle32).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle31).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle32).get());


        // try the monitor and check the monitor moves `Deleted` -> `Init`

        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 1, true);

        ((ServiceUnitStateChannelImpl) channel1).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        ((ServiceUnitStateChannelImpl) channel2).monitorOwnerships(
                List.of(brokerId1, brokerId2));
        waitUntilState(channel1, bundle3, Init);
        waitUntilState(channel2, bundle3, Init);

        validateMonitorCounters(leader,
                0,
                1,
                1,
                0,
                0,
                0,
                0);


        cleanTableViews();

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 300 * 1000, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "stateTombstoneDelayTimeInMillis", 300 * 1000, true);
    }

    @Test(priority = 17)
    public void testOverrideInactiveBrokerStateData()
            throws IllegalAccessException, ExecutionException, InterruptedException, TimeoutException {

        var leaderChannel = channel1;
        var followerChannel = channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(brokerId2)) {
            leaderChannel = channel2;
            followerChannel = channel1;
        }

        String broker = brokerId1;

        // test override states
        String releasingBundle = "public/releasing/0xfffffff0_0xffffffff";
        String splittingBundle = bundle;
        String assigningBundle = "public/assigning/0xfffffff0_0xffffffff";
        String freeBundle = "public/free/0xfffffff0_0xffffffff";
        String deletedBundle = "public/deleted/0xfffffff0_0xffffffff";
        String ownedBundle = "public/owned/0xfffffff0_0xffffffff";
        overrideTableViews(releasingBundle,
                new ServiceUnitStateData(Releasing, null, broker, 1));
        overrideTableViews(splittingBundle,
                new ServiceUnitStateData(Splitting, null, broker,
                        Map.of(childBundle1Range, Optional.empty(),
                                childBundle2Range, Optional.empty()), 1));
        overrideTableViews(assigningBundle,
                new ServiceUnitStateData(Assigning, broker, null, 1));
        overrideTableViews(freeBundle,
                new ServiceUnitStateData(Free, null, broker, 1));
        overrideTableViews(deletedBundle,
                new ServiceUnitStateData(Deleted, null, broker, 1));
        overrideTableViews(ownedBundle,
                new ServiceUnitStateData(Owned, broker, null, 1));

        // test stable metadata state
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId2)))
                .when(loadManager).selectAsync(any(), any());
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        followerChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        FieldUtils.writeDeclaredField(followerChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        waitUntilNewOwner(channel2, releasingBundle, brokerId2);
        waitUntilNewOwner(channel2, childBundle11, brokerId2);
        waitUntilNewOwner(channel2, childBundle12, brokerId2);
        waitUntilNewOwner(channel2, assigningBundle, brokerId2);
        waitUntilNewOwner(channel2, ownedBundle, brokerId2);
        assertEquals(Optional.empty(), channel2.getOwnerAsync(freeBundle).get());
        assertTrue(channel2.getOwnerAsync(deletedBundle).isCompletedExceptionally());
        assertTrue(channel2.getOwnerAsync(splittingBundle).isCompletedExceptionally());

        // clean-up
        FieldUtils.writeDeclaredField(leaderChannel, "maxCleanupDelayTimeInSecs", 3 * 60, true);
        cleanTableViews();

    }

    @Test(priority = 18)
    public void testOverrideOrphanStateData()
            throws IllegalAccessException, ExecutionException, InterruptedException, TimeoutException {

        var leaderChannel = channel1;
        var followerChannel = channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(brokerId2)) {
            leaderChannel = channel2;
            followerChannel = channel1;
        }

        String broker = brokerId1;

        // test override states
        String releasingBundle = "public/releasing/0xfffffff0_0xffffffff";
        String splittingBundle = bundle;
        String assigningBundle = "public/assigning/0xfffffff0_0xffffffff";
        String freeBundle = "public/free/0xfffffff0_0xffffffff";
        String deletedBundle = "public/deleted/0xfffffff0_0xffffffff";
        String ownedBundle = "public/owned/0xfffffff0_0xffffffff";
        overrideTableViews(releasingBundle,
                new ServiceUnitStateData(Releasing, null, broker, 1));
        overrideTableViews(splittingBundle,
                new ServiceUnitStateData(Splitting, null, broker,
                        Map.of(childBundle1Range, Optional.empty(),
                                childBundle2Range, Optional.empty()), 1));
        overrideTableViews(assigningBundle,
                new ServiceUnitStateData(Assigning, broker, null, 1));
        overrideTableViews(freeBundle,
                new ServiceUnitStateData(Free, null, broker, 1));
        overrideTableViews(deletedBundle,
                new ServiceUnitStateData(Deleted, null, broker, 1));
        overrideTableViews(ownedBundle,
                new ServiceUnitStateData(Owned, broker, null, 1));

        // test stable metadata state
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId2)))
                .when(loadManager).selectAsync(any(), any());
        FieldUtils.writeDeclaredField(leaderChannel, "inFlightStateWaitingTimeInMillis",
                -1, true);
        FieldUtils.writeDeclaredField(followerChannel, "inFlightStateWaitingTimeInMillis",
                -1, true);
        ((ServiceUnitStateChannelImpl) leaderChannel)
                .monitorOwnerships(List.of(brokerId1, brokerId2));

        waitUntilNewOwner(channel2, releasingBundle, broker);
        waitUntilNewOwner(channel2, childBundle11, broker);
        waitUntilNewOwner(channel2, childBundle12, broker);
        waitUntilNewOwner(channel2, assigningBundle, brokerId2);
        waitUntilNewOwner(channel2, ownedBundle, broker);
        assertEquals(Optional.empty(), channel2.getOwnerAsync(freeBundle).get());
        assertTrue(channel2.getOwnerAsync(deletedBundle).isCompletedExceptionally());
        assertTrue(channel2.getOwnerAsync(splittingBundle).isCompletedExceptionally());

        // clean-up
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        cleanTableViews();
    }

    @Test(priority = 19)
    public void testActiveGetOwner() throws Exception {


        // set the bundle owner is the broker
        String broker = brokerId2;
        String bundle = "public/owned/0xfffffff0_0xffffffff";
        overrideTableViews(bundle,
                new ServiceUnitStateData(Owned, broker, null, 1));
        var owner = channel1.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS).get();
        assertEquals(owner, broker);

        // simulate the owner is inactive
        var spyRegistry = spy(new BrokerRegistryImpl(pulsar));
        doReturn(CompletableFuture.completedFuture(Optional.empty()))
                .when(spyRegistry).lookupAsync(eq(broker));
        FieldUtils.writeDeclaredField(channel1,
                "brokerRegistry", spyRegistry , true);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1000, true);


        // verify getOwnerAsync times out because the owner is inactive now.
        long start = System.currentTimeMillis();
        var ex = expectThrows(ExecutionException.class, () -> channel1.getOwnerAsync(bundle).get());
        assertTrue(ex.getCause() instanceof TimeoutException);
        assertTrue(System.currentTimeMillis() - start >= 1000);

        // simulate ownership cleanup(no selected owner) by the leader channel
        doReturn(CompletableFuture.completedFuture(Optional.empty()))
                .when(loadManager).selectAsync(any(), any());
        var leaderChannel = channel1;
        String leader1 = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader1, leader2);
        if (leader1.equals(brokerId2)) {
            leaderChannel = channel2;
        }
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        // verify the ownership cleanup, and channel's getOwnerAsync returns empty result without timeout
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 20 * 1000, true);
        start = System.currentTimeMillis();
        assertTrue(channel1.getOwnerAsync(bundle).get().isEmpty());
        assertTrue(System.currentTimeMillis() - start < 20_000);

        // simulate ownership cleanup(brokerId1 selected owner) by the leader channel
        overrideTableViews(bundle,
                new ServiceUnitStateData(Owned, broker, null, 1));
        doReturn(CompletableFuture.completedFuture(Optional.of(brokerId1)))
                .when(loadManager).selectAsync(any(), any());
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        getCleanupJobs(leaderChannel).clear();
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        // verify the ownership cleanup, and channel's getOwnerAsync returns brokerId1 without timeout
        start = System.currentTimeMillis();
        assertEquals(brokerId1, channel1.getOwnerAsync(bundle).get().get());
        assertTrue(System.currentTimeMillis() - start < 20_000);

        // test clean-up
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel1,
                "brokerRegistry", registry , true);
        cleanTableViews();

    }

    private static ConcurrentHashMap<String, CompletableFuture<Optional<String>>> getOwnerRequests(
            ServiceUnitStateChannel channel) throws IllegalAccessException {
        return (ConcurrentHashMap<String, CompletableFuture<Optional<String>>>)
                FieldUtils.readDeclaredField(channel,
                        "getOwnerRequests", true);
    }

    private static SessionEvent getLastMetadataSessionEvent(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        return (SessionEvent)
                FieldUtils.readField(channel, "lastMetadataSessionEvent", true);
    }

    private static long getLastMetadataSessionEventTimestamp(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        return (long)
                FieldUtils.readField(channel, "lastMetadataSessionEventTimestamp", true);
    }

    private static ConcurrentHashMap<String, CompletableFuture<Void>> getCleanupJobs(
            ServiceUnitStateChannel channel) throws IllegalAccessException {
        return (ConcurrentHashMap<String, CompletableFuture<Void>>)
                FieldUtils.readField(channel, "cleanupJobs", true);
    }


    private static void waitUntilNewChannelOwner(ServiceUnitStateChannel channel, String oldOwner) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    CompletableFuture<Optional<String>> owner = channel.getChannelOwnerAsync();
                    if (!owner.isDone()) {
                        return false;
                    }

                    return !StringUtils.equals(oldOwner, owner.get().orElse(null));
                });
    }

    private static void waitUntilOwnerChanges(ServiceUnitStateChannel channel, String serviceUnit, String oldOwner) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    CompletableFuture<Optional<String>> owner = channel.getOwnerAsync(serviceUnit);
                    if (!owner.isDone()) {
                        return false;
                    }
                    return !StringUtils.equals(oldOwner, owner.get().orElse(null));
                });
    }

    private static void waitUntilNewOwner(ServiceUnitStateChannel channel, String serviceUnit, String newOwner) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    try {
                        CompletableFuture<Optional<String>> owner = channel.getOwnerAsync(serviceUnit);
                        if (!owner.isDone()) {
                            return false;
                        }
                        return StringUtils.equals(newOwner, owner.get().orElse(null));
                    } catch (Exception e) {
                        return false;
                    }
                });
    }

    private static void waitUntilState(ServiceUnitStateChannel channel, String key)
            throws IllegalAccessException {
        TableViewImpl<ServiceUnitStateData> tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    ServiceUnitStateData actual = tv.get(key);
                    if (actual == null) {
                        return true;
                    } else {
                        return actual.state() != Owned;
                    }
                });
    }

    private static void waitUntilState(ServiceUnitStateChannel channel, String key, ServiceUnitState expected)
            throws IllegalAccessException {
        TableViewImpl<ServiceUnitStateData> tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    ServiceUnitStateData data = tv.get(key);
                    ServiceUnitState actual = state(data);
                    return actual == expected;
                });
    }

    private void waitUntilStateWithMonitor(ServiceUnitStateChannel channel, String key, ServiceUnitState expected)
            throws IllegalAccessException {
        TableViewImpl<ServiceUnitStateData> tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    ((ServiceUnitStateChannelImpl) channel)
                            .monitorOwnerships(List.of(brokerId1, brokerId2));
                    ServiceUnitStateData data = tv.get(key);
                    ServiceUnitState actual = state(data);
                    return actual == expected;
                });
    }

    private static void cleanTableView(ServiceUnitStateChannel channel, String serviceUnit)
            throws IllegalAccessException {
        var tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        var cache = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv, "data", true);
        cache.remove(serviceUnit);
    }

    private void cleanTableViews()
            throws IllegalAccessException {
        var tv1 = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel1, "tableview", true);
        var cache1 = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv1, "data", true);
        cache1.clear();

        var tv2 = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel2, "tableview", true);
        var cache2 = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv2, "data", true);
        cache2.clear();
    }

    private void overrideTableViews(String serviceUnit, ServiceUnitStateData val) throws IllegalAccessException {
        overrideTableView(channel1, serviceUnit, val);
        overrideTableView(channel2, serviceUnit, val);
    }

    private static void overrideTableView(ServiceUnitStateChannel channel, String serviceUnit, ServiceUnitStateData val)
            throws IllegalAccessException {
        var tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        var cache = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv, "data", true);
        if(val == null){
            cache.remove(serviceUnit);
        } else {
            cache.put(serviceUnit, val);
        }
    }

    private static void cleanOpsCounters(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        var handlerCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "handlerCounters", true);

        for(var val : handlerCounters.values()){
            val.getFailure().set(0);
            val.getTotal().set(0);
        }

        var eventCounters =
                (Map<ServiceUnitStateChannelImpl.EventType, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "eventCounters", true);

        for(var val : eventCounters.values()){
            val.getFailure().set(0);
            val.getTotal().set(0);
        }

        var ownerLookUpCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "ownerLookUpCounters", true);

        for(var val : ownerLookUpCounters.values()){
            val.getFailure().set(0);
            val.getTotal().set(0);
        }
    }

    private void cleanOwnershipMonitorCounters(ServiceUnitStateChannel channel) throws IllegalAccessException {
        FieldUtils.writeDeclaredField(channel, "totalInactiveBrokerCleanupCnt", 0, true);
        FieldUtils.writeDeclaredField(channel, "totalServiceUnitTombstoneCleanupCnt", 0, true);
        FieldUtils.writeDeclaredField(channel, "totalOrphanServiceUnitCleanupCnt", 0, true);
        FieldUtils.writeDeclaredField(channel, "totalCleanupErrorCnt", new AtomicLong(0), true);
        FieldUtils.writeDeclaredField(channel, "totalInactiveBrokerCleanupScheduledCnt", 0, true);
        FieldUtils.writeDeclaredField(channel, "totalInactiveBrokerCleanupIgnoredCnt", 0, true);
        FieldUtils.writeDeclaredField(channel, "totalInactiveBrokerCleanupCancelledCnt", 0, true);
    }

    private void cleanMetadataState(ServiceUnitStateChannel channel) throws IllegalAccessException {
        channel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(channel, "lastMetadataSessionEventTimestamp", 0L, true);
    }

    private static long getCleanupMetric(ServiceUnitStateChannel channel, String metric)
            throws IllegalAccessException {
        Object var = FieldUtils.readDeclaredField(channel, metric, true);
        if (var instanceof AtomicLong) {
            return ((AtomicLong) var).get();
        } else {
            return (long) var;
        }
    }

    private static void validateHandlerCounters(ServiceUnitStateChannel channel,
                                                long assignedT, long assignedF,
                                                long ownedT, long ownedF,
                                                long releasedT, long releasedF,
                                                long splittingT, long splittingF,
                                                long freeT, long freeF,
                                                long initT, long initF,
                                                long deletedT, long deletedF)
            throws IllegalAccessException {
        var handlerCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "handlerCounters", true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertEquals(assignedT, handlerCounters.get(Assigning).getTotal().get());
                    assertEquals(assignedF, handlerCounters.get(Assigning).getFailure().get());
                    assertEquals(ownedT, handlerCounters.get(Owned).getTotal().get());
                    assertEquals(ownedF, handlerCounters.get(Owned).getFailure().get());
                    assertEquals(releasedT, handlerCounters.get(Releasing).getTotal().get());
                    assertEquals(releasedF, handlerCounters.get(Releasing).getFailure().get());
                    assertEquals(splittingT, handlerCounters.get(Splitting).getTotal().get());
                    assertEquals(splittingF, handlerCounters.get(Splitting).getFailure().get());
                    assertEquals(freeT, handlerCounters.get(Free).getTotal().get());
                    assertEquals(freeF, handlerCounters.get(Free).getFailure().get());
                    assertEquals(initT, handlerCounters.get(Init).getTotal().get());
                    assertEquals(initF, handlerCounters.get(Init).getFailure().get());
                    assertEquals(deletedT, handlerCounters.get(Deleted).getTotal().get());
                    assertEquals(deletedF, handlerCounters.get(Deleted).getFailure().get());
                });
    }

    private static void validateEventCounters(ServiceUnitStateChannel channel,
                                              long assignT, long assignF,
                                              long splitT, long splitF,
                                              long unloadT, long unloadF)
            throws IllegalAccessException {
        var eventCounters =
                (Map<ServiceUnitStateChannelImpl.EventType, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "eventCounters", true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertEquals(assignT, eventCounters.get(Assign).getTotal().get());
                    assertEquals(assignF, eventCounters.get(Assign).getFailure().get());
                    assertEquals(splitT, eventCounters.get(Split).getTotal().get());
                    assertEquals(splitF, eventCounters.get(Split).getFailure().get());
                    assertEquals(unloadT, eventCounters.get(Unload).getTotal().get());
                    assertEquals(unloadF, eventCounters.get(Unload).getFailure().get());
                });
    }

    private static void validateOwnerLookUpCounters(ServiceUnitStateChannel channel,
                                                    long assigned,
                                                    long owned,
                                                    long released,
                                                    long splitting,
                                                    long free,
                                                    long deleted,
                                                    long init
                                                    )
            throws IllegalAccessException {
        var ownerLookUpCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "ownerLookUpCounters", true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertEquals(assigned, ownerLookUpCounters.get(Assigning).getTotal().get());
                    assertEquals(owned, ownerLookUpCounters.get(Owned).getTotal().get());
                    assertEquals(released, ownerLookUpCounters.get(Releasing).getTotal().get());
                    assertEquals(splitting, ownerLookUpCounters.get(Splitting).getTotal().get());
                    assertEquals(free, ownerLookUpCounters.get(Free).getTotal().get());
                    assertEquals(deleted, ownerLookUpCounters.get(Deleted).getTotal().get());
                    assertEquals(init, ownerLookUpCounters.get(Init).getTotal().get());
                });
    }

    private static void validateMonitorCounters(ServiceUnitStateChannel channel,
                                                long totalInactiveBrokerCleanupCnt,
                                                long totalServiceUnitTombstoneCleanupCnt,
                                                long totalOrphanServiceUnitCleanupCnt,
                                                long totalCleanupErrorCnt,
                                                long totalInactiveBrokerCleanupScheduledCnt,
                                                long totalInactiveBrokerCleanupIgnoredCnt,
                                                long totalInactiveBrokerCleanupCancelledCnt)
            throws IllegalAccessException {
        assertEquals(totalInactiveBrokerCleanupCnt, getCleanupMetric(channel, "totalInactiveBrokerCleanupCnt"));
        assertEquals(totalServiceUnitTombstoneCleanupCnt,
                getCleanupMetric(channel, "totalServiceUnitTombstoneCleanupCnt"));
        assertEquals(totalOrphanServiceUnitCleanupCnt, getCleanupMetric(channel, "totalOrphanServiceUnitCleanupCnt"));
        assertEquals(totalCleanupErrorCnt, getCleanupMetric(channel, "totalCleanupErrorCnt"));
        assertEquals(totalInactiveBrokerCleanupScheduledCnt,
                getCleanupMetric(channel, "totalInactiveBrokerCleanupScheduledCnt"));
        assertEquals(totalInactiveBrokerCleanupIgnoredCnt,
                getCleanupMetric(channel, "totalInactiveBrokerCleanupIgnoredCnt"));
        assertEquals(totalInactiveBrokerCleanupCancelledCnt,
                getCleanupMetric(channel, "totalInactiveBrokerCleanupCancelledCnt"));
    }

    ServiceUnitStateChannelImpl createChannel(PulsarService pulsar)
            throws IllegalAccessException {
        var tmpChannel = new ServiceUnitStateChannelImpl(pulsar);
        FieldUtils.writeDeclaredField(tmpChannel, "ownershipMonitorDelayTimeInSecs", 5, true);
        var channel = spy(tmpChannel);

        doReturn(loadManagerContext).when(channel).getContext();
        doReturn(registry).when(channel).getBrokerRegistry();
        doReturn(loadManager).when(channel).getLoadManager();


        var leaderElectionService = new LeaderElectionService(
                pulsar.getCoordinationService(), pulsar.getBrokerId(), pulsar.getSafeWebServiceAddress(),
                state -> {
                    if (state == LeaderElectionState.Leading) {
                        channel.scheduleOwnershipMonitor();
                    } else {
                        channel.cancelOwnershipMonitor();
                    }
                });
        leaderElectionService.start();

        doReturn(leaderElectionService).when(channel).getLeaderElectionService();

        return channel;
    }
}
