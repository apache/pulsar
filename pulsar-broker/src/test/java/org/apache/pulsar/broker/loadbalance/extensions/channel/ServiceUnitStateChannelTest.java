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

import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Free;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Owned;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Released;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState.Splitting;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Assign;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Split;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.EventType.Unload;
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl.MAX_CLEAN_UP_DELAY_TIME_IN_SECS;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.ConnectionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.Reconnected;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ServiceUnitStateChannelTest extends MockedPulsarServiceBaseTest {

    private PulsarService pulsar1;
    private PulsarService pulsar2;
    private ServiceUnitStateChannel channel1;
    private ServiceUnitStateChannel channel2;
    private String lookupServiceAddress1;
    private String lookupServiceAddress2;
    private String bundle;

    private String bundle1;
    private String bundle2;
    private PulsarTestContext additionalPulsarTestContext;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(10);
        super.internalSetup(conf);

        admin.tenants().createTenant("pulsar", createDefaultTenantInfo());
        admin.namespaces().createNamespace("pulsar/system");
        admin.tenants().createTenant("public", createDefaultTenantInfo());
        admin.namespaces().createNamespace("public/default");

        pulsar1 = pulsar;
        additionalPulsarTestContext = createAdditionalPulsarTestContext(getDefaultConf());
        pulsar2 = additionalPulsarTestContext.getPulsarService();
        channel1 = spy(new ServiceUnitStateChannelImpl(pulsar1));
        channel1.start();
        channel2 = spy(new ServiceUnitStateChannelImpl(pulsar2));
        channel2.start();
        lookupServiceAddress1 = (String)
                FieldUtils.readDeclaredField(channel1, "lookupServiceAddress", true);
        lookupServiceAddress2 = (String)
                FieldUtils.readDeclaredField(channel2, "lookupServiceAddress", true);

        bundle = String.format("%s/%s", "public/default", "0x00000000_0xffffffff");
        bundle1 = String.format("%s/%s", "public/default", "0x00000000_0xfffffff0");
        bundle2 = String.format("%s/%s", "public/default", "0xfffffff0_0xffffffff");
    }

    @BeforeMethod
    protected void initTableViews() throws Exception {
        cleanTableView(channel1, bundle);
        cleanTableView(channel2, bundle);
        cleanOpsCounters(channel1);
        cleanOpsCounters(channel2);
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

    @Test(priority = 0)
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

        if (newChannelOwner1.equals(Optional.of(lookupServiceAddress1))) {
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
        var channel = new ServiceUnitStateChannelImpl(pulsar);
        int errorCnt = validateChannelStart(channel);
        assertEquals(6, errorCnt);
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
            channel.publishAssignEventAsync(bundle, lookupServiceAddress1).get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            channel.publishUnloadEventAsync(
                    new Unload(lookupServiceAddress1, bundle, Optional.of(lookupServiceAddress2)))
                    .get(2, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IllegalStateException) {
                errorCnt++;
            }
        }
        try {
            channel.publishSplitEventAsync(new Split(bundle, lookupServiceAddress1, Map.of()))
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

        var assigned1 = channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        var assigned2 = channel2.publishAssignEventAsync(bundle, lookupServiceAddress2);
        assertNotNull(assigned1);
        assertNotNull(assigned2);
        waitUntilOwnerChanges(channel1, bundle, null);
        waitUntilOwnerChanges(channel2, bundle, null);
        String assignedAddr1 = assigned1.get(5, TimeUnit.SECONDS);
        String assignedAddr2 = assigned2.get(5, TimeUnit.SECONDS);

        assertEquals(assignedAddr1, assignedAddr2);
        assertTrue(assignedAddr1.equals(lookupServiceAddress1)
                || assignedAddr1.equals(lookupServiceAddress2), assignedAddr1);

        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(getOwnerRequests1.size(), 0);
        assertEquals(getOwnerRequests2.size(), 0);

        validateHandlerCounters(channel1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0);
        validateHandlerCounters(channel2, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0);
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

        var owner3 = channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        var owner4 = channel2.publishAssignEventAsync(bundle, lookupServiceAddress2);
        assertTrue(owner3.isCompletedExceptionally());
        assertNotNull(owner4);
        String ownerAddrOpt2 = owner4.get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddrOpt2, lookupServiceAddress2);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        FieldUtils.writeDeclaredField(channel1, "producer", producer, true);
    }

    @Test(priority = 4)
    public void unloadTest()
            throws ExecutionException, InterruptedException, TimeoutException, IllegalAccessException {

        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());


        channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(lookupServiceAddress1));

        Unload unload = new Unload(lookupServiceAddress1, bundle, Optional.of(lookupServiceAddress2));
        channel1.publishUnloadEventAsync(unload);

        waitUntilNewOwner(channel1, bundle, lookupServiceAddress2);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress2);

        ownerAddr1 = channel1.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        ownerAddr2 = channel2.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(lookupServiceAddress2));

        validateHandlerCounters(channel1, 2, 0, 2, 0, 1, 0, 0, 0, 0, 0);
        validateHandlerCounters(channel2, 2, 0, 2, 0, 1, 0, 0, 0, 0, 0);
        validateEventCounters(channel1, 1, 0, 0, 0, 1, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
    }

    @Test(priority = 5)
    public void unloadTestWhenDestBrokerFails()
            throws ExecutionException, InterruptedException, IllegalAccessException {

        var getOwnerRequests1 = getOwnerRequests(channel1);
        var getOwnerRequests2 = getOwnerRequests(channel2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, Optional.of(lookupServiceAddress1));

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
        Unload unload = new Unload(lookupServiceAddress1, bundle, Optional.of(lookupServiceAddress2));
        channel1.publishUnloadEventAsync(unload);
        // channel1 is broken. the ownership transfer won't be complete.
        waitUntilNewState(channel1, bundle);
        waitUntilNewState(channel2, bundle);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertFalse(owner1.isDone());
        assertFalse(owner2.isDone());

        assertEquals(1, getOwnerRequests1.size());
        assertEquals(1, getOwnerRequests2.size());

        // In 10 secs, the getOwnerAsync requests(lookup requests) should time out.
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner1.isCompletedExceptionally()));
        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner2.isCompletedExceptionally()));

        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());


        // TODO: retry lookups and assert that the monitor cleans up the stuck assignments
        /*
        owner1 = channel1.getOwnerAsync(bundle);
        owner2 = channel2.getOwnerAsync(bundle);
        assertFalse(channel1.getOwnerAsync(bundle).isDone());
        assertFalse(channel1.getOwnerAsync(bundle).isDone());
         */
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2, "producer", producer, true);
    }

    @Test(priority = 6)
    public void splitAndRetryTest() throws Exception {
        channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();
        assertEquals(ownerAddr1, Optional.of(lookupServiceAddress1));
        assertEquals(ownerAddr2, Optional.of(lookupServiceAddress1));
        assertTrue(ownerAddr1.isPresent());

        NamespaceService namespaceService = spy(pulsar1.getNamespaceService());
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
            return future;
        }).when(namespaceService).updateNamespaceBundles(any(), any());
        doReturn(namespaceService).when(pulsar1).getNamespaceService();

        Split split = new Split(bundle, ownerAddr1.get(), new HashMap<>());
        channel1.publishSplitEventAsync(split);

        waitUntilNewOwner(channel1, bundle, null);
        waitUntilNewOwner(channel2, bundle, null);

        validateHandlerCounters(channel1, 1, 0, 9, 0, 0, 0, 1, 0, 7, 0);
        validateHandlerCounters(channel2, 1, 0, 9, 0, 0, 0, 1, 0, 7, 0);
        validateEventCounters(channel1, 1, 0, 1, 0, 0, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
        // Verify the retry count
        verify(((ServiceUnitStateChannelImpl) channel1), times(badVersionExceptionCount + 1))
                .splitServiceUnitOnceAndRetry(any(), any(), any(), any(), any(), any(), anyLong(), any());

        // Assert child bundle ownerships in the channels.
        String childBundle1 = "public/default/0x7fffffff_0xffffffff";
        String childBundle2 = "public/default/0x00000000_0x7fffffff";

        waitUntilNewOwner(channel1, childBundle1, lookupServiceAddress1);
        waitUntilNewOwner(channel1, childBundle2, lookupServiceAddress1);
        waitUntilNewOwner(channel2, childBundle1, lookupServiceAddress1);
        waitUntilNewOwner(channel2, childBundle2, lookupServiceAddress1);
        assertEquals(Optional.of(lookupServiceAddress1), channel1.getOwnerAsync(childBundle1).get());
        assertEquals(Optional.of(lookupServiceAddress1), channel1.getOwnerAsync(childBundle2).get());
        assertEquals(Optional.of(lookupServiceAddress1), channel2.getOwnerAsync(childBundle1).get());
        assertEquals(Optional.of(lookupServiceAddress1), channel2.getOwnerAsync(childBundle2).get());

        cleanTableView(channel1, childBundle1);
        cleanTableView(channel2, childBundle1);
        cleanTableView(channel1, childBundle2);
        cleanTableView(channel2, childBundle2);
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
        var leaderCleanupJobs = spy(cleanupJobs1);
        var followerCleanupJobs = spy(cleanupJobs2);
        var leaderChannel = channel1;
        var followerChannel = channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(lookupServiceAddress2)) {
            leaderChannel = channel2;
            followerChannel = channel1;
            var tmp = followerCleanupJobs;
            followerCleanupJobs = leaderCleanupJobs;
            leaderCleanupJobs = tmp;
        }
        FieldUtils.writeDeclaredField(leaderChannel, "cleanupJobs", leaderCleanupJobs,
                true);
        FieldUtils.writeDeclaredField(followerChannel, "cleanupJobs", followerCleanupJobs,
                true);

        var owner1 = channel1.getOwnerAsync(bundle1);
        var owner2 = channel2.getOwnerAsync(bundle2);

        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        String broker = lookupServiceAddress1;
        channel1.publishAssignEventAsync(bundle1, broker);
        channel2.publishAssignEventAsync(bundle2, broker);
        waitUntilNewOwner(channel1, bundle1, broker);
        waitUntilNewOwner(channel2, bundle1, broker);
        waitUntilNewOwner(channel1, bundle2, broker);
        waitUntilNewOwner(channel2, bundle2, broker);

        // test stable metadata state
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        followerChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        FieldUtils.writeDeclaredField(followerChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        waitUntilNewOwner(channel1, bundle1, null);
        waitUntilNewOwner(channel2, bundle1, null);
        waitUntilNewOwner(channel1, bundle2, null);
        waitUntilNewOwner(channel2, bundle2, null);

        verify(leaderCleanupJobs, times(1)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        assertEquals(0, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));

        // test jittery metadata state
        channel1.publishAssignEventAsync(bundle1, broker);
        channel2.publishAssignEventAsync(bundle2, broker);
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
        assertEquals(1, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));

        // broker is back online
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Created);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Created);

        verify(leaderCleanupJobs, times(2)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        assertEquals(0, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));


        // broker is offline again
        FieldUtils.writeDeclaredField(leaderChannel, "maxCleanupDelayTimeInSecs", 3, true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        verify(leaderCleanupJobs, times(3)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        assertEquals(1, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(3, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));

        // finally cleanup
        waitUntilNewOwner(channel1, bundle1, null);
        waitUntilNewOwner(channel2, bundle1, null);
        waitUntilNewOwner(channel1, bundle2, null);
        waitUntilNewOwner(channel2, bundle2, null);

        verify(leaderCleanupJobs, times(3)).computeIfAbsent(eq(broker), any());
        verify(followerCleanupJobs, times(0)).computeIfAbsent(eq(broker), any());
        assertEquals(0, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(2, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(4, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(3, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));

        // test unstable state
        channel1.publishAssignEventAsync(bundle1, broker);
        channel2.publishAssignEventAsync(bundle2, broker);
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
        assertEquals(0, leaderCleanupJobs.size());
        assertEquals(0, followerCleanupJobs.size());
        assertEquals(2, getCleanupMetric(leaderChannel, "totalCleanupCnt"));
        assertEquals(2, getCleanupMetric(leaderChannel, "totalBrokerCleanupTombstoneCnt"));
        assertEquals(4, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupTombstoneCnt"));
        assertEquals(0, getCleanupMetric(leaderChannel, "totalCleanupErrorCnt"));
        assertEquals(3, getCleanupMetric(leaderChannel, "totalCleanupScheduledCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupIgnoredCnt"));
        assertEquals(1, getCleanupMetric(leaderChannel, "totalCleanupCancelledCnt"));

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

        var producer = (Producer<ServiceUnitStateData>) FieldUtils.readDeclaredField(channel1, "producer", true);
        producer.newMessage().key(bundle).send();
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);
        assertTrue(owner1.get().isEmpty());
        assertTrue(owner2.get().isEmpty());

        var assigned1 = channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        assertNotNull(assigned1);

        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        String assignedAddr1 = assigned1.get(5, TimeUnit.SECONDS);
        assertEquals(lookupServiceAddress1, assignedAddr1);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        var assigned2 = channel2.publishAssignEventAsync(bundle, lookupServiceAddress2);
        assertNotNull(assigned2);
        Exception ex = null;
        try {
            assigned2.join();
        } catch (CompletionException e) {
            ex = e;
        }
        assertNotNull(ex);
        assertEquals(TimeoutException.class, ex.getCause().getClass());
        assertEquals(Optional.of(lookupServiceAddress1), channel2.getOwnerAsync(bundle).get());
        assertEquals(Optional.of(lookupServiceAddress1), channel1.getOwnerAsync(bundle).get());

        var compactor = spy (pulsar1.getStrategicCompactor());
        Field strategicCompactorField = FieldUtils.getDeclaredField(PulsarService.class, "strategicCompactor", true);
        FieldUtils.writeField(strategicCompactorField, pulsar1, compactor, true);
        FieldUtils.writeField(strategicCompactorField, pulsar2, compactor, true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(140, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(compactor, times(1))
                        .compact(eq(ServiceUnitStateChannelImpl.TOPIC), any()));

        var channel3 = new ServiceUnitStateChannelImpl(pulsar1);
        channel3.start();
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(
                        channel3.getOwnerAsync(bundle).get(), Optional.of(lookupServiceAddress1)));
        channel3.close();
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
    }

    @Test(priority = 11)
    public void ownerLookupCountTests() throws IllegalAccessException {

        overrideTableView(channel1, bundle, null);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Assigned, "b1"));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Owned, "b1"));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Released, "b1"));
        channel1.getOwnerAsync(bundle);
        channel1.getOwnerAsync(bundle);

        overrideTableView(channel1, bundle, new ServiceUnitStateData(Splitting, "b1"));
        channel1.getOwnerAsync(bundle);

        validateOwnerLookUpCounters(channel1, 2, 3, 2, 1, 1);

    }


    // TODO: add the channel recovery test when broker registry is added.

    private static ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>> getOwnerRequests(
            ServiceUnitStateChannel channel) throws IllegalAccessException {
        return (ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>)
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

    private static ConcurrentOpenHashMap<String, CompletableFuture<Void>> getCleanupJobs(
            ServiceUnitStateChannel channel) throws IllegalAccessException {
        return (ConcurrentOpenHashMap<String, CompletableFuture<Void>>)
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

    private static void waitUntilNewState(ServiceUnitStateChannel channel, String key)
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

    private static void cleanTableView(ServiceUnitStateChannel channel, String serviceUnit)
            throws IllegalAccessException {
        var tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        var cache = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv, "data", true);
        cache.remove(serviceUnit);
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
                (Map<ServiceUnitStateChannelImpl.EventType, AtomicLong>)
                        FieldUtils.readDeclaredField(channel, "ownerLookUpCounters", true);

        for(var val : ownerLookUpCounters.values()){
            val.set(0);
        }
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
                                                long freeT, long freeF)
            throws IllegalAccessException {
        var handlerCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "handlerCounters", true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertEquals(assignedT, handlerCounters.get(Assigned).getTotal().get());
                    assertEquals(assignedF, handlerCounters.get(Assigned).getFailure().get());
                    assertEquals(ownedT, handlerCounters.get(Owned).getTotal().get());
                    assertEquals(ownedF, handlerCounters.get(Owned).getFailure().get());
                    assertEquals(releasedT, handlerCounters.get(Released).getTotal().get());
                    assertEquals(releasedF, handlerCounters.get(Released).getFailure().get());
                    assertEquals(splittingT, handlerCounters.get(Splitting).getTotal().get());
                    assertEquals(splittingF, handlerCounters.get(Splitting).getFailure().get());
                    assertEquals(freeT, handlerCounters.get(Free).getTotal().get());
                    assertEquals(freeF, handlerCounters.get(Free).getFailure().get());
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
                                                    long free)
            throws IllegalAccessException {
        var ownerLookUpCounters =
                (Map<ServiceUnitState, AtomicLong>)
                        FieldUtils.readDeclaredField(channel, "ownerLookUpCounters", true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertEquals(assigned, ownerLookUpCounters.get(Assigned).get());
                    assertEquals(owned, ownerLookUpCounters.get(Owned).get());
                    assertEquals(released, ownerLookUpCounters.get(Released).get());
                    assertEquals(splitting, ownerLookUpCounters.get(Splitting).get());
                    assertEquals(free, ownerLookUpCounters.get(Free).get());
                });
    }
}
