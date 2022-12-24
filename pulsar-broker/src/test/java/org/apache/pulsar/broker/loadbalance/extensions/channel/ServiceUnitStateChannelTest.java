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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
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

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        super.internalSetup(conf);

        admin.tenants().createTenant("pulsar", createDefaultTenantInfo());
        admin.namespaces().createNamespace("pulsar/system");
        admin.tenants().createTenant("public", createDefaultTenantInfo());
        admin.namespaces().createNamespace("public/default");

        pulsar1 = pulsar;
        pulsar2 = startBrokerWithoutAuthorization(getDefaultConf());
        channel1 = new ServiceUnitStateChannelImpl(pulsar1);
        channel1.start();
        channel2 = new ServiceUnitStateChannelImpl(pulsar2);
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
    }


    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        channel1.close();
        channel2.close();
        pulsar1 = null;
        pulsar2.close();
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

        if (newChannelOwner1.equals(lookupServiceAddress1)) {
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
            throws ExecutionException, InterruptedException, TimeoutException {
        int errorCnt = 0;
        try {
            channel.isChannelOwnerAsync().get(2, TimeUnit.SECONDS);
        } catch (IllegalStateException e) {
            errorCnt++;
        }
        try {
            channel.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        } catch (IllegalStateException e) {
            errorCnt++;
        }
        try {
            channel.getOwnerAsync(bundle);
        } catch (IllegalStateException e) {
            errorCnt++;
        }
        try {
            channel.publishAssignEventAsync(bundle, lookupServiceAddress1);
        } catch (IllegalStateException e) {
            errorCnt++;
        }
        try {
            channel.publishUnloadEventAsync(
                    new Unload(lookupServiceAddress1, bundle, Optional.of(lookupServiceAddress2)));
        } catch (IllegalStateException e) {
            errorCnt++;
        }
        try {
            channel.publishSplitEventAsync(new Split(bundle, lookupServiceAddress1, Map.of()));
        } catch (IllegalStateException e) {
            errorCnt++;
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

        assertNull(owner1.get());
        assertNull(owner2.get());

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
        // TODO: check conflict resolution
        // assertEquals(assignedAddr1, ownerAddr1);
        assertEquals(getOwnerRequests1.size(), 0);
        assertEquals(getOwnerRequests2.size(), 0);
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

        assertNull(owner1.get());
        assertNull(owner2.get());

        owner1 = channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        owner2 = channel2.publishAssignEventAsync(bundle, lookupServiceAddress2);
        assertTrue(owner1.isCompletedExceptionally());
        assertNotNull(owner2);
        String ownerAddr2 = owner2.get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddr2, lookupServiceAddress2);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress2);
        assertEquals(0, getOwnerRequests1.size());
        assertEquals(0, getOwnerRequests2.size());

        FieldUtils.writeDeclaredField(channel1, "producer", producer, true);
    }

    @Test(priority = 4)
    public void unloadTest()
            throws ExecutionException, InterruptedException, TimeoutException {

        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertNull(owner1.get());
        assertNull(owner2.get());


        channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();

        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, lookupServiceAddress1);

        Unload unload = new Unload(lookupServiceAddress1, bundle, Optional.of(lookupServiceAddress2));
        channel1.publishUnloadEventAsync(unload);

        waitUntilNewOwner(channel1, bundle, lookupServiceAddress2);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress2);

        ownerAddr1 = channel1.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        ownerAddr2 = channel2.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS);
        assertEquals(ownerAddr1, ownerAddr2);
        assertEquals(ownerAddr1, lookupServiceAddress2);
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
        assertEquals(ownerAddr1, lookupServiceAddress1);

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
    public void splitTest() throws Exception {
        channel1.publishAssignEventAsync(bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel1, bundle, lookupServiceAddress1);
        waitUntilNewOwner(channel2, bundle, lookupServiceAddress1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle).get();
        assertEquals(ownerAddr1, lookupServiceAddress1);
        assertEquals(ownerAddr2, lookupServiceAddress1);

        Split split = new Split(bundle, ownerAddr1, new HashMap<>());
        channel1.publishSplitEventAsync(split);

        waitUntilNewOwner(channel1, bundle, null);
        waitUntilNewOwner(channel2, bundle, null);

        // TODO: assert child bundle ownerships in the channels.
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

        assertNull(owner1.get());
        assertNull(owner2.get());

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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
        assertEquals(0, getCleanupMetric(leaderChannel, "totalServiceUnitCleanupErrorCnt"));
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
                    if (oldOwner == null) {
                        return owner != null;
                    }
                    return !oldOwner.equals(owner);
                });
    }

    private static void waitUntilOwnerChanges(ServiceUnitStateChannel channel, String serviceUnit, String oldOwner) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    CompletableFuture<String> owner = channel.getOwnerAsync(serviceUnit);
                    if (!owner.isDone()) {
                        return false;
                    }
                    return !StringUtils.equals(oldOwner, owner.get());
                });
    }

    private static void waitUntilNewOwner(ServiceUnitStateChannel channel, String serviceUnit, String newOwner) {
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(15, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    try {
                        CompletableFuture<String> owner = channel.getOwnerAsync(serviceUnit);
                        if (!owner.isDone()) {
                            return false;
                        }
                        return StringUtils.equals(newOwner, owner.get());
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
                        return actual.state() != ServiceUnitState.Owned;
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

    private static long getCleanupMetric(ServiceUnitStateChannel channel, String metric)
            throws IllegalAccessException {
        return (long) FieldUtils.readDeclaredField(channel, metric, true);
    }
}
