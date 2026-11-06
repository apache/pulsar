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
import static org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl.TOPIC;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.ConnectionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.Reconnected;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.BrokerRegistryImpl;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.LoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensions.models.Split;
import org.apache.pulsar.broker.loadbalance.extensions.models.Unload;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.mockito.Answers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "broker")
@SuppressWarnings("unchecked")
public class ServiceUnitStateChannelTest extends MockedPulsarServiceBaseTest {

    private PulsarService pulsar1;
    private PulsarService pulsar2;
    private ServiceUnitStateChannel channel1;
    private ServiceUnitStateChannel channel2;
    private String namespaceName;
    private String namespaceName2;
    private String brokerId1;
    private String brokerId2;
    private String brokerId3;
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

    private PulsarAdmin pulsarAdmin;

    private ExtensibleLoadManagerImpl loadManager;

    private final String serviceUnitStateTableViewClassName;

    private Brokers brokers;

    // Reset before each test, so that the failures a failed test did not use cannot fail the next one
    private final AtomicInteger remainingNamespaceBundlesUpdateFailures = new AtomicInteger();

    // What the mocks and spies that the channels call return. Their answers are installed in setup, before anything
    // calls them, and read these: Mockito stubs the method of the next call on a mock, from any thread, so a test
    // can't stub a mock that other threads call.
    private final AtomicBoolean failBrokerHealthChecks = new AtomicBoolean();
    private final Map<String, CompletableFuture<?>> brokerLookups = new ConcurrentHashMap<>();
    private final AtomicReference<CompletableFuture<Optional<String>>> selectedBroker = new AtomicReference<>();
    private static final CompletableFuture<Void> FAILED_HEALTH_CHECK =
            CompletableFuture.failedFuture(new RuntimeException("failed"));
    private final AtomicReference<CompletableFuture<Void>> brokerHealthCheck =
            new AtomicReference<>(FAILED_HEALTH_CHECK);

    @DataProvider(name = "serviceUnitStateTableViewClassName")
    public static Object[][] serviceUnitStateTableViewClassName() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName()},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName()}
        };
    }

    @Factory(dataProvider = "serviceUnitStateTableViewClassName")
    public ServiceUnitStateChannelTest(String serviceUnitStateTableViewClassName) {
        this.serviceUnitStateTableViewClassName = serviceUnitStateTableViewClassName;
    }

    private void updateConfig(ServiceConfiguration conf) {
        conf.setAllowAutoTopicCreation(true);
        conf.setAllowAutoTopicCreationType(TopicType.PARTITIONED);
        conf.setLoadBalancerDebugModeEnabled(true);
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(10);
        // The metadata store variant runs a subclass of its table view, for cleanTableViews()
        conf.setLoadManagerServiceUnitStateTableViewClassName(
                serviceUnitStateTableViewClassName.equals(ServiceUnitStateMetadataStoreTableViewImpl.class.getName())
                        ? InitTrackingMetadataStoreTableView.class.getName() : serviceUnitStateTableViewClassName);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        updateConfig(conf);
        super.internalSetup(conf);

        namespaceName = "my-tenant/my-ns";
        namespaceName2 = "my-tenant/my-ns2";
        admin.tenants().createTenant("my-tenant", createDefaultTenantInfo());
        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().createNamespace(namespaceName2);

        pulsar1 = pulsar;
        failBrokerHealthChecks.set(false);
        brokerLookups.clear();
        selectedBroker.set(null);
        brokerHealthCheck.set(FAILED_HEALTH_CHECK);
        registry = spy(new BrokerRegistryImpl(pulsar1));
        doAnswer(invocation -> {
            var lookup = brokerLookups.get(invocation.getArgument(0, String.class));
            return lookup != null ? lookup : invocation.callRealMethod();
        }).when(registry).lookupAsync(any());
        registry.start();
        pulsarAdmin = spy(pulsar.getAdminClient());
        doAnswer(invocation -> failBrokerHealthChecks.get() ? brokers : invocation.callRealMethod())
                .when(pulsarAdmin).brokers();
        loadManagerContext = mock(LoadManagerContext.class);
        doReturn(mock(LoadDataStore.class)).when(loadManagerContext).brokerLoadDataStore();
        doReturn(mock(LoadDataStore.class)).when(loadManagerContext).topBundleLoadDataStore();
        loadManager = mock(ExtensibleLoadManagerImpl.class);
        doAnswer(invocation -> {
            var selected = selectedBroker.get();
            return selected != null ? selected : Answers.RETURNS_DEFAULTS.answer(invocation);
        }).when(loadManager).selectAsync(any(), any(), any());
        var conf2 = getDefaultConf();
        updateConfig(conf2);
        additionalPulsarTestContext = createAdditionalPulsarTestContext(conf2);
        pulsar2 = additionalPulsarTestContext.getPulsarService();

        channel1 = createChannel(pulsar1);
        channel1.start();

        channel2 = createChannel(pulsar2);
        channel2.start();
        brokerId1 = (String)
                FieldUtils.readDeclaredField(channel1, "brokerId", true);
        brokerId2 = (String)
                FieldUtils.readDeclaredField(channel2, "brokerId", true);
        brokerId3 = "broker-3";

        bundle = namespaceName + "/0x00000000_0xffffffff";
        bundle1 = namespaceName + "/0x00000000_0xfffffff0";
        bundle2 = namespaceName + "/0xfffffff0_0xffffffff";
        bundle3 = namespaceName2 + "/0x00000000_0xffffffff";
        childBundle1Range = "0x7fffffff_0xffffffff";
        childBundle2Range = "0x00000000_0x7fffffff";

        childBundle11 = namespaceName + "/" + childBundle1Range;
        childBundle12 = namespaceName + "/" + childBundle2Range;

        childBundle31 = namespaceName2 + "/" + childBundle1Range;
        childBundle32 = namespaceName2 + "/" + childBundle2Range;

        brokers = mock(Brokers.class);
        doAnswer(invocation -> brokerHealthCheck.get()).when(brokers).healthcheckAsync(any());
    }

    @BeforeMethod
    protected void initChannels() throws Exception {
        remainingNamespaceBundlesUpdateFailures.set(0);
        disableChannels();
        cleanTableViews();
        cleanOwnershipMonitorCounters(channel1);
        cleanOwnershipMonitorCounters(channel2);
        cleanOpsCounters(channel1);
        cleanOpsCounters(channel2);
        cleanMetadataState(channel1);
        cleanMetadataState(channel2);
        enableChannels();
        failBrokerHealthChecks.set(false);
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

        if (newChannelOwner1.equals(Optional.of(brokerId1))) {
            assertTrue(channel1.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
            assertFalse(channel2.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
        } else {
            assertFalse(channel1.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
            assertTrue(channel2.isChannelOwnerAsync().get(2, TimeUnit.SECONDS));
        }
    }

    @Test(priority = 1)
    public void testCompletedGetOwnerRequestDoesNotRemoveNewRequest() {
        ServiceUnitStateChannelImpl channel = (ServiceUnitStateChannelImpl) channel1;
        String serviceUnit = namespaceName + "/0x10000000_0x10000001";
        var getOwnerRequests = channel.getOwnerRequests();
        getOwnerRequests.remove(serviceUnit);
        CompletableFuture<String> oldRequest = channel.dedupeGetOwnerRequest(serviceUnit);
        assertEquals(getOwnerRequests.get(serviceUnit), oldRequest);

        CompletableFuture<String> newRequest = null;
        try {
            // State-event handlers remove the current request before completing it, allowing a later lookup
            // to install a new request generation before the old request's completion cleanup runs.
            assertTrue(getOwnerRequests.remove(serviceUnit, oldRequest));
            newRequest = channel.dedupeGetOwnerRequest(serviceUnit);
            assertTrue(newRequest != oldRequest);
            assertTrue(getOwnerRequests.get(serviceUnit) == newRequest);

            // The previous unconditional removal would remove newRequest here.
            assertTrue(oldRequest.complete(brokerId1));

            assertTrue(getOwnerRequests.get(serviceUnit) == newRequest,
                    "A stale get-owner cleanup must not remove a newer request future");

            assertTrue(newRequest.complete(brokerId2));
            assertFalse(getOwnerRequests.containsKey(serviceUnit),
                    "The newer request must remove itself after completion");
        } finally {
            getOwnerRequests.remove(serviceUnit);
            oldRequest.cancel(false);
            if (newRequest != null) {
                newRequest.cancel(false);
            }
        }
    }

    @Test(priority = 1)
    public void testSkippedEventDoesNotRemoveNewGetOwnerRequest() throws Exception {
        ServiceUnitStateChannelImpl channel = (ServiceUnitStateChannelImpl) channel1;
        String serviceUnit = namespaceName + "/0x10000002_0x10000003";
        var getOwnerRequests = channel.getOwnerRequests();
        CompletableFuture<String> oldRequest = new CompletableFuture<>();
        CompletableFuture<String> newRequest = new CompletableFuture<>();
        try {
            overrideTableView(channel, serviceUnit, new ServiceUnitStateData(Owned, brokerId1, 1));
            getOwnerRequests.put(serviceUnit, oldRequest);
            oldRequest.whenComplete((__, ___) -> getOwnerRequests.put(serviceUnit, newRequest));

            channel.handleSkippedEvent(serviceUnit);

            assertEquals(oldRequest.getNow(null), brokerId1);
            assertTrue(getOwnerRequests.get(serviceUnit) == newRequest,
                    "A stale skipped-event cleanup must not remove a newer request future");
        } finally {
            getOwnerRequests.remove(serviceUnit);
            oldRequest.cancel(false);
            overrideTableView(channel, serviceUnit, null);
        }
    }

    @Test(priority = 1)
    public void testCompletedCleanupJobDoesNotRemoveNewCleanupJob() {
        ServiceUnitStateChannelImpl channel = (ServiceUnitStateChannelImpl) channel1;
        String broker = brokerId3;
        var cleanupJobs = channel.getCleanupJobs();
        cleanupJobs.remove(broker);
        channel.scheduleCleanup(broker, 60L);
        CompletableFuture<Void> oldJob = cleanupJobs.get(broker);
        assertNotNull(oldJob);

        CompletableFuture<Void> newJob = null;
        try {
            // Broker-creation handling removes a cleanup job before cancelling it. A later broker-deletion
            // event can therefore schedule a new job before the old job's completion cleanup runs.
            assertTrue(cleanupJobs.remove(broker, oldJob));
            channel.scheduleCleanup(broker, 60L);
            newJob = cleanupJobs.get(broker);
            assertNotNull(newJob);
            assertTrue(newJob != oldJob);

            // The previous unconditional removal would remove newJob here.
            assertTrue(oldJob.cancel(false));

            assertTrue(cleanupJobs.get(broker) == newJob,
                    "A stale cleanup job completion must not remove a newer cleanup job future");

            assertTrue(newJob.cancel(false));
            assertFalse(cleanupJobs.containsKey(broker),
                    "The newer cleanup job must remove itself after completion");
        } finally {
            cleanupJobs.remove(broker);
            oldJob.cancel(false);
            if (newJob != null) {
                newJob.cancel(false);
            }
        }
    }

    @Test(priority = 100)
    public void channelValidationTest()
            throws ExecutionException, InterruptedException, IllegalAccessException, PulsarServerException,
            TimeoutException {
        var channel = createChannel(pulsar);
        int errorCnt = validateChannelStart(channel);
        assertEquals(6, errorCnt);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> startFuture = executor.submit(() -> {
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

        Future<?> closeFuture = executor.submit(() -> {
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
            if (e.getCause() instanceof IllegalStateException) {
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

        // The channel of the broker that lost the assignment can miss the Assigning that the other one moves on
        var winner = assignedAddr1.equals(brokerId1) ? channel1 : channel2;
        var loser = winner == channel1 ? channel2 : channel1;
        validateHandlerCounters(winner, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateHandlerCounters(loser, Map.of(Assigning, 1), 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
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

        var tableView = getTableView(channel1);
        var spyTableView = spy(tableView);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(future).when(spyTableView).put(any(), any());

        try {
            setTableView(channel1, spyTableView);

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
        } finally {
            setTableView(channel1, tableView);
        }

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

        // channel1 can miss the Assigning that channel2 moves on, channel2 the first Assigning and the Releasing that
        // channel1 moves on
        validateHandlerCounters(channel1, Map.of(Assigning, 1), 2, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateHandlerCounters(channel2, Map.of(Assigning, 1, Releasing, 1),
                2, 0, 2, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        validateEventCounters(channel1, 1, 0, 0, 0, 1, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
    }

    @Test(priority = 4)
    public void testLaggingChannelMissesStatesItDoesNotActOn() throws Exception {
        if (!getTableView(channel2).isMetadataStoreBased()) {
            // The table view over the system topic hands every message to the channel
            return;
        }
        // Each broker has its own metadata store client. Hold the event thread of broker 2's, which hands channel2 its
        // notifications, while channel1 assigns the bundle to its broker.
        var store2 = pulsar2.getLocalMetadataStore();
        String holdPath = "/service-unit-state-channel-test-hold";
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        Runnable cancelListener = store2.registerCancellableListener(notification -> {
            if (notification.getPath().equals(holdPath) && notification.getType() == NotificationType.Created) {
                held.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        try {
            store2.put(holdPath, new byte[0], Optional.empty());
            assertTrue(held.await(10, TimeUnit.SECONDS));
            channel1.publishAssignEventAsync(bundle, brokerId1);
            waitUntilNewOwner(channel1, bundle, brokerId1);
            validateHandlerCounters(channel1, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        } finally {
            release.countDown();
            cancelListener.run();
        }
        store2.delete(holdPath, Optional.empty()).get(10, TimeUnit.SECONDS);

        // channel2 reads the bundle once it is Owned: it misses the Assigning, which only channel1 acts on
        waitUntilNewOwner(channel2, bundle, brokerId1);
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(bundle).get(10, TimeUnit.SECONDS));
        validateHandlerCounters(channel2, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    @Test(priority = 4)
    public void testCleanupWaitsForTheInitOfAServiceUnitThatLeftTheView() throws Exception {
        if (!getTableView(channel1).isMetadataStoreBased()) {
            // The cleanup clears the system topic table views directly
            return;
        }
        channel1.publishAssignEventAsync(bundle, brokerId1);
        waitUntilNewOwner(channel1, bundle, brokerId1);
        waitUntilNewOwner(channel2, bundle, brokerId1);

        // Once the bundle's deletion has removed it from channel1's view, hold channel1 before it handles the Init
        // event, until the cleanup first checks for the Init events the channels have yet to handle
        var tableView1 = getInitTrackingTableView(channel1);
        var held = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        tableView1.beforeInitHandled = serviceUnit -> {
            if (serviceUnit.equals(bundle)) {
                held.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        tableView1.beforeAwaitingInitCheck = release::countDown;
        var initCounter = ((ServiceUnitStateChannelImpl) channel1).handlerCounters.get(Init).getTotal();
        try {
            getTableView(channel2).delete(bundle).get(10, TimeUnit.SECONDS);
            assertTrue(held.await(10, TimeUnit.SECONDS));
            assertNull(tableView1.get(bundle));
            long inits = initCounter.get();

            cleanTableViews();
            assertEquals(inits + 1, initCounter.get());
        } finally {
            release.countDown();
            tableView1.beforeInitHandled = serviceUnit -> { };
            tableView1.beforeAwaitingInitCheck = () -> { };
        }
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

        var tableView = getTableView(channel2);
        var spyTableView = spy(tableView);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(future).when(spyTableView).put(any(), any());
        try {
            setTableView(channel2, spyTableView);
            FieldUtils.writeDeclaredField(channel1,
                    "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
            FieldUtils.writeDeclaredField(channel2,
                    "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
            Unload unload = new Unload(brokerId1, bundle, Optional.of(brokerId2));
            channel1.publishUnloadEventAsync(unload);
            // channel2 is broken. the ownership transfer won't be complete.
            waitUntilState(channel1, bundle);
            waitUntilState(channel2, bundle);
            var owner1 = channel1.getOwnerAsync(bundle);
            var owner2 = channel2.getOwnerAsync(bundle);

            assertTrue(owner1.isDone());
            assertEquals(brokerId2, owner1.get().get());
            assertFalse(owner2.isDone());

            assertEquals(0, getOwnerRequests1.size());
            assertEquals(1, getOwnerRequests2.size());

            // In 10 secs, the getOwnerAsync requests(lookup requests) should time out.
            Awaitility.await().atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertTrue(owner2.isCompletedExceptionally()));

            assertEquals(0, getOwnerRequests2.size());

            // recovered, check the monitor update state : Assigned -> Owned
            selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId1)));
        } finally {
            setTableView(channel2, tableView);
        }

        try {
            FieldUtils.writeDeclaredField(channel1,
                    "inFlightStateWaitingTimeInMillis", 1, true);
            FieldUtils.writeDeclaredField(channel2,
                    "inFlightStateWaitingTimeInMillis", 1, true);
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
        } finally {
            FieldUtils.writeDeclaredField(channel1,
                    "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
            FieldUtils.writeDeclaredField(channel2,
                    "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        }

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

        // The split's first 2 updates of the namespace bundles fail, the third succeeds
        int badVersionExceptionCount = 3;
        failNamespaceBundlesUpdates(namespaceName, badVersionExceptionCount - 1);

        // Assert child bundle ownerships in the channels.

        Split split = new Split(bundle, ownerAddr1.get(), Map.of(
                childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
        channel1.publishSplitEventAsync(split);

        waitUntilState(channel1, bundle, Init);
        waitUntilState(channel2, bundle, Init);

        // channel2 can miss the states that channel1 moves on
        validateHandlerCounters(channel1, 1, 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0);
        validateHandlerCounters(channel2, Map.of(Assigning, 1, Splitting, 1, Deleted, 1),
                1, 0, 3, 0, 0, 0, 1, 0, 0, 0, 1, 0, 1, 0);
        validateEventCounters(channel1, 1, 0, 1, 0, 0, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);
        // Verify the retry count
        verify(((ServiceUnitStateChannelImpl) channel1), times(badVersionExceptionCount))
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
                "inFlightStateWaitingTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 1, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1, true);
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
                0,
                0,
                0,
                0,
                0,
                0);

        try {
            disableChannels();
            overrideTableView(channel1, childBundle11, null);
            overrideTableView(channel2, childBundle11, null);
            overrideTableView(channel1, childBundle12, null);
            overrideTableView(channel2, childBundle12, null);
        } finally {
            enableChannels();
        }

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
        ServiceUnitStateChannelImpl channel1 = (ServiceUnitStateChannelImpl) this.channel1;
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


    @Test
    public void metadataStateMetricsTest() throws IllegalAccessException {
        ServiceUnitStateChannelImpl channel1 = (ServiceUnitStateChannelImpl) this.channel1;

        long now = System.currentTimeMillis();
        long oldTimestamp = now - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000) - 1;
        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEvent", SessionReestablished, true);
        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEventTimestamp", oldTimestamp, true);
        long beforeMetricsCall = System.currentTimeMillis();
        var metrics = channel1.getMetrics();
        long afterMetricsCall = System.currentTimeMillis();
        assertEquals(0, getMetric(metrics, "brk_sunit_state_chn_metadata_state").intValue());
        assertEquals(1, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_is_reestablished")
                .intValue());
        assertEquals(oldTimestamp, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_timestamp_ms")
                .longValue());
        long ageSeconds = getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_age_seconds")
                .longValue();
        long minAgeSeconds = TimeUnit.MILLISECONDS.toSeconds(beforeMetricsCall - oldTimestamp);
        long maxAgeSeconds = TimeUnit.MILLISECONDS.toSeconds(afterMetricsCall - oldTimestamp);
        assertTrue(ageSeconds >= minAgeSeconds && ageSeconds <= maxAgeSeconds,
                "Unexpected age seconds: " + ageSeconds + ", expected within [" + minAgeSeconds + ", "
                        + maxAgeSeconds + "]");

        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEvent", SessionReestablished, true);
        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEventTimestamp", now, true);
        metrics = channel1.getMetrics();
        assertEquals(1, getMetric(metrics, "brk_sunit_state_chn_metadata_state").intValue());
        assertEquals(1, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_is_reestablished")
                .intValue());
        assertEquals(now, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_timestamp_ms")
                .longValue());
        ageSeconds = getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_age_seconds")
                .longValue();
        assertTrue(ageSeconds >= 0 && ageSeconds <= 1, "Unexpected age seconds: " + ageSeconds);

        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEvent", SessionLost, true);
        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEventTimestamp", now, true);
        metrics = channel1.getMetrics();
        assertEquals(2, getMetric(metrics, "brk_sunit_state_chn_metadata_state").intValue());
        assertEquals(0, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_is_reestablished")
                .intValue());
        assertEquals(now, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_timestamp_ms")
                .longValue());
        ageSeconds = getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_age_seconds")
                .longValue();
        assertTrue(ageSeconds >= 0 && ageSeconds <= 1, "Unexpected age seconds: " + ageSeconds);

        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEvent", SessionReestablished, true);
        FieldUtils.writeDeclaredField(channel1, "lastMetadataSessionEventTimestamp", 0L, true);
        metrics = channel1.getMetrics();
        assertEquals(0, getMetric(metrics, "brk_sunit_state_chn_metadata_state").intValue());
        assertEquals(1, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_is_reestablished")
                .intValue());
        assertEquals(0L, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_timestamp_ms")
                .longValue());
        assertEquals(-1L, getMetric(metrics, "brk_sunit_state_chn_last_metadata_session_event_age_seconds")
                .longValue());
    }

    private static Number getMetric(List<Metrics> metrics, String metricName) {
        for (Metrics metric : metrics) {
            Object value = metric.getMetrics().get(metricName);
            if (value == null) {
                continue;
            }
            if (!(value instanceof Number)) {
                fail(metricName + " is not numeric: " + value);
            }
            return (Number) value;
        }
        fail("Missing " + metricName + " metric");
        return -1L;
    }

    @Test(priority = 8)
    public void handleBrokerCreationEventTest() throws IllegalAccessException {
        var cleanupJobs = getCleanupJobs(channel1);
        String broker = brokerId2;
        var future = new CompletableFuture<Void>();
        cleanupJobs.put(broker, future);
        ((ServiceUnitStateChannelImpl) channel1).handleBrokerRegistrationEvent(broker, NotificationType.Created);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(0, cleanupJobs.size());
            assertTrue(future.isCancelled());
        });

    }

    @Test(priority = 8)
    public void handleBrokerCreationEventDoesNotCancelNewCleanupJobTest() {
        ServiceUnitStateChannelImpl channel = (ServiceUnitStateChannelImpl) channel1;
        var cleanupJobs = channel.getCleanupJobs();
        String broker = brokerId2;
        CompletableFuture<Void> healthCheck = new CompletableFuture<>();
        cleanupJobs.remove(broker);
        channel.scheduleCleanup(broker, 60L);
        CompletableFuture<Void> oldJob = cleanupJobs.get(broker);
        assertNotNull(oldJob);

        clearInvocations(brokers);
        brokerHealthCheck.set(healthCheck);
        failBrokerHealthChecks.set(true);
        CompletableFuture<Void> newJob = null;
        try {
            channel.handleBrokerRegistrationEvent(broker, NotificationType.Created);
            verify(brokers, times(1)).healthcheckAsync(any());

            // The old cleanup can finish while the asynchronous health check is still pending. A later
            // broker-deletion event can then schedule a new cleanup job for the same broker.
            assertTrue(oldJob.complete(null));
            assertFalse(cleanupJobs.containsKey(broker));
            channel.scheduleCleanup(broker, 60L);
            newJob = cleanupJobs.get(broker);
            assertNotNull(newJob);
            assertTrue(newJob != oldJob);

            healthCheck.complete(null);

            CompletableFuture<Void> expectedNewJob = newJob;
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                assertTrue(cleanupJobs.get(broker) == expectedNewJob,
                        "A stale broker-creation callback must not remove a newer cleanup job");
                assertFalse(expectedNewJob.isCancelled());
            });

            assertTrue(newJob.cancel(false));
            assertFalse(cleanupJobs.containsKey(broker),
                    "The newer cleanup job must remove itself after cancellation");
        } finally {
            cleanupJobs.remove(broker);
            oldJob.cancel(false);
            if (newJob != null) {
                newJob.cancel(false);
            }
            failBrokerHealthChecks.set(false);
            brokerHealthCheck.set(FAILED_HEALTH_CHECK);
        }
    }

    @Test(priority = 9)
    public void handleBrokerDeletionEventTest() throws Exception {

        var cleanupJobs1 = getCleanupJobs(channel1);
        var cleanupJobs2 = getCleanupJobs(channel2);
        var leaderCleanupJobsTmp = spy(cleanupJobs1);
        var followerCleanupJobsTmp = spy(cleanupJobs2);
        ServiceUnitStateChannelImpl leaderChannel = (ServiceUnitStateChannelImpl) channel1;
        ServiceUnitStateChannelImpl followerChannel = (ServiceUnitStateChannelImpl) channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(brokerId2)) {
            leaderChannel = (ServiceUnitStateChannelImpl) channel2;
            followerChannel = (ServiceUnitStateChannelImpl) channel1;
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
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId2)));
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

        failBrokerHealthChecks.set(true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);


        leaderChannel.handleBrokerRegistrationEvent(brokerId2,
                NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(brokerId2,
                NotificationType.Deleted);

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
        failBrokerHealthChecks.set(false);

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
        failBrokerHealthChecks.set(true);
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
        failBrokerHealthChecks.set(false);

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

    @Test(priority = 2000)
    public void conflictAndCompactionTest() throws Exception {
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
        if (serviceUnitStateTableViewClassName.equals(
                ServiceUnitStateMetadataStoreTableViewImpl.class.getCanonicalName())) {
            // no compaction
            return;
        }

        var compactor = spy(pulsar1.getStrategicCompactor());
        Field strategicCompactorField = FieldUtils.getDeclaredField(PulsarService.class, "strategicCompactor", true);
        FieldUtils.writeField(strategicCompactorField, pulsar1, compactor, true);
        FieldUtils.writeField(strategicCompactorField, pulsar2, compactor, true);

        var threshold = admin.topicPolicies()
                .getCompactionThreshold(TOPIC);
        admin.topicPolicies()
                .setCompactionThreshold(TOPIC, 0);

        try {
            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(140, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        channel1.publishAssignEventAsync(bundle, brokerId1);
                        // Check now instead of waiting for the brokers' compaction monitor (every 10 s). The check
                        // calls the compactor before returning, so this stops after the first compaction.
                        pulsar1.getBrokerService().checkCompaction();
                        pulsar2.getBrokerService().checkCompaction();
                        verify(compactor, times(1))
                                .compact(eq(TOPIC), any());
                    });


            var channel3 = createChannel(pulsar);
            channel3.start();
            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(5, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(
                            channel3.getOwnerAsync(bundle).get(), Optional.of(brokerId1)));
            channel3.close();
        } finally {
            FieldUtils.writeDeclaredField(channel2,
                    "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
            if (threshold != null) {
                admin.topicPolicies()
                        .setCompactionThreshold(TOPIC, threshold);
            }
        }


    }

    @Test(priority = 11)
    public void ownerLookupCountTests() throws IllegalAccessException {
        try {
            disableChannels();
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
        } finally {
            enableChannels();
        }

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
                "inFlightStateWaitingTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel1,
                "stateTombstoneDelayTimeInMillis", 1, true);

        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1, true);
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

        var tableview = getTableView(channel1);
        var tableview2 = getTableView(channel2);
        var tableviewSpy = spy(tableview);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(future).when(tableviewSpy).put(any(), any());
        setTableView(channel2, tableviewSpy);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 3 * 1000, true);
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId2)));
        channel1.publishAssignEventAsync(bundle, brokerId2);
        // channel1 is broken. the assign won't be complete.
        waitUntilState(channel1, bundle, Assigning);
        waitUntilState(channel2, bundle, Assigning);
        var owner1 = channel1.getOwnerAsync(bundle);
        var owner2 = channel2.getOwnerAsync(bundle);

        assertTrue(owner1.isDone());
        assertFalse(owner2.isDone());

        // In 10 secs, the getOwnerAsync requests(lookup requests) should time out.
        Awaitility.await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(owner2.isCompletedExceptionally()));

        // recovered, check the monitor update state : Assigned -> Owned
        setTableView(channel2, tableview2);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1, true);

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
    public void splitTestWhenTableViewPutFails()
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

        var tableview = getTableView(channel1);
        var tableviewSpy = spy(tableview);
        var future = CompletableFuture.failedFuture(new RuntimeException());
        doReturn(future).when(tableviewSpy).put(any(), any());
        setTableView(channel1, tableviewSpy);
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
        setTableView(channel1, tableview);
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1, true);


        var leader = channel1.isChannelOwnerAsync().get() ? channel1 : channel2;
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId1)));
        waitUntilStateWithMonitor(leader, bundle, Init);
        waitUntilStateWithMonitor(channel1, bundle, Init);
        waitUntilStateWithMonitor(channel2, bundle, Init);

        var ownerAddr1 = channel1.getOwnerAsync(bundle);
        var ownerAddr2 = channel2.getOwnerAsync(bundle);

        assertTrue(ownerAddr1.get().isEmpty());
        assertTrue(ownerAddr2.get().isEmpty());


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


        try {
            disableChannels();
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
        } finally {
            enableChannels();
        }
    }

    @Test(priority = 16)
    public void testGetOwnerAsync() throws Exception {
        try {
            disableChannels();
            overrideTableView(channel1, bundle, new ServiceUnitStateData(Owned, brokerId1, 1));
            var owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId1, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Owned, brokerId2, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId2, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Assigning, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle);
            assertFalse(owner.isDone());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Assigning, brokerId2, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId2, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Releasing, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle);
            assertFalse(owner.isDone());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Releasing, brokerId2, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId2, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Releasing, null, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(Optional.empty(), owner.get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Splitting, null, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId1, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Splitting, null, brokerId2, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(brokerId2, owner.get().get());

            overrideTableView(channel1, bundle, new ServiceUnitStateData(Free, null, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(Optional.empty(), owner.get());

            overrideTableView(channel1, bundle, null);
            owner = channel1.getOwnerAsync(bundle);
            //assertTrue(owner.isDone());
            assertEquals(Optional.empty(), owner.get());

            overrideTableView(channel1, bundle1, new ServiceUnitStateData(Deleted, null, brokerId1, 1));
            owner = channel1.getOwnerAsync(bundle1);
            //assertTrue(owner.isDone());
            assertTrue(owner.isCompletedExceptionally());
        } finally {
            enableChannels();
        }

    }

    @Test(priority = 17)
    public void splitAndRetryFailureTest() throws Exception {
        channel1.publishAssignEventAsync(bundle3, brokerId1);
        waitUntilNewOwner(channel1, bundle3, brokerId1);
        waitUntilNewOwner(channel2, bundle3, brokerId1);
        var ownerAddr1 = channel1.getOwnerAsync(bundle3).get();
        var ownerAddr2 = channel2.getOwnerAsync(bundle3).get();
        assertEquals(ownerAddr1, Optional.of(brokerId1));
        assertEquals(ownerAddr2, Optional.of(brokerId1));
        assertTrue(ownerAddr1.isPresent());

        // The split gives up after 7 failed updates of the namespace bundles. The split the monitor retries fails
        // the last 2, then succeeds.
        int badVersionExceptionCount = NamespaceService.BUNDLE_SPLIT_RETRY_LIMIT + 2;
        AtomicInteger count = failNamespaceBundlesUpdates(namespaceName2, badVersionExceptionCount);

        // Assert child bundle ownerships in the channels.

        Split split = new Split(bundle3, ownerAddr1.get(), Map.of(
                childBundle1Range, Optional.empty(), childBundle2Range, Optional.empty()));
        channel1.publishSplitEventAsync(split);

        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 1, true);

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    assertEquals(2, count.get());
                });
        ServiceUnitStateChannelImpl leader =
                (ServiceUnitStateChannelImpl) (channel1.isChannelOwnerAsync().get() ? channel1 : channel2);
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId1)));
        leader.monitorOwnerships(List.of(brokerId1, brokerId2));

        waitUntilState(leader, bundle3, Init);
        waitUntilState(channel1, bundle3, Init);
        waitUntilState(channel2, bundle3, Init);

        waitUntilNewOwner(channel1, childBundle31, brokerId1);
        waitUntilNewOwner(channel1, childBundle32, brokerId1);
        waitUntilNewOwner(channel2, childBundle31, brokerId1);
        waitUntilNewOwner(channel2, childBundle32, brokerId1);

        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle31).get());
        assertEquals(Optional.of(brokerId1), channel1.getOwnerAsync(childBundle32).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle31).get());
        assertEquals(Optional.of(brokerId1), channel2.getOwnerAsync(childBundle32).get());


        // channel2 can miss the states that channel1 moves on
        validateHandlerCounters(channel1, 1, 0, 3, 0, 0, 0, 2, 1, 0, 0, 1, 0, 1, 0);
        validateHandlerCounters(channel2, Map.of(Assigning, 1, Splitting, 2, Deleted, 1),
                1, 0, 3, 0, 0, 0, 2, 0, 0, 0, 1, 0, 1, 0);
        validateEventCounters(channel1, 1, 0, 1, 0, 0, 0);
        validateEventCounters(channel2, 0, 0, 0, 0, 0, 0);


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
                0,
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

    @Test(priority = 18)
    public void testOverrideInactiveBrokerStateData()
            throws IllegalAccessException, ExecutionException, InterruptedException, TimeoutException {

        ServiceUnitStateChannelImpl leaderChannel = (ServiceUnitStateChannelImpl) channel1;
        ServiceUnitStateChannelImpl followerChannel = (ServiceUnitStateChannelImpl) channel2;
        String leader = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader, leader2);
        if (leader.equals(brokerId2)) {
            leaderChannel = (ServiceUnitStateChannelImpl) channel2;
            followerChannel = (ServiceUnitStateChannelImpl) channel1;
        }

        String broker = brokerId1;

        // test override states
        String releasingBundle = "public/releasing/0xfffffff0_0xffffffff";
        String splittingBundle = bundle;
        String assigningBundle = "public/assigning/0xfffffff0_0xffffffff";
        String freeBundle = "public/free/0xfffffff0_0xffffffff";
        String deletedBundle = "public/deleted/0xfffffff0_0xffffffff";
        String ownedBundle = "public/owned/0xfffffff0_0xffffffff";
        try {
            disableChannels();
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
        } finally {
            enableChannels();
        }

        // test stable metadata state
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId2)));
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        followerChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        FieldUtils.writeDeclaredField(followerChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);

        failBrokerHealthChecks.set(true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);
        followerChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);


        waitUntilNewOwner(channel2, releasingBundle, brokerId2);
        waitUntilNewOwner(channel2, childBundle11, brokerId2);
        waitUntilNewOwner(channel2, childBundle12, brokerId2);
        waitUntilNewOwner(channel2, assigningBundle, brokerId2);
        waitUntilNewOwner(channel2, ownedBundle, brokerId2);
        assertEquals(Optional.empty(), channel2.getOwnerAsync(freeBundle).get());
        assertTrue(channel2.getOwnerAsync(deletedBundle).isCompletedExceptionally());
        // The split ends with the parent bundle Deleted, then removed
        waitUntilState(channel2, splittingBundle, Init);
        assertTrue(channel2.getOwnerAsync(splittingBundle).get().isEmpty());

        // clean-up
        FieldUtils.writeDeclaredField(leaderChannel, "maxCleanupDelayTimeInSecs", 3 * 60, true);
        cleanTableViews();
        failBrokerHealthChecks.set(false);
    }

    @Test(priority = 19)
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
        String releasingBundle1 = "public/releasing1/0xfffffff0_0xffffffff";
        String releasingBundle2 = "public/releasing2/0xfffffff0_0xffffffff";
        String splittingBundle = bundle;
        String assigningBundle1 = "public/assigning1/0xfffffff0_0xffffffff";
        String assigningBundle2 = "public/assigning2/0xfffffff0_0xffffffff";
        String freeBundle = "public/free/0xfffffff0_0xffffffff";
        String deletedBundle = "public/deleted/0xfffffff0_0xffffffff";
        String ownedBundle1 = "public/owned1/0xfffffff0_0xffffffff";
        String ownedBundle2 = "public/owned2SourceBundle/0xfffffff0_0xffffffff";
        String ownedBundle3 = "public/owned3/0xfffffff0_0xffffffff";
        String inactiveBroker = "broker-inactive-1";
        try {
            disableChannels();
            overrideTableViews(releasingBundle1,
                    new ServiceUnitStateData(Releasing, broker, brokerId2, 1));
            overrideTableViews(releasingBundle2,
                    new ServiceUnitStateData(Releasing, brokerId2, brokerId3, 1));
            overrideTableViews(splittingBundle,
                    new ServiceUnitStateData(Splitting, null, broker,
                            Map.of(childBundle1Range, Optional.empty(),
                                    childBundle2Range, Optional.empty()), 1));
            overrideTableViews(assigningBundle1,
                    new ServiceUnitStateData(Assigning, broker, null, 1));
            overrideTableViews(assigningBundle2,
                    new ServiceUnitStateData(Assigning, broker, brokerId2, 1));
            overrideTableViews(freeBundle,
                    new ServiceUnitStateData(Free, null, broker, 1));
            overrideTableViews(deletedBundle,
                    new ServiceUnitStateData(Deleted, null, broker, 1));
            overrideTableViews(ownedBundle1,
                    new ServiceUnitStateData(Owned, broker, null, 1));
            overrideTableViews(ownedBundle2,
                    new ServiceUnitStateData(Owned, broker, inactiveBroker, 1));
            overrideTableViews(ownedBundle3,
                    new ServiceUnitStateData(Owned, inactiveBroker, broker, 1));
        } finally {
            enableChannels();
        }


        // test stable metadata state
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId2)));
        FieldUtils.writeDeclaredField(leaderChannel, "inFlightStateWaitingTimeInMillis",
                -1, true);
        FieldUtils.writeDeclaredField(followerChannel, "inFlightStateWaitingTimeInMillis",
                -1, true);
        ((ServiceUnitStateChannelImpl) leaderChannel)
                .monitorOwnerships(List.of(brokerId1, brokerId2, "broker-3"));

        ServiceUnitStateChannel finalLeaderChannel = leaderChannel;
        Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> getCleanupJobs(finalLeaderChannel).isEmpty());


        waitUntilNewOwner(channel2, releasingBundle1, brokerId2);
        waitUntilNewOwner(channel2, releasingBundle2, brokerId2);
        // The split ends with the parent bundle Deleted, then removed
        waitUntilState(channel2, splittingBundle, Init);
        assertTrue(channel2.getOwnerAsync(splittingBundle).get().isEmpty());
        waitUntilNewOwner(channel2, childBundle11, brokerId2);
        waitUntilNewOwner(channel2, childBundle12, brokerId2);
        waitUntilNewOwner(channel2, assigningBundle1, brokerId2);
        waitUntilNewOwner(channel2, assigningBundle2, brokerId2);
        assertTrue(channel2.getOwnerAsync(freeBundle).get().isEmpty());
        assertTrue(channel2.getOwnerAsync(deletedBundle).isCompletedExceptionally());
        waitUntilNewOwner(channel2, ownedBundle1, broker);
        waitUntilNewOwner(channel2, ownedBundle2, broker);
        waitUntilNewOwner(channel2, ownedBundle3, brokerId2);

        validateMonitorCounters(leaderChannel,
                1,
                0,
                6,
                0,
                1,
                0,
                0);

        // clean-up
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        FieldUtils.writeDeclaredField(channel2,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        cleanTableViews();
    }

    @Test(priority = 20)
    public void testActiveGetOwner() throws Exception {

        // case 1: the bundle owner is empty
        String broker = brokerId2;
        String bundle = "public/owned/0xfffffff0_0xffffffff";
        try {
            disableChannels();
            overrideTableViews(bundle, null);
            assertEquals(Optional.empty(), channel1.getOwnerAsync(bundle).get());

            // case 2: the bundle ownership is transferring, and the dst broker is not the channel owner
            overrideTableViews(bundle,
                    new ServiceUnitStateData(Releasing, broker, brokerId1, 1));
            assertEquals(Optional.of(broker), channel1.getOwnerAsync(bundle).get());


            // case 3: the bundle ownership is transferring, and the dst broker is the channel owner
            overrideTableViews(bundle,
                    new ServiceUnitStateData(Assigning, brokerId1, brokerId2, 1));
            assertFalse(channel1.getOwnerAsync(bundle).isDone());

            // case 4: the bundle ownership is found
            overrideTableViews(bundle,
                    new ServiceUnitStateData(Owned, broker, null, 1));
            var owner = channel1.getOwnerAsync(bundle).get(5, TimeUnit.SECONDS).get();
            assertEquals(owner, broker);
        } finally {
            enableChannels();
        }

        // case 5: the owner lookup gets delayed
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 1000, true);
        var delayedFuture = new CompletableFuture<Object>();
        brokerLookups.put(broker, delayedFuture);

        // verify the owner eventually returns in inFlightStateWaitingTimeInMillis.
        long start = System.currentTimeMillis();
        var delayedOwner = channel1.getOwnerAsync(bundle);
        assertFalse(delayedOwner.isDone());
        delayedFuture.complete(Optional.of(broker));
        assertEquals(broker, delayedOwner.get().get());
        long elapsed = System.currentTimeMillis() - start;
        assertTrue(elapsed < 1000);

        // case 6: the owner is inactive
        brokerLookups.put(broker, CompletableFuture.completedFuture(Optional.empty()));

        // verify getOwnerAsync times out
        start = System.currentTimeMillis();
        var ex = expectThrows(ExecutionException.class, () -> channel1.getOwnerAsync(bundle).get());
        assertTrue(ex.getCause() instanceof IllegalStateException);
        assertTrue(System.currentTimeMillis() - start >= 1000);

        if (!getTableView(channel1).isMetadataStoreBased()) {
            try {
                // verify getOwnerAsync returns immediately when not registered
                registry.unregister();
                start = System.currentTimeMillis();
                assertEquals(broker, channel1.getOwnerAsync(bundle).get().get());
                elapsed = System.currentTimeMillis() - start;
                assertTrue(elapsed < 1000);
            } finally {
                registry.registerAsync().join();
            }
        }

        if (getTableView(channel1).isMetadataStoreBased()) {
            try {
                // verify getOwnerAsync returns immediately when not registered
                registry.unregister();
                channel1.getOwnerAsync(bundle).get().get();
                fail("Request should fail because it is in the state that tries to reconnect to the metadata store");
            } catch (Exception e) {
                Throwable actEx = FutureUtil.unwrapCompletionException(e);
                assertTrue(actEx instanceof MetadataStoreException);
                assertTrue(actEx.getMessage().contains("reconnect to the metadata store."));
            } finally {
                registry.registerAsync().join();
            }
        }


        // case 7: the ownership cleanup(no new owner) by the leader channel
        selectedBroker.set(CompletableFuture.completedFuture(Optional.empty()));
        ServiceUnitStateChannelImpl leaderChannel = (ServiceUnitStateChannelImpl) channel1;
        String leader1 = channel1.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        String leader2 = channel2.getChannelOwnerAsync().get(2, TimeUnit.SECONDS).get();
        assertEquals(leader1, leader2);
        if (leader1.equals(brokerId2)) {
            leaderChannel = (ServiceUnitStateChannelImpl) channel2;
        }
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        failBrokerHealthChecks.set(true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        // verify the ownership cleanup, and channel's getOwnerAsync returns empty result without timeout
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 20 * 1000, true);
        start = System.currentTimeMillis();
        assertTrue(channel1.getOwnerAsync(bundle).get().isEmpty());
        waitUntilState(channel1, bundle, Init);
        waitUntilState(channel2, bundle, Init);

        assertTrue(System.currentTimeMillis() - start < 20_000);
        failBrokerHealthChecks.set(false);
        // case 8: simulate ownership cleanup(brokerId1 as the new owner) by the leader channel
        try {
            disableChannels();
            overrideTableViews(bundle,
                    new ServiceUnitStateData(Owned, broker, null, 1));
        } finally {
            enableChannels();
        }
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId1)));
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        FieldUtils.writeDeclaredField(leaderChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 1000), true);
        getCleanupJobs(leaderChannel).clear();
        failBrokerHealthChecks.set(true);
        leaderChannel.handleBrokerRegistrationEvent(broker, NotificationType.Deleted);

        // verify the ownership cleanup, and channel's getOwnerAsync returns brokerId1 without timeout
        start = System.currentTimeMillis();
        assertEquals(brokerId1, channel1.getOwnerAsync(bundle).get().get());
        assertTrue(System.currentTimeMillis() - start < 20_000);

        // test clean-up
        FieldUtils.writeDeclaredField(channel1,
                "inFlightStateWaitingTimeInMillis", 30 * 1000, true);
        cleanTableViews();
        failBrokerHealthChecks.set(false);
    }

    @Test(priority = 21)
    public void testGetOwnershipEntrySetBeforeChannelStart() {
        var tmpChannel = new ServiceUnitStateChannelImpl(pulsar1);
        try {
            tmpChannel.getOwnershipEntrySet();
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
            assertEquals("Invalid channel state:Constructed", e.getMessage());
        }
    }

    @Test(priority = 22)
    public void unloadTimeoutCheckTest()
            throws Exception {

        String topic = "persistent://" + namespaceName + "/test-topic";
        NamespaceBundle bundleName = pulsar.getNamespaceService().getBundle(TopicName.get(topic));
        var releasing = new ServiceUnitStateData(Releasing, pulsar2.getBrokerId(), pulsar1.getBrokerId(), 1);

        try {
            disableChannels();
            overrideTableView(channel1, bundleName.toString(), releasing);
            var topicFuture = pulsar1.getBrokerService().getOrCreateTopic(topic);
            topicFuture.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (!(e.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException && e.getMessage()
                    .contains("Please redo the lookup"))) {
                fail();
            }
        } finally {
            enableChannels();
        }

        pulsar1.getBrokerService()
                .unloadServiceUnit(bundleName, true, true, 5,
                        TimeUnit.SECONDS).get(2, TimeUnit.SECONDS);
    }

    @Test(priority = 23)
    public void testCleanSystemTopicOwnership()
            throws Exception {
        String topic = "persistent://pulsar/system/test-system-topic";
        NamespaceBundle bundleName = pulsar.getNamespaceService().getBundle(TopicName.get(topic));
        var releasing = new ServiceUnitStateData(Releasing, pulsar2.getBrokerId(), pulsar1.getBrokerId(), 1);
        selectedBroker.set(CompletableFuture.completedFuture(Optional.of(brokerId1)));

        try {
            disableChannels();
            overrideTableView(channel1, bundleName.toString(), releasing);
        } finally {
            enableChannels();
        }

        channel1.cleanOwnerships();
        channel2.cleanOwnerships();
    }

    @Test(priority = 24)
    public void testHandleExistingResolvesAssigningStateOnChannelRestart()
            throws Exception {
        // Regression test for: handleExisting() must immediately resolve an Assigning state
        // targeting this broker to Owned, simulating the broker-restart recovery scenario.
        //
        // When a broker restarts, its ServiceUnitStateChannel calls handleExisting() for each
        // entry in the table view during start(). Without the fix, Assigning states were silently
        // ignored, leaving bundles stuck until the ownership monitor rescued them after
        // inFlightStateWaitingTimeInMillis (default 30s). The fix is verified by asserting
        // that Owned state appears within 15s — shorter than the 30s monitor threshold —
        // which proves handleExisting() drove the resolution, not the ownership monitor.

        // Case 1: Assigning targeting brokerId1 with no source broker
        //         (fresh assignment after a Free override when no broker was available)
        String assigningBundle1 = "public/test-existing-assigning1/0xfffffff0_0xffffffff";
        var assigningData1 = new ServiceUnitStateData(Assigning, brokerId1, null, 1);

        // Case 2: Assigning targeting brokerId1 with a source broker
        //         (transfer interrupted mid-flight by broker restart)
        String assigningBundle2 = "public/test-existing-assigning2/0xfffffff0_0xffffffff";
        var assigningData2 = new ServiceUnitStateData(Assigning, brokerId1, brokerId2, 1);

        // Pre-populate the Assigning states in the tableview while channels are disabled.
        // This is required for the metadata store implementation: the conflict resolver
        // checks that the existing versionId == (new versionId - 1), so Owned(v=2) is
        // only accepted when Assigning(v=1) is already stored. Without pre-population,
        // shouldKeepLeft(null, Owned(v=2)) returns true (conflict) and the put is silently
        // dropped, leaving the bundle stuck in the Init state.
        try {
            disableChannels();
            overrideTableViews(assigningBundle1, assigningData1);
            overrideTableViews(assigningBundle2, assigningData2);
        } finally {
            enableChannels();
        }

        var handleExistingMethod = ServiceUnitStateChannelImpl.class
                .getDeclaredMethod("handleExisting", String.class, ServiceUnitStateData.class);
        handleExistingMethod.setAccessible(true);

        // Simulate restart: handleExisting() is called by ServiceUnitStateTableView.start() for
        // each entry present in the tableview snapshot when the channel starts up.
        handleExistingMethod.invoke(channel1, assigningBundle1, assigningData1);
        handleExistingMethod.invoke(channel1, assigningBundle2, assigningData2);

        try {
            // Both bundles must reach Owned state within 15s (< inFlightStateWaitingTimeInMillis 30s).
            // Without the fix, the tableview state would remain Assigning until the monitor runs at ~30s.
            Awaitility.await().atMost(15, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS)
                    .untilAsserted(() -> {
                        assertEquals(Owned, state(getTableView(channel1).get(assigningBundle1)));
                        assertEquals(Owned, state(getTableView(channel2).get(assigningBundle1)));
                        assertEquals(Owned, state(getTableView(channel1).get(assigningBundle2)));
                        assertEquals(Owned, state(getTableView(channel2).get(assigningBundle2)));
                    });
        } finally {
            cleanTableViews();
        }
    }

    @Test
    public void testCleanupDrainsAssignmentsAndRejectsNewOnes() throws Exception {
        var channel = (ServiceUnitStateChannelImpl) channel1;
        var tableView = channel.getTableView();
        var delayedTableView = spy(tableView);
        String serviceUnit = namespaceName + "/0x00000000_0xffffffff";
        String lateServiceUnit = namespaceName2 + "/0x00000000_0xffffffff";
        var pendingAssignment = new CompletableFuture<ServiceUnitStateData>();
        var published = new CompletableFuture<Void>();
        var disabled = new CompletableFuture<Void>();
        selectedBroker.set(CompletableFuture.completedFuture(Optional.empty()));
        doAnswer(invocation -> {
            ServiceUnitStateData data = invocation.getArgument(1);
            if (data.state() == Assigning && !data.force()) {
                pendingAssignment.complete(data);
                return published;
            }
            return tableView.put(serviceUnit, data);
        }).when(delayedTableView).put(eq(serviceUnit), any(ServiceUnitStateData.class));
        doAnswer(invocation -> {
            invocation.callRealMethod();
            disabled.complete(null);
            return null;
        }).when(channel).disable();
        channel.setTableView(delayedTableView);
        @Cleanup("shutdownNow")
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Void> cleanup = null;
        try {
            channel.publishAssignEventAsync(serviceUnit, brokerId1);
            var assignment = pendingAssignment.get(10, TimeUnit.SECONDS);
            cleanup = CompletableFuture.runAsync(channel::cleanOwnerships, executor);
            disabled.get(10, TimeUnit.SECONDS);
            assertFalse(cleanup.isDone(), "Cleanup must wait for the accepted assignment write");
            var rejected = expectThrows(ExecutionException.class,
                    () -> channel.publishAssignEventAsync(lateServiceUnit, brokerId1).get(10, TimeUnit.SECONDS));
            assertTrue(rejected.getCause() instanceof IllegalStateException);
            assertNull(tableView.get(lateServiceUnit));
            tableView.put(serviceUnit, assignment).get(10, TimeUnit.SECONDS);
            published.complete(null);
            cleanup.get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                var remaining = tableView.get(serviceUnit);
                assertTrue(remaining == null || remaining.state() == Free,
                        "An accepted assignment must be included in cleanup: " + remaining);
            });
        } finally {
            published.complete(null);
            if (cleanup != null) {
                cleanup.get(10, TimeUnit.SECONDS);
            }
            doCallRealMethod().when(channel).disable();
            channel.setTableView(tableView);
            channel.enable();
            tableView.delete(serviceUnit).get(10, TimeUnit.SECONDS);
            tableView.delete(lateServiceUnit).get(10, TimeUnit.SECONDS);
        }
    }

    @DataProvider
    public Object[][] cleanupDestination() {
        return new Object[][]{{false}, {true}};
    }

    @Test(dataProvider = "cleanupDestination")
    public void testCleanupRetriesConcurrentAssignment(boolean hasDestinationBroker) throws Exception {
        var channel = (ServiceUnitStateChannelImpl) channel1;
        var tableView = channel.getTableView();
        var delayedTableView = spy(tableView);
        String serviceUnit = namespaceName + "/0x00000000_0xffffffff";
        var pendingOwned = new CompletableFuture<ServiceUnitStateData>();
        var ownedPublished = new CompletableFuture<Void>();
        var conflictingOverride = new CompletableFuture<ServiceUnitStateData>();
        selectedBroker.set(CompletableFuture.completedFuture(
                hasDestinationBroker ? Optional.of(brokerId2) : Optional.empty()));
        doAnswer(invocation -> {
            ServiceUnitStateData data = invocation.getArgument(1);
            if (data.state() == Owned && !data.force()) {
                pendingOwned.complete(data);
                return ownedPublished;
            }
            var owned = pendingOwned.getNow(null);
            if (data.force() && owned != null && data.versionId() == owned.versionId()) {
                // Publish the delayed Owned update before the cleanup's same-version override. Both writes
                // succeed, but the real conflict resolver discards the stale cleanup update.
                return tableView.put(serviceUnit, owned).thenCompose(__ -> {
                    ownedPublished.complete(null);
                    conflictingOverride.complete(data);
                    return tableView.put(serviceUnit, data);
                });
            }
            return tableView.put(serviceUnit, data);
        }).when(delayedTableView).put(eq(serviceUnit), any(ServiceUnitStateData.class));
        channel.setTableView(delayedTableView);
        try {
            var assignment = channel.publishAssignEventAsync(serviceUnit, brokerId1);
            var owned = pendingOwned.get(10, TimeUnit.SECONDS);
            assertEquals(2L, owned.versionId());
            channel.cleanOwnerships();
            assertEquals(owned.versionId(), conflictingOverride.get(10, TimeUnit.SECONDS).versionId());
            Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                var remaining = tableView.get(serviceUnit);
                if (hasDestinationBroker) {
                    assertNotNull(remaining);
                    assertEquals(Owned, remaining.state());
                    assertEquals(brokerId2, remaining.dstBroker());
                } else {
                    assertTrue(remaining == null || remaining.state() == Free,
                            "Cleanup must not leave the concurrently assigned bundle owned by the stopped broker: "
                                    + remaining);
                }
            });
            assertTrue(assignment.isDone());
        } finally {
            ownedPublished.complete(null);
            channel.setTableView(tableView);
            channel.enable();
            tableView.delete(serviceUnit).get(10, TimeUnit.SECONDS);
        }
    }

    private static ConcurrentHashMap<String, CompletableFuture<String>> getOwnerRequests(
            ServiceUnitStateChannel channel) throws IllegalAccessException {
        return (ConcurrentHashMap<String, CompletableFuture<String>>)
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

    /**
     * Fails the next {@code failures} updates of the namespace bundles with a BadVersion, as a concurrent update of
     * the namespace's local policies would. The failures are programmed into the mock ZooKeeper instead of stubbed
     * on the broker's NamespaceService spy: Mockito stubs the method of the next call on a spy, from any thread, and
     * broker threads call the spy at any time.
     *
     * @return the number of failures left
     */
    private AtomicInteger failNamespaceBundlesUpdates(String namespace, int failures) {
        String localPoliciesPath = "/admin/local-policies/" + namespace;
        remainingNamespaceBundlesUpdateFailures.set(failures);
        for (int i = 0; i < failures; i++) {
            // The mock ZooKeeper checks its programmed failures on its single executor thread
            mockZooKeeper.failConditional(KeeperException.Code.BADVERSION, (op, path) -> {
                if (op == MockZooKeeper.Op.SET && path.equals(localPoliciesPath)
                        && remainingNamespaceBundlesUpdateFailures.get() > 0) {
                    remainingNamespaceBundlesUpdateFailures.decrementAndGet();
                    return true;
                }
                return false;
            });
        }
        return remainingNamespaceBundlesUpdateFailures;
    }


    @SuppressWarnings("deprecation")
    private static void waitUntilNewChannelOwner(ServiceUnitStateChannel channel, String oldOwner) {
        Awaitility.await()
                .pollDelay(0, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    CompletableFuture<Optional<String>> owner = channel.getChannelOwnerAsync();
                    if (!owner.isDone()) {
                        return false;
                    }

                    return !StringUtils.equals(oldOwner, owner.get().orElse(null));
                });
    }

    @SuppressWarnings("deprecation")
    private static void waitUntilOwnerChanges(ServiceUnitStateChannel channel, String serviceUnit, String oldOwner) {
        Awaitility.await()
                .pollDelay(0, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    CompletableFuture<Optional<String>> owner = channel.getOwnerAsync(serviceUnit);
                    if (!owner.isDone()) {
                        return false;
                    }
                    return !StringUtils.equals(oldOwner, owner.get().orElse(null));
                });
    }

    @SuppressWarnings("deprecation")
    private static void waitUntilNewOwner(ServiceUnitStateChannel channel, String serviceUnit, String newOwner) {
        Awaitility.await()
                .pollDelay(0, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
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

    private static ServiceUnitStateTableView getTableView(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        return ((ServiceUnitStateChannelImpl) channel).getTableView();
    }

    private static void setTableView(ServiceUnitStateChannel channel,
                                     ServiceUnitStateTableView tableView)
            throws IllegalAccessException {
        ((ServiceUnitStateChannelImpl) channel).setTableView(tableView);
    }

    private static void waitUntilState(ServiceUnitStateChannel channel, String key)
            throws IllegalAccessException {
        var tv = getTableView(channel);
        Awaitility.await()
                .pollDelay(0, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
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
        var tv = getTableView(channel);
        Awaitility.await()
                .pollDelay(0, TimeUnit.MILLISECONDS)
                .pollInterval(10, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    ServiceUnitStateData data = tv.get(key);
                    ServiceUnitState actual = state(data);
                    return actual == expected;
                });
    }

    private void waitUntilStateWithMonitor(ServiceUnitStateChannel channel, String key, ServiceUnitState expected)
            throws IllegalAccessException {
        var tv = getTableView(channel);
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

    private void cleanTableViews()
            throws IllegalAccessException {
        if (serviceUnitStateTableViewClassName.equals(ServiceUnitStateTableViewImpl.class.getCanonicalName())) {
            cleanTableView(channel1);
            cleanTableView(channel2);
            return;
        }
        // Each channel handles an Init event for each service unit that leaves its view, whichever channel deletes it,
        // once the deletion reaches it, and a channel can still be handling the last events of a test. Wait until
        // the channels have handled all of them, so that none counts in the next test.
        var channels = List.of(channel1, channel2);
        Set<String> serviceUnits = new HashSet<>();
        for (var channel : channels) {
            ((ServiceUnitStateChannelImpl) channel).getOwnerRequests().clear();
            getTableView(channel).entrySet().forEach(entry -> serviceUnits.add(entry.getKey()));
        }
        // Ignores the service units that are already deleted
        var tableView = getTableView(channel1);
        for (String serviceUnit : serviceUnits) {
            tableView.delete(serviceUnit).join();
        }
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            for (var channel : channels) {
                assertTrue(getTableView(channel).entrySet().isEmpty());
                assertFalse(getInitTrackingTableView(channel).isAwaitingInit());
            }
        });
    }

    private void cleanTableView(ServiceUnitStateChannel channel) throws IllegalAccessException {
        var getOwnerRequests = (Map<String, CompletableFuture<String>>)
                FieldUtils.readField(channel, "getOwnerRequests", true);
        getOwnerRequests.clear();
        var tv = getTableView(channel);
        var tableview = (TableView<ServiceUnitStateData>)
                FieldUtils.readField(tv, "tableview", true);
        var cache = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tableview, "data", true);
        cache.clear();
    }

    private void overrideTableViews(String serviceUnit, ServiceUnitStateData val) throws IllegalAccessException {
        overrideTableView(channel1, serviceUnit, val);
        overrideTableView(channel2, serviceUnit, val);
    }

    @Test(enabled = false)
    public static void overrideTableView(ServiceUnitStateChannel channel,
                                         String serviceUnit, ServiceUnitStateData val) throws IllegalAccessException {
        var getOwnerRequests = (Map<String, CompletableFuture<String>>)
                FieldUtils.readField(channel, "getOwnerRequests", true);
        getOwnerRequests.clear();
        var tv = getTableView(channel);

        var handlerCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "handlerCounters", true);

        var cur = tv.get(serviceUnit);
        if (cur != null) {
            long intCountStart = handlerCounters.get(Init).getTotal().get();
            var deletedCount = new AtomicLong(0);
            tv.delete(serviceUnit).join();
            deletedCount.incrementAndGet();
            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        assertEquals(
                                handlerCounters.get(Init).getTotal().get()
                                        - intCountStart, deletedCount.get());
                        assertNull(tv.get(serviceUnit));
                    });
        }



        if (val != null) {
            long stateCountStart = handlerCounters.get(state(val)).getTotal().get();
            var stateCount = new AtomicLong(0);
            tv.put(serviceUnit, val).join();
            stateCount.incrementAndGet();

            Awaitility.await()
                    .pollInterval(200, TimeUnit.MILLISECONDS)
                    .atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        assertEquals(
                                handlerCounters.get(state(val)).getTotal().get()
                                        - stateCountStart, stateCount.get());
                        assertEquals(val, tv.get(serviceUnit));
                    });
        }


    }

    private static void cleanOpsCounters(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        var handlerCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "handlerCounters", true);

        for (var val : handlerCounters.values()) {
            val.getFailure().set(0);
            val.getTotal().set(0);
        }

        var eventCounters =
                (Map<ServiceUnitStateChannelImpl.EventType, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "eventCounters", true);

        for (var val : eventCounters.values()) {
            val.getFailure().set(0);
            val.getTotal().set(0);
        }

        var ownerLookUpCounters =
                (Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters>)
                        FieldUtils.readDeclaredField(channel, "ownerLookUpCounters", true);

        for (var val : ownerLookUpCounters.values()) {
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
        ((ServiceUnitStateChannelImpl) channel).handleMetadataSessionEvent(SessionReestablished);
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
        validateHandlerCounters(channel, Map.of(), assignedT, assignedF, ownedT, ownedF, releasedT, releasedF,
                splittingT, splittingF, freeT, freeF, initT, initF, deletedT, deletedF);
    }

    /**
     * Validates the handler counters of a channel that can miss up to {@code missable} events of some states.
     *
     * <p>A table view over the metadata store reads a service unit's state when it handles its notification, so a
     * channel that falls behind, or whose own write completes late, sees only the latest state. It can miss a state
     * that it does not act on, when the channel that acts on it moves on before the read. It never misses a state it
     * acts on, since nothing moves on before it does. The table view over the system topic hands every message to
     * the channel. A missed state is not handled later, so the counters are final once the channel has handled the
     * last state of each service unit.
     */
    private static void validateHandlerCounters(ServiceUnitStateChannel channel,
                                                Map<ServiceUnitState, Integer> missable,
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
        var maxMissed = getTableView(channel).isMetadataStoreBased() ? missable : Map.<ServiceUnitState, Integer>of();

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .untilAsserted(() -> { // wait until true
                    assertHandled(handlerCounters, Assigning, assignedT, maxMissed);
                    assertEquals(assignedF, handlerCounters.get(Assigning).getFailure().get());
                    assertHandled(handlerCounters, Owned, ownedT, maxMissed);
                    assertEquals(ownedF, handlerCounters.get(Owned).getFailure().get());
                    assertHandled(handlerCounters, Releasing, releasedT, maxMissed);
                    assertEquals(releasedF, handlerCounters.get(Releasing).getFailure().get());
                    assertHandled(handlerCounters, Splitting, splittingT, maxMissed);
                    assertEquals(splittingF, handlerCounters.get(Splitting).getFailure().get());
                    assertHandled(handlerCounters, Free, freeT, maxMissed);
                    assertEquals(freeF, handlerCounters.get(Free).getFailure().get());
                    assertHandled(handlerCounters, Init, initT, maxMissed);
                    assertEquals(initF, handlerCounters.get(Init).getFailure().get());
                    assertHandled(handlerCounters, Deleted, deletedT, maxMissed);
                    assertEquals(deletedF, handlerCounters.get(Deleted).getFailure().get());
                });
    }

    private static void assertHandled(Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters> handlerCounters,
                                      ServiceUnitState state, long expected,
                                      Map<ServiceUnitState, Integer> maxMissed) {
        long handled = handlerCounters.get(state).getTotal().get();
        long minHandled = expected - maxMissed.getOrDefault(state, 0);
        assertTrue(handled >= minHandled && handled <= expected,
                state + " events handled: " + handled + ", expected " + minHandled + " to " + expected);
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
            throws IllegalAccessException, PulsarServerException {
        var tmpChannel = new ServiceUnitStateChannelImpl(pulsar);
        FieldUtils.writeDeclaredField(tmpChannel, "ownershipMonitorDelayTimeInSecs", 5, true);
        var channel = spy(tmpChannel);

        doReturn(loadManagerContext).when(channel).getContext();
        doReturn(registry).when(channel).getBrokerRegistry();
        doReturn(loadManager).when(channel).getLoadManager();
        doReturn(pulsarAdmin).when(channel).getPulsarAdmin();


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

    private void disableChannels() {
        ((ServiceUnitStateChannelImpl) channel1).disable();
        ((ServiceUnitStateChannelImpl) channel2).disable();
    }

    private void enableChannels() {
        ((ServiceUnitStateChannelImpl) channel1).enable();
        ((ServiceUnitStateChannelImpl) channel2).enable();
    }

    private static InitTrackingMetadataStoreTableView getInitTrackingTableView(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        return (InitTrackingMetadataStoreTableView) getTableView(channel);
    }

    /**
     * The metadata store table view, recording the service units whose Init event the channel has yet to handle.
     *
     * <p>A service unit leaves the view before the view hands the channel its Init event, so neither the view nor the
     * channel's handler counters tell whether the channel still has to handle it. The record keeps a service unit from
     * the time the channel handles it into the view until the channel has handled its Init event.
     */
    public static class InitTrackingMetadataStoreTableView extends ServiceUnitStateMetadataStoreTableViewImpl {
        private final Set<String> serviceUnitsAwaitingInit = ConcurrentHashMap.newKeySet();
        private volatile Consumer<String> beforeInitHandled = serviceUnit -> { };
        private volatile Runnable beforeAwaitingInitCheck = () -> { };

        @Override
        public void start(PulsarService pulsar,
                          BiConsumer<String, ServiceUnitStateData> tailItemListener,
                          BiConsumer<String, ServiceUnitStateData> existingItemListener,
                          BiConsumer<String, ServiceUnitStateData> outdatedItemListeners)
                throws MetadataStoreException {
            super.start(pulsar, (serviceUnit, data) -> {
                if (data != null) {
                    serviceUnitsAwaitingInit.add(serviceUnit);
                    tailItemListener.accept(serviceUnit, data);
                    return;
                }
                // The service unit has left the view
                try {
                    beforeInitHandled.accept(serviceUnit);
                    tailItemListener.accept(serviceUnit, null);
                } finally {
                    serviceUnitsAwaitingInit.remove(serviceUnit);
                }
            }, (serviceUnit, data) -> {
                serviceUnitsAwaitingInit.add(serviceUnit);
                existingItemListener.accept(serviceUnit, data);
            }, (serviceUnit, data) -> {
                try {
                    outdatedItemListeners.accept(serviceUnit, data);
                } finally {
                    // The view drops an outdated service unit without an Init event
                    serviceUnitsAwaitingInit.remove(serviceUnit);
                }
            });
        }

        boolean isAwaitingInit() {
            beforeAwaitingInitCheck.run();
            return !serviceUnitsAwaitingInit.isEmpty();
        }
    }
}
