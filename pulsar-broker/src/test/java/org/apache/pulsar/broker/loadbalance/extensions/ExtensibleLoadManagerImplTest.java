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
package org.apache.pulsar.broker.loadbalance.extensions;

import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Admin;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Bandwidth;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.MsgRate;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Sessions;
import static org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision.Reason.Topics;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Failure;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Skip;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Label.Success;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Balanced;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannel;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.broker.namespace.LookupOptions;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.resources.TenantResources;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit test for {@link ExtensibleLoadManagerImpl}.
 */
@Slf4j
@Test(groups = "broker")
public class ExtensibleLoadManagerImplTest extends MockedPulsarServiceBaseTest {

    private PulsarService pulsar1;
    private PulsarService pulsar2;

    private PulsarTestContext additionalPulsarTestContext;

    private PulsarResources resources;

    private ExtensibleLoadManagerImpl primaryLoadManager;

    private ExtensibleLoadManagerImpl secondaryLoadManager;

    private ServiceUnitStateChannelImpl channel1;
    private ServiceUnitStateChannelImpl channel2;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setAllowAutoTopicCreation(true);
        conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        conf.setLoadBalancerSheddingEnabled(false);
        super.internalSetup(conf);
        pulsar1 = pulsar;
        ServiceConfiguration defaultConf = getDefaultConf();
        defaultConf.setAllowAutoTopicCreation(true);
        defaultConf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        defaultConf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        defaultConf.setLoadBalancerSheddingEnabled(false);
        additionalPulsarTestContext = createAdditionalPulsarTestContext(defaultConf);
        pulsar2 = additionalPulsarTestContext.getPulsarService();

        ExtensibleLoadManagerWrapper primaryLoadManagerWrapper =
                (ExtensibleLoadManagerWrapper) pulsar1.getLoadManager().get();
        primaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(primaryLoadManagerWrapper, "loadManager", true));
        FieldUtils.writeField(primaryLoadManagerWrapper, "loadManager", primaryLoadManager, true);

        ExtensibleLoadManagerWrapper secondaryLoadManagerWrapper =
                (ExtensibleLoadManagerWrapper) pulsar2.getLoadManager().get();
        secondaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(secondaryLoadManagerWrapper, "loadManager", true));
        FieldUtils.writeField(secondaryLoadManagerWrapper, "loadManager", secondaryLoadManager, true);

        channel1 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(primaryLoadManager, "serviceUnitStateChannel", true);
        channel2 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(secondaryLoadManager, "serviceUnitStateChannel", true);

        admin.clusters().createCluster(this.conf.getClusterName(),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"),
                        Sets.newHashSet(this.conf.getClusterName())));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default",
                Sets.newHashSet(this.conf.getClusterName()));
    }

    protected void beforePulsarStart(PulsarService pulsar) throws Exception {
        if (resources == null) {
            MetadataStoreExtended localStore = pulsar.createLocalMetadataStore(null);
            MetadataStoreExtended configStore = (MetadataStoreExtended) pulsar.createConfigurationMetadataStore(null);
            resources = new PulsarResources(localStore, configStore);
        }
        this.createNamespaceIfNotExists(resources, NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                NamespaceName.SYSTEM_NAMESPACE);
    }

    protected void createNamespaceIfNotExists(PulsarResources resources,
                                              String publicTenant,
                                              NamespaceName ns) throws Exception {
        TenantResources tr = resources.getTenantResources();
        NamespaceResources nsr = resources.getNamespaceResources();

        if (!tr.tenantExists(publicTenant)) {
            tr.createTenant(publicTenant,
                    TenantInfo.builder()
                            .adminRoles(Sets.newHashSet(conf.getSuperUserRoles()))
                            .allowedClusters(Sets.newHashSet(conf.getClusterName()))
                            .build());
        }

        if (!nsr.namespaceExists(ns)) {
            Policies nsp = new Policies();
            nsp.replication_clusters = Collections.singleton(conf.getClusterName());
            nsr.createPolicies(ns, nsp);
        }
    }

    @Override
    @AfterClass
    protected void cleanup() throws Exception {
        pulsar1 = null;
        pulsar2.close();
        super.internalCleanup();
        this.additionalPulsarTestContext.close();
    }

    @BeforeMethod
    protected void initializeState() throws IllegalAccessException {
        reset(primaryLoadManager, secondaryLoadManager);
        cleanTableView(channel1);
        cleanTableView(channel2);
    }

    @Test
    public void testAssignInternalTopic() throws Exception {
        Optional<BrokerLookupData> brokerLookupData1 = primaryLoadManager.assign(
                Optional.of(TopicName.get(ServiceUnitStateChannelImpl.TOPIC)),
                getBundleAsync(pulsar1, TopicName.get(ServiceUnitStateChannelImpl.TOPIC)).get()).get();
        Optional<BrokerLookupData> brokerLookupData2 = secondaryLoadManager.assign(
                Optional.of(TopicName.get(ServiceUnitStateChannelImpl.TOPIC)),
                getBundleAsync(pulsar1, TopicName.get(ServiceUnitStateChannelImpl.TOPIC)).get()).get();
        assertEquals(brokerLookupData1, brokerLookupData2);
        assertTrue(brokerLookupData1.isPresent());

        LeaderElectionService leaderElectionService = (LeaderElectionService)
                FieldUtils.readField(channel1, "leaderElectionService", true);
        Optional<LeaderBroker> currentLeader = leaderElectionService.getCurrentLeader();
        assertTrue(currentLeader.isPresent());
        assertEquals(brokerLookupData1.get().getWebServiceUrl(), currentLeader.get().getServiceUrl());
    }

    @Test
    public void testAssign() throws Exception {
        TopicName topicName = TopicName.get("test-assign");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle).get();
        assertTrue(brokerLookupData.isPresent());
        log.info("Assign the bundle {} to {}", bundle, brokerLookupData);
        // Should get owner info from channel.
        Optional<BrokerLookupData> brokerLookupData1 = secondaryLoadManager.assign(Optional.empty(), bundle).get();
        assertEquals(brokerLookupData, brokerLookupData1);

        verify(primaryLoadManager, times(1)).getBrokerSelectionStrategy();
        verify(secondaryLoadManager, times(0)).getBrokerSelectionStrategy();

        Optional<LookupResult> lookupResult = pulsar2.getNamespaceService()
                .getBrokerServiceUrlAsync(topicName, null).get();
        assertTrue(lookupResult.isPresent());
        assertEquals(lookupResult.get().getLookupData().getHttpUrl(), brokerLookupData.get().getWebServiceUrl());

        Optional<URL> webServiceUrl = pulsar2.getNamespaceService()
                .getWebServiceUrl(bundle, LookupOptions.builder().requestHttps(false).build());
        assertTrue(webServiceUrl.isPresent());
        assertEquals(webServiceUrl.get().toString(), brokerLookupData.get().getWebServiceUrl());
    }

    @Test
    public void testCheckOwnershipAsync() throws Exception {
        NamespaceBundle bundle = getBundleAsync(pulsar1, TopicName.get("test-check-ownership")).get();
        // 1. The bundle is never assigned.
        assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());

        // 2. Assign the bundle to a broker.
        Optional<BrokerLookupData> lookupData = primaryLoadManager.assign(Optional.empty(), bundle).get();
        assertTrue(lookupData.isPresent());
        if (lookupData.get().getPulsarServiceUrl().equals(pulsar1.getBrokerServiceUrl())) {
            assertTrue(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        } else {
            assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
            assertTrue(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        }

    }

    @Test
    public void testFilter() throws Exception {
        TopicName topicName = TopicName.get("test-filter");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();

        doReturn(List.of(new BrokerFilter() {
            @Override
            public String name() {
                return "Mock broker filter";
            }

            @Override
            public void initialize(PulsarService pulsar) {
                // No-op
            }

            @Override
            public Map<String, BrokerLookupData> filter(Map<String, BrokerLookupData> brokers,
                                                        ServiceUnitId serviceUnit,
                                                        LoadManagerContext context) throws BrokerFilterException {
                brokers.remove(pulsar1.getLookupServiceAddress());
                return brokers;
            }

        })).when(primaryLoadManager).getBrokerFilterPipeline();

        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle).get();
        assertTrue(brokerLookupData.isPresent());
        assertEquals(brokerLookupData.get().getWebServiceUrl(), pulsar2.getWebServiceAddress());
    }

    @Test
    public void testFilterHasException() throws Exception {
        TopicName topicName = TopicName.get("test-filter-has-exception");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();

        doReturn(List.of(new MockBrokerFilter() {
            @Override
            public Map<String, BrokerLookupData> filter(Map<String, BrokerLookupData> brokers,
                                                        ServiceUnitId serviceUnit,
                                                        LoadManagerContext context) throws BrokerFilterException {
                brokers.clear();
                throw new BrokerFilterException("Test");
            }
        })).when(primaryLoadManager).getBrokerFilterPipeline();

        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle).get();
        assertTrue(brokerLookupData.isPresent());
    }

    @Test(timeOut = 30 * 1000)
    public void testUnloadAdminAPI() throws Exception {
        TopicName topicName = TopicName.get("test-unload");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();

        String broker = admin.lookups().lookupTopic(topicName.toString());
        log.info("Assign the bundle {} to {}", bundle, broker);

        checkOwnershipState(broker, bundle);
        admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange());
        assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(secondaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());

        broker = admin.lookups().lookupTopic(topicName.toString());
        log.info("Assign the bundle {} to {}", bundle, broker);

        String dstBrokerUrl = pulsar1.getLookupServiceAddress();
        String dstBrokerServiceUrl;
        if (broker.equals(pulsar1.getBrokerServiceUrl())) {
            dstBrokerUrl = pulsar2.getLookupServiceAddress();
            dstBrokerServiceUrl = pulsar2.getBrokerServiceUrl();
        } else {
            dstBrokerServiceUrl = pulsar1.getBrokerServiceUrl();
        }
        checkOwnershipState(broker, bundle);

        admin.namespaces().unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), dstBrokerUrl);

        assertEquals(admin.lookups().lookupTopic(topicName.toString()), dstBrokerServiceUrl);

        // Test transfer to current broker.
        try {
            admin.namespaces()
                    .unloadNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange(), dstBrokerUrl);
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("cannot be transfer to same broker"));
        }
    }

    private void checkOwnershipState(String broker, NamespaceBundle bundle)
            throws ExecutionException, InterruptedException {
        var targetLoadManager = secondaryLoadManager;
        var otherLoadManager = primaryLoadManager;
        if (broker.equals(pulsar1.getBrokerServiceUrl())) {
            targetLoadManager = primaryLoadManager;
            otherLoadManager = secondaryLoadManager;
        }
        assertTrue(targetLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
        assertFalse(otherLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
    }


    @Test
    public void testMoreThenOneFilter() throws Exception {
        TopicName topicName = TopicName.get("test-filter-has-exception");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();

        String lookupServiceAddress1 = pulsar1.getLookupServiceAddress();
        doReturn(List.of(new MockBrokerFilter() {
            @Override
            public Map<String, BrokerLookupData> filter(Map<String, BrokerLookupData> brokers,
                                                        ServiceUnitId serviceUnit,
                                                        LoadManagerContext context) throws BrokerFilterException {
                brokers.remove(lookupServiceAddress1);
                return brokers;
            }
        },new MockBrokerFilter() {
            @Override
            public Map<String, BrokerLookupData> filter(Map<String, BrokerLookupData> brokers,
                                                        ServiceUnitId serviceUnit,
                                                        LoadManagerContext context) throws BrokerFilterException {
                brokers.clear();
                throw new BrokerFilterException("Test");
            }
        })).when(primaryLoadManager).getBrokerFilterPipeline();

        Optional<BrokerLookupData> brokerLookupData = primaryLoadManager.assign(Optional.empty(), bundle).get();
        assertTrue(brokerLookupData.isPresent());
        assertEquals(brokerLookupData.get().getWebServiceUrl(), pulsar2.getWebServiceAddress());
    }

    @Test
    public void testGetMetrics() throws Exception {
        {
            var brokerLoadMetrics = (AtomicReference<List<Metrics>>)
                    FieldUtils.readDeclaredField(primaryLoadManager, "brokerLoadMetrics", true);
            BrokerLoadData loadData = new BrokerLoadData();
            SystemResourceUsage usage = new SystemResourceUsage();
            var cpu = new ResourceUsage(1.0, 100.0);
            var memory = new ResourceUsage(800.0, 200.0);
            var directMemory = new ResourceUsage(2.0, 100.0);
            var bandwidthIn = new ResourceUsage(3.0, 100.0);
            var bandwidthOut = new ResourceUsage(4.0, 100.0);
            usage.setCpu(cpu);
            usage.setMemory(memory);
            usage.setDirectMemory(directMemory);
            usage.setBandwidthIn(bandwidthIn);
            usage.setBandwidthOut(bandwidthOut);
            loadData.update(usage, 1, 2, 3, 4, 5, 6, conf);
            brokerLoadMetrics.set(loadData.toMetrics(pulsar.getAdvertisedAddress()));
        }
        {
            var unloadMetrics = (AtomicReference<List<Metrics>>)
                    FieldUtils.readDeclaredField(primaryLoadManager, "unloadMetrics", true);
            UnloadCounter unloadCounter = new UnloadCounter();
            FieldUtils.writeDeclaredField(unloadCounter, "unloadBrokerCount", 2l, true);
            FieldUtils.writeDeclaredField(unloadCounter, "unloadBundleCount", 3l, true);
            FieldUtils.writeDeclaredField(unloadCounter, "loadAvg", 1.5, true);
            FieldUtils.writeDeclaredField(unloadCounter, "loadStd", 0.3, true);
            FieldUtils.writeDeclaredField(unloadCounter, "breakdownCounters", Map.of(
                    Success, new LinkedHashMap<>() {{
                        put(Overloaded, new MutableLong(1));
                        put(Underloaded, new MutableLong(2));
                    }},
                    Skip, new LinkedHashMap<>() {{
                        put(Balanced, new MutableLong(3));
                        put(NoBundles, new MutableLong(4));
                        put(CoolDown, new MutableLong(5));
                        put(OutDatedData, new MutableLong(6));
                        put(NoLoadData, new MutableLong(7));
                        put(NoBrokers, new MutableLong(8));
                        put(Unknown, new MutableLong(9));
                    }},
                    Failure, Map.of(
                            Unknown, new MutableLong(10))
            ), true);
            unloadMetrics.set(unloadCounter.toMetrics(pulsar.getAdvertisedAddress()));
        }
        {
            var splitMetrics = (AtomicReference<List<Metrics>>)
                    FieldUtils.readDeclaredField(primaryLoadManager, "splitMetrics", true);
            SplitCounter splitCounter = new SplitCounter();
            FieldUtils.writeDeclaredField(splitCounter, "splitCount", 35l, true);
            FieldUtils.writeDeclaredField(splitCounter, "breakdownCounters", Map.of(
                    SplitDecision.Label.Success, Map.of(
                            Topics, new AtomicLong(1),
                            Sessions, new AtomicLong(2),
                            MsgRate, new AtomicLong(3),
                            Bandwidth, new AtomicLong(4),
                            Admin, new AtomicLong(5)),
                    SplitDecision.Label.Failure, Map.of(
                            SplitDecision.Reason.Unknown, new AtomicLong(6))
            ), true);
            splitMetrics.set(splitCounter.toMetrics(pulsar.getAdvertisedAddress()));
        }

        {
            AssignCounter assignCounter = new AssignCounter();
            assignCounter.incrementSuccess();
            assignCounter.incrementEmpty();
            assignCounter.incrementEmpty();
            assignCounter.incrementSkip();
            assignCounter.incrementSkip();
            assignCounter.incrementSkip();
            FieldUtils.writeDeclaredField(primaryLoadManager, "assignCounter", assignCounter, true);
        }

        {

            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupCnt", 1, true);
            FieldUtils.writeDeclaredField(channel1, "totalServiceUnitTombstoneCleanupCnt", 2, true);
            FieldUtils.writeDeclaredField(channel1, "totalOrphanServiceUnitCleanupCnt", 3, true);
            FieldUtils.writeDeclaredField(channel1, "totalCleanupErrorCnt", new AtomicLong(4), true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupScheduledCnt", 5, true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupIgnoredCnt", 6, true);
            FieldUtils.writeDeclaredField(channel1, "totalInactiveBrokerCleanupCancelledCnt", 7, true);

            Map<ServiceUnitState, AtomicLong> ownerLookUpCounters = new LinkedHashMap<>();
            Map<ServiceUnitState, ServiceUnitStateChannelImpl.Counters> handlerCounters = new LinkedHashMap<>();
            Map<ServiceUnitStateChannelImpl.EventType, ServiceUnitStateChannelImpl.Counters> eventCounters =
                    new LinkedHashMap<>();
            int i = 1;
            int j = 0;
            for (var state : ServiceUnitState.values()) {
                ownerLookUpCounters.put(state, new AtomicLong(i));
                handlerCounters.put(state,
                        new ServiceUnitStateChannelImpl.Counters(
                                new AtomicLong(j + 1), new AtomicLong(j + 2)));
                i++;
                j += 2;
            }
            i = 0;
            for (var type : ServiceUnitStateChannelImpl.EventType.values()) {
                eventCounters.put(type,
                        new ServiceUnitStateChannelImpl.Counters(
                                new AtomicLong(i + 1), new AtomicLong(i + 2)));
                i += 2;
            }
            FieldUtils.writeDeclaredField(channel1, "ownerLookUpCounters", ownerLookUpCounters, true);
            FieldUtils.writeDeclaredField(channel1, "eventCounters", eventCounters, true);
            FieldUtils.writeDeclaredField(channel1, "handlerCounters", handlerCounters, true);
        }

        var expected = Set.of(
                """
                        dimensions=[{broker=localhost, metric=loadBalancing}], metrics=[{brk_lb_bandwidth_in_usage=3.0, brk_lb_bandwidth_out_usage=4.0, brk_lb_cpu_usage=1.0, brk_lb_directMemory_usage=2.0, brk_lb_memory_usage=400.0}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=loadBalancing}], metrics=[{brk_lb_resource_usage=4.0}]
                        dimensions=[{broker=localhost, feature=max, metric=loadBalancing}], metrics=[{brk_lb_resource_usage=0.04}]
                        dimensions=[{broker=localhost, metric=bundleUnloading}], metrics=[{brk_lb_unload_broker_total=2, brk_lb_unload_bundle_total=3}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Unknown, result=Failure}], metrics=[{brk_lb_unload_broker_breakdown_total=10}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Balanced, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoBundles, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=4}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=CoolDown, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=5}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=OutDatedData, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=6}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoLoadData, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=7}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=NoBrokers, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=8}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Unknown, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=9}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Overloaded, result=Success}], metrics=[{brk_lb_unload_broker_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=Underloaded, result=Success}], metrics=[{brk_lb_unload_broker_breakdown_total=2}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=bundleUnloading, stat=avg}], metrics=[{brk_lb_resource_usage_stats=1.5}]
                        dimensions=[{broker=localhost, feature=max_ema, metric=bundleUnloading, stat=std}], metrics=[{brk_lb_resource_usage_stats=0.3}]
                        dimensions=[{broker=localhost, metric=bundlesSplit}], metrics=[{brk_lb_bundles_split_total=35}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Topics, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Sessions, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=2}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=MsgRate, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Bandwidth, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=4}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Admin, result=Success}], metrics=[{brk_lb_bundles_split_breakdown_total=5}]
                        dimensions=[{broker=localhost, metric=bundlesSplit, reason=Unknown, result=Failure}], metrics=[{brk_lb_bundles_split_breakdown_total=6}]
                        dimensions=[{broker=localhost, metric=assign, result=Empty}], metrics=[{brk_lb_assign_broker_breakdown_total=2}]
                        dimensions=[{broker=localhost, metric=assign, result=Skip}], metrics=[{brk_lb_assign_broker_breakdown_total=3}]
                        dimensions=[{broker=localhost, metric=assign, result=Success}], metrics=[{brk_lb_assign_broker_breakdown_total=1}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Init}], metrics=[{brk_sunit_state_chn_owner_lookup_total=1}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Free}], metrics=[{brk_sunit_state_chn_owner_lookup_total=2}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Owned}], metrics=[{brk_sunit_state_chn_owner_lookup_total=3}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Assigning}], metrics=[{brk_sunit_state_chn_owner_lookup_total=4}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Releasing}], metrics=[{brk_sunit_state_chn_owner_lookup_total=5}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Splitting}], metrics=[{brk_sunit_state_chn_owner_lookup_total=6}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, state=Deleted}], metrics=[{brk_sunit_state_chn_owner_lookup_total=7}]
                        dimensions=[{broker=localhost, event=Assign, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=1}]
                        dimensions=[{broker=localhost, event=Assign, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=2}]
                        dimensions=[{broker=localhost, event=Split, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=3}]
                        dimensions=[{broker=localhost, event=Split, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=4}]
                        dimensions=[{broker=localhost, event=Unload, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=5}]
                        dimensions=[{broker=localhost, event=Unload, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_event_publish_ops_total=6}]
                        dimensions=[{broker=localhost, event=Init, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=1}]
                        dimensions=[{broker=localhost, event=Init, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=2}]
                        dimensions=[{broker=localhost, event=Free, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=3}]
                        dimensions=[{broker=localhost, event=Free, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=4}]
                        dimensions=[{broker=localhost, event=Owned, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=5}]
                        dimensions=[{broker=localhost, event=Owned, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=6}]
                        dimensions=[{broker=localhost, event=Assigning, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=7}]
                        dimensions=[{broker=localhost, event=Assigning, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=8}]
                        dimensions=[{broker=localhost, event=Releasing, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=9}]
                        dimensions=[{broker=localhost, event=Releasing, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=10}]
                        dimensions=[{broker=localhost, event=Splitting, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=11}]
                        dimensions=[{broker=localhost, event=Splitting, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=12}]
                        dimensions=[{broker=localhost, event=Deleted, metric=sunitStateChn, result=Total}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=13}]
                        dimensions=[{broker=localhost, event=Deleted, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_subscribe_ops_total=14}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Failure}], metrics=[{brk_sunit_state_chn_cleanup_ops_total=4}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Skip}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=6}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Cancel}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=7}]
                        dimensions=[{broker=localhost, metric=sunitStateChn, result=Schedule}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=5}]
                        dimensions=[{broker=localhost, metric=sunitStateChn}], metrics=[{brk_sunit_state_chn_inactive_broker_cleanup_ops_total=1, brk_sunit_state_chn_orphan_su_cleanup_ops_total=3, brk_sunit_state_chn_su_tombstone_cleanup_ops_total=2}]
                        """.split("\n"));
        var actual = primaryLoadManager.getMetrics().stream().map(m -> m.toString()).collect(Collectors.toSet());
        assertEquals(actual, expected);
    }

    private static abstract class MockBrokerFilter implements BrokerFilter {

        @Override
        public String name() {
            return "Mock-broker-filter";
        }

        @Override
        public void initialize(PulsarService pulsar) {
            // No-op
        }

    }

    private static void cleanTableView(ServiceUnitStateChannel channel)
            throws IllegalAccessException {
        var tv = (TableViewImpl<ServiceUnitStateData>)
                FieldUtils.readField(channel, "tableview", true);
        var cache = (ConcurrentMap<String, ServiceUnitStateData>)
                FieldUtils.readField(tv, "data", true);
        cache.clear();
    }

    private CompletableFuture<NamespaceBundle> getBundleAsync(PulsarService pulsar, TopicName topic) {
        return pulsar.getNamespaceService().getBundleAsync(topic);
    }
}
