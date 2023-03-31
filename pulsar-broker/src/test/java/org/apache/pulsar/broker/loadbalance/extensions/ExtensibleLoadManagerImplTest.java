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
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.CoolDown;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.HitCount;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBrokers;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoBundles;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.NoLoadData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.OutDatedData;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Overloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Underloaded;
import static org.apache.pulsar.broker.loadbalance.extensions.models.UnloadDecision.Reason.Unknown;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.Sets;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitState;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensions.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensions.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensions.models.AssignCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitCounter;
import org.apache.pulsar.broker.loadbalance.extensions.models.SplitDecision;
import org.apache.pulsar.broker.loadbalance.extensions.models.UnloadCounter;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.loadbalance.extensions.store.LoadDataStore;
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
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.testcontainers.shaded.org.awaitility.Awaitility;
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
        conf.setForceDeleteNamespaceAllowed(true);
        conf.setAllowAutoTopicCreation(true);
        conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        conf.setLoadBalancerSheddingEnabled(false);
        super.internalSetup(conf);
        pulsar1 = pulsar;
        ServiceConfiguration defaultConf = getDefaultConf();
        defaultConf.setAllowAutoTopicCreation(true);
        defaultConf.setForceDeleteNamespaceAllowed(true);
        defaultConf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        defaultConf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        defaultConf.setLoadBalancerSheddingEnabled(false);
        additionalPulsarTestContext = createAdditionalPulsarTestContext(defaultConf);
        pulsar2 = additionalPulsarTestContext.getPulsarService();

        setPrimaryLoadManager();

        setSecondaryLoadManager();

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
    protected void initializeState() throws PulsarAdminException {
        admin.namespaces().unload("public/default");
        reset(primaryLoadManager, secondaryLoadManager);
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

    @Test(timeOut = 30 * 1000)
    public void testSplitBundleAdminAPI() throws Exception {
        String namespace = "public/default";
        String topic = "persistent://" + namespace + "/test-split";
        admin.topics().createPartitionedTopic(topic, 10);
        BundlesData bundles = admin.namespaces().getBundles(namespace);
        int numBundles = bundles.getNumBundles();
        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();

        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);

        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;

        admin.namespaces().splitNamespaceBundle(namespace, firstBundle, true, null);

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        assertEquals(bundlesData.getNumBundles(), numBundles + 1);
        String lowBundle = String.format("0x%08x", bundleRanges.get(0));
        String midBundle = String.format("0x%08x", mid);
        String highBundle = String.format("0x%08x", bundleRanges.get(1));
        assertTrue(bundlesData.getBoundaries().contains(lowBundle));
        assertTrue(bundlesData.getBoundaries().contains(midBundle));
        assertTrue(bundlesData.getBoundaries().contains(highBundle));

        // Test split bundle with invalid bundle range.
        try {
            admin.namespaces().splitNamespaceBundle(namespace, "invalid", true, null);
            fail();
        } catch (PulsarAdminException ex) {
            assertTrue(ex.getMessage().contains("Invalid bundle range"));
        }
    }

    @Test(timeOut = 30 * 1000)
    public void testSplitBundleWithSpecificPositionAdminAPI() throws Exception {
        String namespace = "public/default";
        String topic = "persistent://" + namespace + "/test-split-with-specific-position";
        admin.topics().createPartitionedTopic(topic, 10);
        BundlesData bundles = admin.namespaces().getBundles(namespace);
        int numBundles = bundles.getNumBundles();

        var bundleRanges = bundles.getBoundaries().stream().map(Long::decode).sorted().toList();

        String firstBundle = bundleRanges.get(0) + "_" + bundleRanges.get(1);

        long mid = bundleRanges.get(0) + (bundleRanges.get(1) - bundleRanges.get(0)) / 2;
        long splitPosition = mid + 100;

        admin.namespaces().splitNamespaceBundle(namespace, firstBundle, true,
                "specified_positions_divide", List.of(bundleRanges.get(0), bundleRanges.get(1), splitPosition));

        BundlesData bundlesData = admin.namespaces().getBundles(namespace);
        assertEquals(bundlesData.getNumBundles(), numBundles + 1);
        String lowBundle = String.format("0x%08x", bundleRanges.get(0));
        String midBundle = String.format("0x%08x", splitPosition);
        String highBundle = String.format("0x%08x", bundleRanges.get(1));
        assertTrue(bundlesData.getBoundaries().contains(lowBundle));
        assertTrue(bundlesData.getBoundaries().contains(midBundle));
        assertTrue(bundlesData.getBoundaries().contains(highBundle));
    }
    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespaceBundle() throws Exception {
        TopicName topicName = TopicName.get("test-delete-namespace-bundle");
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();

        String broker = admin.lookups().lookupTopic(topicName.toString());
        log.info("Assign the bundle {} to {}", bundle, broker);

        checkOwnershipState(broker, bundle);

        admin.namespaces().deleteNamespaceBundle(topicName.getNamespace(), bundle.getBundleRange());
        assertFalse(primaryLoadManager.checkOwnershipAsync(Optional.empty(), bundle).get());
    }

    @Test(timeOut = 30 * 1000)
    public void testDeleteNamespace() throws Exception {
        String namespace = "public/test-delete-namespace";
        TopicName topicName = TopicName.get(namespace + "/test-delete-namespace-topic");
        admin.namespaces().createNamespace(namespace);
        admin.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(this.conf.getClusterName()));
        assertTrue(admin.namespaces().getNamespaces("public").contains(namespace));
        admin.topics().createPartitionedTopic(topicName.toString(), 2);
        admin.lookups().lookupTopic(topicName.toString());
        NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
        try {
            admin.namespaces().deleteNamespaceBundle(namespace, bundle.getBundleRange());
            fail();
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Cannot delete non empty bundle"));
        }
        admin.namespaces().deleteNamespaceBundle(namespace, bundle.getBundleRange(), true);
        admin.lookups().lookupTopic(topicName.toString());

        admin.namespaces().deleteNamespace(namespace, true);
        assertFalse(admin.namespaces().getNamespaces("public").contains(namespace));
    }

    @Test(timeOut = 30 * 1000)
    public void testCheckOwnershipPresentWithSystemNamespace() throws Exception {
        NamespaceBundle namespaceBundle =
                getBundleAsync(pulsar1, TopicName.get(NamespaceName.SYSTEM_NAMESPACE + "/test")).get();
        try {
            pulsar1.getNamespaceService().checkOwnershipPresent(namespaceBundle);
        } catch (Exception ex) {
            log.info("Got exception", ex);
            assertTrue(ex.getCause() instanceof UnsupportedOperationException);
        }
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
    public void testTopBundlesLoadDataStoreTableViewFromChannelOwner() throws Exception {
        var topBundlesLoadDataStorePrimary =
                (LoadDataStore) FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", true);
        var serviceUnitStateChannelPrimary =
                (ServiceUnitStateChannelImpl) FieldUtils.readDeclaredField(primaryLoadManager,
                        "serviceUnitStateChannel", true);
        var tvPrimary =
                (TableViewImpl) FieldUtils.readDeclaredField(topBundlesLoadDataStorePrimary, "tableView", true);

        var topBundlesLoadDataStoreSecondary =
                (LoadDataStore) FieldUtils.readDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", true);
        var tvSecondary =
                (TableViewImpl) FieldUtils.readDeclaredField(topBundlesLoadDataStoreSecondary, "tableView", true);

        if (serviceUnitStateChannelPrimary.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
            assertNotNull(tvPrimary);
            assertNull(tvSecondary);
        } else {
            assertNull(tvPrimary);
            assertNotNull(tvSecondary);
        }

        restartBroker();
        pulsar1 = pulsar;
        setPrimaryLoadManager();

        var serviceUnitStateChannelPrimaryNew =
                (ServiceUnitStateChannelImpl) FieldUtils.readDeclaredField(primaryLoadManager,
                        "serviceUnitStateChannel", true);
        var topBundlesLoadDataStorePrimaryNew =
                (LoadDataStore) FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore"
                        , true);
        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
                    assertFalse(serviceUnitStateChannelPrimaryNew.isChannelOwnerAsync().get(5, TimeUnit.SECONDS));
                    assertNotNull(FieldUtils.readDeclaredField(topBundlesLoadDataStoreSecondary, "tableView"
                            , true));
                    assertNull(FieldUtils.readDeclaredField(topBundlesLoadDataStorePrimaryNew, "tableView"
                            , true));
                }
        );
    }

    @Test
    public void testRoleChange()
            throws Exception {


        var topBundlesLoadDataStorePrimary = (LoadDataStore<TopBundlesLoadData>)
                FieldUtils.readDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", true);
        var topBundlesLoadDataStorePrimarySpy = spy(topBundlesLoadDataStorePrimary);
        AtomicInteger countPri = new AtomicInteger(3);
        AtomicInteger countPri2 = new AtomicInteger(3);
        doAnswer(invocationOnMock -> {
            if (countPri.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStorePrimarySpy).startTableView();
        doAnswer(invocationOnMock -> {
            if (countPri2.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStorePrimarySpy).closeTableView();
        FieldUtils.writeDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", topBundlesLoadDataStorePrimarySpy, true);

        var topBundlesLoadDataStoreSecondary = (LoadDataStore<TopBundlesLoadData>)
                FieldUtils.readDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", true);
        var topBundlesLoadDataStoreSecondarySpy = spy(topBundlesLoadDataStoreSecondary);
        AtomicInteger countSec = new AtomicInteger(3);
        AtomicInteger countSec2 = new AtomicInteger(3);
        doAnswer(invocationOnMock -> {
            if (countSec.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStoreSecondarySpy).startTableView();
        doAnswer(invocationOnMock -> {
            if (countSec2.decrementAndGet() > 0) {
                throw new RuntimeException();
            }
            // Call the real method
            reset();
            return null;
        }).when(topBundlesLoadDataStoreSecondarySpy).closeTableView();
        FieldUtils.writeDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", topBundlesLoadDataStoreSecondarySpy, true);

        if (channel1.isChannelOwnerAsync().get(5, TimeUnit.SECONDS)) {
            primaryLoadManager.playFollower();
            primaryLoadManager.playFollower();
            secondaryLoadManager.playLeader();
            secondaryLoadManager.playLeader();
            primaryLoadManager.playLeader();
            primaryLoadManager.playLeader();
            secondaryLoadManager.playFollower();
            secondaryLoadManager.playFollower();
        } else {
            primaryLoadManager.playLeader();
            primaryLoadManager.playLeader();
            secondaryLoadManager.playFollower();
            secondaryLoadManager.playFollower();
            primaryLoadManager.playFollower();
            primaryLoadManager.playFollower();
            secondaryLoadManager.playLeader();
            secondaryLoadManager.playLeader();
        }


        verify(topBundlesLoadDataStorePrimarySpy, times(3)).startTableView();
        verify(topBundlesLoadDataStorePrimarySpy, times(3)).closeTableView();
        verify(topBundlesLoadDataStoreSecondarySpy, times(3)).startTableView();
        verify(topBundlesLoadDataStoreSecondarySpy, times(3)).closeTableView();

        FieldUtils.writeDeclaredField(primaryLoadManager, "topBundlesLoadDataStore", topBundlesLoadDataStorePrimary, true);
        FieldUtils.writeDeclaredField(secondaryLoadManager, "topBundlesLoadDataStore", topBundlesLoadDataStoreSecondary, true);
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
                        put(Overloaded, new AtomicLong(1));
                        put(Underloaded, new AtomicLong(2));
                    }},
                    Skip, new LinkedHashMap<>() {{
                        put(HitCount, new AtomicLong(3));
                        put(NoBundles, new AtomicLong(4));
                        put(CoolDown, new AtomicLong(5));
                        put(OutDatedData, new AtomicLong(6));
                        put(NoLoadData, new AtomicLong(7));
                        put(NoBrokers, new AtomicLong(8));
                        put(Unknown, new AtomicLong(9));
                    }},
                    Failure, Map.of(
                            Unknown, new AtomicLong(10))
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
                        dimensions=[{broker=localhost, metric=bundleUnloading, reason=HitCount, result=Skip}], metrics=[{brk_lb_unload_broker_breakdown_total=3}]
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

    private void setPrimaryLoadManager() throws IllegalAccessException {
        ExtensibleLoadManagerWrapper wrapper =
                (ExtensibleLoadManagerWrapper) pulsar1.getLoadManager().get();
        primaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(wrapper, "loadManager", true));
        FieldUtils.writeField(wrapper, "loadManager", primaryLoadManager, true);
        channel1 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(primaryLoadManager, "serviceUnitStateChannel", true);
    }

    private void setSecondaryLoadManager() throws IllegalAccessException {
        ExtensibleLoadManagerWrapper wrapper =
                (ExtensibleLoadManagerWrapper) pulsar2.getLoadManager().get();
        secondaryLoadManager = spy((ExtensibleLoadManagerImpl)
                FieldUtils.readField(wrapper, "loadManager", true));
        FieldUtils.writeField(wrapper, "loadManager", secondaryLoadManager, true);
        channel2 = (ServiceUnitStateChannelImpl)
                FieldUtils.readField(secondaryLoadManager, "serviceUnitStateChannel", true);
    }

    private CompletableFuture<NamespaceBundle> getBundleAsync(PulsarService pulsar, TopicName topic) {
        return pulsar.getNamespaceService().getBundleAsync(topic);
    }
}
