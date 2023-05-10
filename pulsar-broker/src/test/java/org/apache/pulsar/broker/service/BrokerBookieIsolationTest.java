/**
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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper;
import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicyImpl.REPP_DNS_RESOLVER_CLASS;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.pulsar.bookie.rackawareness.BookieRackAffinityMapping;
import org.apache.pulsar.broker.ManagedLedgerClientFactory;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.ModularLoadManagerImpl;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.BrokerPersistenceException;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BookieInfo;
import org.apache.pulsar.common.policies.data.BookiesRackConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BrokerBookieIsolationTest {

    private LocalBookkeeperEnsemble bkEnsemble;
    private PulsarService pulsarService;

    private static final List<ACL> Acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private final ObjectMapper jsonMapper = ObjectMapperFactory.create();

    @BeforeMethod
    protected void setup() throws Exception {
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(4, 0, () -> 0);
        bkEnsemble.start();
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (pulsarService != null) {
            pulsarService.close();
        }
        bkEnsemble.stop();
    }

    /**
     * Validate that broker can support tenant based bookie isolation.
     *
     * <pre>
     * 1. create two bookie-info group : default-group and isolated-group
     * 2. namespace ns1 : uses default-group
     *    validate: bookie-ensemble for ns1-topics's ledger will be from default-group
     * 3. namespace ns2,ns3,ns4: uses isolated-group
     *    validate: bookie-ensemble for above namespace-topics's ledger will be from isolated-group
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testBookieIsolation() throws Exception {
        final String tenant1 = "tenant1";
        final String cluster = "use";
        final String ns1 = String.format("%s/%s/%s", tenant1, cluster, "ns1");
        final String ns2 = String.format("%s/%s/%s", tenant1, cluster, "ns2");
        final String ns3 = String.format("%s/%s/%s", tenant1, cluster, "ns3");
        final String ns4 = String.format("%s/%s/%s", tenant1, cluster, "ns4");
        final int totalPublish = 100;

        final String brokerBookkeeperClientIsolationGroups = "default-group";
        final String tenantNamespaceIsolationGroups = "tenant1-isolation";

        BookieServer[] bookies = bkEnsemble.getBookies();
        ZooKeeper zkClient = bkEnsemble.getZkClient();

        Set<BookieId> defaultBookies = Sets.newHashSet(bookies[0].getBookieId(),
                bookies[1].getBookieId());
        Set<BookieId> isolatedBookies = Sets.newHashSet(bookies[2].getBookieId(),
                bookies[3].getBookieId());

        setDefaultIsolationGroup(brokerBookkeeperClientIsolationGroups, zkClient, defaultBookies);
        setDefaultIsolationGroup(tenantNamespaceIsolationGroups, zkClient, isolatedBookies);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName(cluster);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setAdvertisedAddress("localhost");
        config.setBookkeeperClientIsolationGroups(brokerBookkeeperClientIsolationGroups);
        config.setDefaultNumberOfNamespaceBundles(8);

        config.setManagedLedgerDefaultEnsembleSize(2);
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);

        config.setAllowAutoTopicCreationType("non-partitioned");

        int totalEntriesPerLedger = 20;
        int totalLedgers = totalPublish / totalEntriesPerLedger;
        config.setManagedLedgerMaxEntriesPerLedger(totalEntriesPerLedger);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsarService = new PulsarService(config);
        pulsarService.start();


        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarService.getWebServiceAddress()).build();

        ClusterData clusterData = ClusterData.builder().serviceUrl(pulsarService.getWebServiceAddress()).build();
        admin.clusters().createCluster(cluster, clusterData);
        TenantInfoImpl tenantInfo = new TenantInfoImpl(null, Sets.newHashSet(cluster));
        admin.tenants().createTenant(tenant1, tenantInfo);
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        admin.namespaces().createNamespace(ns3);
        admin.namespaces().createNamespace(ns4);
        admin.namespaces().setBookieAffinityGroup(ns2,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());
        admin.namespaces().setBookieAffinityGroup(ns3,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());
        admin.namespaces().setBookieAffinityGroup(ns4,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());

        assertEquals(admin.namespaces().getBookieAffinityGroup(ns2),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns3),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns4),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroups)
                        .build());

        //Checks the namespace bundles after setting the bookie affinity
        assertEquals(admin.namespaces().getBundles(ns2).getNumBundles(), config.getDefaultNumberOfNamespaceBundles());

        try {
            admin.namespaces().getBookieAffinityGroup(ns1);
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarService.getBrokerServiceUrl())
                .statsInterval(-1, TimeUnit.SECONDS).build();

        PersistentTopic topic1 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns1, "topic1", totalPublish);
        PersistentTopic topic2 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns2, "topic1", totalPublish);
        PersistentTopic topic3 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns3, "topic1", totalPublish);
        PersistentTopic topic4 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns4, "topic1", totalPublish);

        Bookie bookie1 = bookies[0].getBookie();
        Field ledgerManagerField = Bookie.class.getDeclaredField("ledgerManager");
        ledgerManagerField.setAccessible(true);
        LedgerManager ledgerManager = (LedgerManager) ledgerManagerField.get(bookie1);

        // namespace: ns1
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic1.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), defaultBookies);

        // namespace: ns2
        ml = (ManagedLedgerImpl) topic2.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        // namespace: ns3
        ml = (ManagedLedgerImpl) topic3.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        // namespace: ns4
        ml = (ManagedLedgerImpl) topic4.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        ManagedLedgerClientFactory mlFactory =
            (ManagedLedgerClientFactory) pulsarService.getManagedLedgerClientFactory();
        Map<EnsemblePlacementPolicyConfig, BookKeeper> bkPlacementPolicyToBkClientMap = mlFactory
                .getBkEnsemblePolicyToBookKeeperMap();

        // broker should create only 1 bk-client and factory per isolation-group
        assertEquals(bkPlacementPolicyToBkClientMap.size(), 1);

        // make sure bk-isolation group also configure REPP_DNS_RESOLVER_CLASS as ZkBookieRackAffinityMapping to
        // configure rack-aware policy with in isolated group
        Map<EnsemblePlacementPolicyConfig, BookKeeper> bkMap = mlFactory.getBkEnsemblePolicyToBookKeeperMap();
        BookKeeper bk = bkMap.values().iterator().next();
        Method getConf = BookKeeper.class.getDeclaredMethod("getConf");
        getConf.setAccessible(true);
        ClientConfiguration clientConf = (ClientConfiguration) getConf.invoke(bk);
        assertEquals(clientConf.getProperty(REPP_DNS_RESOLVER_CLASS), BookieRackAffinityMapping.class.getName());
    }

    /**
     * It verifies that "ZkIsolatedBookieEnsemblePlacementPolicy" considers secondary affinity-group if primary group
     * doesn't have enough non-faulty bookies.
     *
     * @throws Exception
     */
    @Test
    public void testBookieIsolationWithSecondaryGroup() throws Exception {
        final String tenant1 = "tenant1";
        final String cluster = "use";
        final String ns1 = String.format("%s/%s/%s", tenant1, cluster, "ns1");
        final String ns2 = String.format("%s/%s/%s", tenant1, cluster, "ns2");
        final String ns3 = String.format("%s/%s/%s", tenant1, cluster, "ns3");
        final String ns4 = String.format("%s/%s/%s", tenant1, cluster, "ns4");
        final int totalPublish = 100;

        final String brokerBookkeeperClientIsolationGroups = "default-group";
        final String tenantNamespaceIsolationGroupsPrimary = "tenant1-isolation-primary";
        final String tenantNamespaceIsolationGroupsSecondary = "tenant1-isolation=secondary";

        BookieServer[] bookies = bkEnsemble.getBookies();
        ZooKeeper zkClient = bkEnsemble.getZkClient();

        Set<BookieId> defaultBookies = Sets.newHashSet(bookies[0].getBookieId(),
                bookies[1].getBookieId());
        Set<BookieId> isolatedBookies = Sets.newHashSet(bookies[2].getBookieId(),
                bookies[3].getBookieId());
        Set<BookieId> downedBookies = Sets.newHashSet(BookieId.parse("1.1.1.1:1111"),
                BookieId.parse("1.1.1.1:1112"));

        setDefaultIsolationGroup(brokerBookkeeperClientIsolationGroups, zkClient, defaultBookies);
        // primary group empty
        setDefaultIsolationGroup(tenantNamespaceIsolationGroupsPrimary, zkClient, downedBookies);
        setDefaultIsolationGroup(tenantNamespaceIsolationGroupsSecondary, zkClient, isolatedBookies);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName(cluster);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setAdvertisedAddress("localhost");
        config.setBookkeeperClientIsolationGroups(brokerBookkeeperClientIsolationGroups);

        config.setManagedLedgerDefaultEnsembleSize(2);
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);
        config.setAllowAutoTopicCreationType("non-partitioned");

        int totalEntriesPerLedger = 20;
        int totalLedgers = totalPublish / totalEntriesPerLedger;
        config.setManagedLedgerMaxEntriesPerLedger(totalEntriesPerLedger);
        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsarService = new PulsarService(config);
        pulsarService.start();

        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarService.getWebServiceAddress()).build();

        ClusterData clusterData = ClusterData.builder().serviceUrl(pulsarService.getWebServiceAddress()).build();
        admin.clusters().createCluster(cluster, clusterData);
        TenantInfoImpl tenantInfo = new TenantInfoImpl(null, Sets.newHashSet(cluster));
        admin.tenants().createTenant(tenant1, tenantInfo);
        admin.namespaces().createNamespace(ns1);
        admin.namespaces().createNamespace(ns2);
        admin.namespaces().createNamespace(ns3);
        admin.namespaces().createNamespace(ns4);
        admin.namespaces().setBookieAffinityGroup(ns2,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        admin.namespaces().setBookieAffinityGroup(ns3,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        admin.namespaces().setBookieAffinityGroup(ns4,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .build());

        assertEquals(admin.namespaces().getBookieAffinityGroup(ns2),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns3),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns4),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .build());

        try {
            admin.namespaces().getBookieAffinityGroup(ns1);
        } catch (PulsarAdminException.NotFoundException e) {
            // Ok
        }

        @Cleanup
        PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarService.getBrokerServiceUrl())
                .statsInterval(-1, TimeUnit.SECONDS).build();

        PersistentTopic topic1 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns1, "topic1", totalPublish);
        PersistentTopic topic2 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns2, "topic1", totalPublish);
        PersistentTopic topic3 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns3, "topic1", totalPublish);

        Bookie bookie1 = bookies[0].getBookie();
        Field ledgerManagerField = Bookie.class.getDeclaredField("ledgerManager");
        ledgerManagerField.setAccessible(true);
        LedgerManager ledgerManager = (LedgerManager) ledgerManagerField.get(bookie1);

        // namespace: ns1
        ManagedLedgerImpl ml = (ManagedLedgerImpl) topic1.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), defaultBookies);

        // namespace: ns2
        ml = (ManagedLedgerImpl) topic2.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        // namespace: ns3
        ml = (ManagedLedgerImpl) topic3.getManagedLedger();
        assertEquals(ml.getLedgersInfoAsList().size(), totalLedgers);
        // validate ledgers' ensemble with affinity bookies
        assertAffinityBookies(ledgerManager, ml.getLedgersInfoAsList(), isolatedBookies);

        ManagedLedgerClientFactory mlFactory =
            (ManagedLedgerClientFactory) pulsarService.getManagedLedgerClientFactory();
        Map<EnsemblePlacementPolicyConfig, BookKeeper> bkPlacementPolicyToBkClientMap = mlFactory
                .getBkEnsemblePolicyToBookKeeperMap();

        // broker should create only 1 bk-client and factory per isolation-group
        assertEquals(bkPlacementPolicyToBkClientMap.size(), 1);

        // ns4 doesn't have secondary group so, publish should fail
        try {
            PersistentTopic topic4 = (PersistentTopic) createTopicAndPublish(pulsarClient, ns4, "topic1", 1);
            fail("should have failed due to not enough non-faulty bookie");
        } catch (BrokerPersistenceException e) {
            // Ok..
        }
    }

    @Test
    public void testDeleteIsolationGroup() throws Exception {

        final String tenant1 = "tenant1";
        final String cluster = "use";
        final String ns2 = String.format("%s/%s/%s", tenant1, cluster, "ns2");
        final String ns3 = String.format("%s/%s/%s", tenant1, cluster, "ns3");

        final String brokerBookkeeperClientIsolationGroups = "default-group";
        final String tenantNamespaceIsolationGroupsPrimary = "tenant1-isolation-primary";
        final String tenantNamespaceIsolationGroupsSecondary = "tenant1-isolation=secondary";

        BookieServer[] bookies = bkEnsemble.getBookies();
        ZooKeeper zkClient = bkEnsemble.getZkClient();

        Set<BookieId> defaultBookies = Sets.newHashSet(bookies[0].getBookieId(),
                bookies[1].getBookieId());
        Set<BookieId> isolatedBookies = Sets.newHashSet(bookies[2].getBookieId(),
                bookies[3].getBookieId());

        setDefaultIsolationGroup(brokerBookkeeperClientIsolationGroups, zkClient, defaultBookies);
        // primary group empty
        setDefaultIsolationGroup(tenantNamespaceIsolationGroupsPrimary, zkClient, Sets.newHashSet());
        setDefaultIsolationGroup(tenantNamespaceIsolationGroupsSecondary, zkClient, isolatedBookies);

        ServiceConfiguration config = new ServiceConfiguration();
        config.setLoadManagerClassName(ModularLoadManagerImpl.class.getName());
        config.setClusterName(cluster);
        config.setWebServicePort(Optional.of(0));
        config.setZookeeperServers("127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config.setBrokerShutdownTimeoutMs(0L);
        config.setBrokerServicePort(Optional.of(0));
        config.setAdvertisedAddress("localhost");
        config.setBookkeeperClientIsolationGroups(brokerBookkeeperClientIsolationGroups);

        config.setManagedLedgerDefaultEnsembleSize(2);
        config.setManagedLedgerDefaultWriteQuorum(2);
        config.setManagedLedgerDefaultAckQuorum(2);
        config.setAllowAutoTopicCreationType("non-partitioned");

        config.setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsarService = new PulsarService(config);
        pulsarService.start();

        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(pulsarService.getWebServiceAddress()).build();

        ClusterData clusterData = ClusterData.builder().serviceUrl(pulsarService.getWebServiceAddress()).build();
        admin.clusters().createCluster(cluster, clusterData);
        TenantInfoImpl tenantInfo = new TenantInfoImpl(null, Sets.newHashSet(cluster));
        admin.tenants().createTenant(tenant1, tenantInfo);
        admin.namespaces().createNamespace(ns2);
        admin.namespaces().createNamespace(ns3);

        // (1) set affinity-group
        admin.namespaces().setBookieAffinityGroup(ns2,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        admin.namespaces().setBookieAffinityGroup(ns3,
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());

        // (2) get affinity-group
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns2),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());
        assertEquals(admin.namespaces().getBookieAffinityGroup(ns3),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());

        // (3) delete affinity-group
        admin.namespaces().deleteBookieAffinityGroup(ns2);

        assertNull(admin.namespaces().getBookieAffinityGroup(ns2));

        assertEquals(admin.namespaces().getBookieAffinityGroup(ns3),
                BookieAffinityGroupData.builder()
                        .bookkeeperAffinityGroupPrimary(tenantNamespaceIsolationGroupsPrimary)
                        .bookkeeperAffinityGroupSecondary(tenantNamespaceIsolationGroupsSecondary)
                        .build());

    }

    private void assertAffinityBookies(LedgerManager ledgerManager, List<LedgerInfo> ledgers1,
            Set<BookieId> defaultBookies) throws Exception {
        for (LedgerInfo lInfo : ledgers1) {
            long ledgerId = lInfo.getLedgerId();
            CompletableFuture<Versioned<LedgerMetadata>> ledgerMetaFuture = ledgerManager.readLedgerMetadata(ledgerId);
            LedgerMetadata ledgerMetadata = ledgerMetaFuture.get().getValue();
            Set<BookieId> ledgerBookies = Sets.newHashSet();
            ledgerBookies.addAll(ledgerMetadata.getAllEnsembles().values().iterator().next());
            assertEquals(ledgerBookies.size(), defaultBookies.size());
            ledgerBookies.removeAll(defaultBookies);
            assertEquals(ledgerBookies.size(), 0);
        }
    }

    private Topic createTopicAndPublish(PulsarClient pulsarClient, String ns, String topicLocalName, int totalPublish)
            throws Exception {
        final String topicName = String.format("persistent://%s/%s", ns, topicLocalName);
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("my-subscriber-name")
                .subscribe();
        consumer.close();

        ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer().topic(topicName).sendTimeout(5, TimeUnit.SECONDS);

        Producer<byte[]> producer = producerBuilder.create();
        for (int i = 0; i < totalPublish; i++) {
            String message = "my-message-" + i;
            producer.send(message.getBytes());
        }
        producer.close();

        return pulsarService.getBrokerService().getTopicReference(topicName).get();
    }

    private void setDefaultIsolationGroup(String brokerBookkeeperClientIsolationGroups, ZooKeeper zkClient,
            Set<BookieId> bookieAddresses) throws Exception {
        BookiesRackConfiguration bookies = null;
        try {
            byte[] data = zkClient.getData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, false, null);
            System.out.println(new String(data));
            bookies = jsonMapper.readValue(data, BookiesRackConfiguration.class);
        } catch (KeeperException.NoNodeException e) {
            // Ok.. create new bookie znode
            zkClient.create(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, "".getBytes(), Acl,
                    CreateMode.PERSISTENT);
        }
        if (bookies == null) {
            bookies = new BookiesRackConfiguration();
        }

        Map<String, BookieInfo> bookieInfoMap = Maps.newHashMap();
        for (BookieId bkSocket : bookieAddresses) {
            BookieInfo info = BookieInfo.builder().rack("use").hostname(bkSocket.toString()).build();
            bookieInfoMap.put(bkSocket.toString(), info);
        }
        bookies.put(brokerBookkeeperClientIsolationGroups, bookieInfoMap);

        zkClient.setData(BookieRackAffinityMapping.BOOKIE_INFO_ROOT_PATH, jsonMapper.writeValueAsBytes(bookies), -1);
    }
    private static final Logger log = LoggerFactory.getLogger(BrokerBookieIsolationTest.class);

}
