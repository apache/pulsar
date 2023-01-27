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
package org.apache.pulsar.broker.admin.v1;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.ConflictException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import org.apache.pulsar.client.admin.internal.LookupImpl;
import org.apache.pulsar.client.admin.internal.TenantsImpl;
import org.apache.pulsar.client.admin.internal.TopicsImpl;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AuthAction;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyData;
import org.apache.pulsar.common.policies.data.AutoFailoverPolicyType;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import org.apache.pulsar.common.policies.data.BrokerAssignment;
import org.apache.pulsar.common.policies.data.BrokerNamespaceIsolationData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PoliciesUtil;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.Compactor;
import org.apache.pulsar.metadata.cache.impl.MetadataCacheImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker-admin")
public class V1_AdminApiTest extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(V1_AdminApiTest.class);

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";

    private MockedPulsarService mockPulsarSetup;

    private PulsarService otherPulsar;

    private PulsarAdmin adminTls;
    private PulsarAdmin otheradmin;

    private NamespaceBundleFactory bundleFactory;

    @BeforeClass
    @Override
    public void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        conf.setSystemTopicEnabled(false);
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setNumExecutorThreadPoolSize(5);

        super.internalSetup();

        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());

        adminTls = spy(PulsarAdmin.builder().tlsTrustCertsFilePath(TLS_SERVER_CERT_FILE_PATH)
                .serviceHttpUrl(brokerUrlTls.toString()).build());

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        mockPulsarSetup = new MockedPulsarService(this.conf);
        mockPulsarSetup.setup();
        otherPulsar = mockPulsarSetup.getPulsar();
        otheradmin = mockPulsarSetup.getAdmin();

        // Setup namespaces
        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        adminTls.close();
        super.internalCleanup();
        mockPulsarSetup.cleanup();
    }

    @AfterMethod(alwaysRun = true)
    public void reset() throws Exception {
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);
        for (String tenant : admin.tenants().getTenants()) {
            for (String namespace : admin.namespaces().getNamespaces(tenant)) {
                deleteNamespaceWithRetry(namespace, true, admin, pulsar,
                        mockPulsarSetup.getPulsar());
            }
        }
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

        resetConfig();

        if (!admin.clusters().getClusters().contains("use")) {
            admin.clusters().createCluster("use",
                    ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        }

        if (!admin.tenants().getTenants().contains("prop-xyz")) {
            TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use"));
            admin.tenants().createTenant("prop-xyz", tenantInfo);
        }
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @DataProvider(name = "numBundles")
    public static Object[][] numBundles() {
        return new Object[][] { { 1 }, { 4 } };
    }

    @DataProvider(name = "bundling")
    public static Object[][] bundling() {
        return new Object[][] { { 0 }, { 4 } };
    }

    @DataProvider(name = "topicName")
    public Object[][] topicNamesProvider() {
        return new Object[][] { { "topic_+&*%{}() \\/$@#^%" }, { "simple-topicName" } };
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { TopicDomain.persistent.value() }, { TopicDomain.non_persistent.value() } };
    }

    @Test
    public void clusters() throws Exception {
        admin.clusters().createCluster("usw",
                ClusterData.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());
        // "test" cluster is part of config-default cluster and it's znode gets created when PulsarService creates
        // failure-domain znode of this default cluster
        assertEquals(admin.clusters().getClusters(), List.of("use", "usw"));

        assertEquals(admin.clusters().getCluster("use"),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        admin.clusters().updateCluster("usw",
                ClusterData.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build());
        assertEquals(admin.clusters().getClusters(), List.of("use", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                ClusterData.builder().serviceUrl("http://new-broker.messaging.use.example.com:8080").build());

        admin.clusters().updateCluster("usw",
                ClusterData.builder()
                        .serviceUrl("http://new-broker.messaging.usw.example.com:8080")
                        .serviceUrlTls("https://new-broker.messaging.usw.example.com:4443")
                        .build());
        assertEquals(admin.clusters().getClusters(), List.of("use", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                ClusterData.builder()
                        .serviceUrl("http://new-broker.messaging.usw.example.com:8080")
                        .serviceUrlTls("https://new-broker.messaging.usw.example.com:4443")
                        .build());

        admin.clusters().deleteCluster("usw");
        Thread.sleep(300);

        assertEquals(admin.clusters().getClusters(), List.of("use"));

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.clusters().deleteCluster("use");
        assertEquals(admin.clusters().getClusters(), new ArrayList<>());

        // Check name validation
        try {
            admin.clusters().createCluster("bf!", ClusterData.builder().serviceUrl("http://dummy.messaging.example.com").build());
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    @Test
    public void clusterNamespaceIsolationPolicies() throws PulsarAdminException {
        try {
            // create
            String policyName1 = "policy-1";
            Map<String, String> parameters1 = new HashMap<>();
            parameters1.put("min_limit", "1");
            parameters1.put("usage_threshold", "100");

            NamespaceIsolationData nsPolicyData1 = NamespaceIsolationData.builder()
                    .namespaces(Lists.newArrayList("other/use/other.*"))
                    .primary(Lists.newArrayList(".*")) // match all broker. make it easy to verify `getBrokersWithNamespaceIsolationPolicy` later
                    .secondary(Lists.newArrayList("prod1-broker.*.messaging.use.example.com"))
                    .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                            .policyType(AutoFailoverPolicyType.min_available)
                            .parameters(parameters1)
                            .build())
                    .build();

            admin.clusters().createNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            String policyName2 = "policy-2";
            Map<String, String> parameters2 = new HashMap<>();
            parameters2.put("min_limit", "1");
            parameters2.put("usage_threshold", "100");

            NamespaceIsolationData nsPolicyData2 = NamespaceIsolationData.builder()
                    .namespaces(Lists.newArrayList("other/use/other.*"))
                    .primary(Lists.newArrayList("prod1-broker[4-6].messaging.use.example.com"))
                    .secondary(Lists.newArrayList("prod1-broker.*.messaging.use.example.com"))
                    .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                            .policyType(AutoFailoverPolicyType.min_available)
                            .parameters(parameters1)
                            .build())
                    .build();
            admin.clusters().createNamespaceIsolationPolicy("use", policyName2, nsPolicyData2);

            // verify create indirectly with get
            Map<String, ? extends NamespaceIsolationData> policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);
            assertEquals(policiesMap.get(policyName2), nsPolicyData2);

            // verify local broker get matched.
            List<BrokerNamespaceIsolationData> isoList = admin.clusters().getBrokersWithNamespaceIsolationPolicy("use");
            assertEquals(isoList.size(), 1);
            assertTrue(isoList.get(0).isPrimary());
            assertEquals(isoList.get(0).getPolicyName(), policyName1);

            // verify update of primary
            nsPolicyData1.getPrimary().remove(0);
            nsPolicyData1.getPrimary().add("prod1-broker[1-2].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify primary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of secondary
            nsPolicyData1.getSecondary().remove(0);
            nsPolicyData1.getSecondary().add("prod1-broker[3-4].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify secondary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover policy limit
            nsPolicyData1.getAutoFailoverPolicy().getParameters().put("min_limit", "10");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify min_limit change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover usage_threshold limit
            nsPolicyData1.getAutoFailoverPolicy().getParameters().put("usage_threshold", "80");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify usage_threshold change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify single get
            NamespaceIsolationDataImpl policy1Data =
                    (NamespaceIsolationDataImpl) admin.clusters().getNamespaceIsolationPolicy("use", policyName1);
            assertEquals(policy1Data, nsPolicyData1);

            // verify creation of more than one policy
            admin.clusters().createNamespaceIsolationPolicy("use", policyName2, nsPolicyData1);

            try {
                admin.clusters().getNamespaceIsolationPolicy("use", "no-such-policy");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }

            // verify delete cluster failed
            try {
                admin.clusters().deleteCluster("use");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof PreconditionFailedException);
            }

            // verify delete
            admin.clusters().deleteNamespaceIsolationPolicy("use", policyName1);
            admin.clusters().deleteNamespaceIsolationPolicy("use", policyName2);

            try {
                admin.clusters().getNamespaceIsolationPolicy("use", policyName1);
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }

            try {
                admin.clusters().getNamespaceIsolationPolicy("use", policyName2);
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }

            try {
                admin.clusters().getNamespaceIsolationPolicies("usc");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }

            try {
                admin.clusters().getNamespaceIsolationPolicy("usc", "no-such-cluster");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof PreconditionFailedException);
            }

            try {
                admin.clusters().createNamespaceIsolationPolicy("usc", "no-such-cluster", nsPolicyData1);
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof PreconditionFailedException);
            }

            try {
                admin.clusters().updateNamespaceIsolationPolicy("usc", "no-such-cluster", policy1Data);
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof PreconditionFailedException);
            }

        } catch (PulsarAdminException e) {
            LOG.warn("TEST FAILED [{}]", e.getMessage());
            throw e;
        }
    }

    @Test
    public void brokers() throws Exception {
        List<String> list = admin.brokers().getActiveBrokers("use");
        Assert.assertNotNull(list);
        Assert.assertEquals(list.size(), 1);

        List<String> list2 = otheradmin.brokers().getActiveBrokers("test");
        Assert.assertNotNull(list2);
        Assert.assertEquals(list2.size(), 1);

        Map<String, NamespaceOwnershipStatus> nsMap = admin.brokers().getOwnedNamespaces("use", list.get(0));
        // since sla-monitor ns is not created nsMap.size() == 1 (for HeartBeat Namespace)
        Assert.assertEquals(nsMap.size(), 2);
        for (String ns : nsMap.keySet()) {
            NamespaceOwnershipStatus nsStatus = nsMap.get(ns);
            if (ns.equals(
                    NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), pulsar.getConfiguration())
                            + "/0x00000000_0xffffffff")) {
                assertEquals(nsStatus.broker_assignment, BrokerAssignment.shared);
                assertFalse(nsStatus.is_controlled);
                assertTrue(nsStatus.is_active);
            }
        }

        String[] parts = list.get(0).split(":");
        Assert.assertEquals(parts.length, 2);
        Map<String, NamespaceOwnershipStatus> nsMap2 = adminTls.brokers().getOwnedNamespaces("use",
                String.format("%s:%d", parts[0], pulsar.getListenPortHTTPS().get()));
        Assert.assertEquals(nsMap2.size(), 2);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.clusters().deleteCluster("use");
        assertEquals(admin.clusters().getClusters(), new ArrayList<>());
    }

    /**
     * <pre>
     * Verifies: zk-update configuration updates service-config
     * 1. create znode for dynamic-config
     * 2. start pulsar service so, pulsar can set the watch on that znode
     * 3. update the configuration with new value
     * 4. wait and verify that new value has been updated
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testUpdateDynamicConfigurationWithZkWatch() throws Exception {
        final int initValue = 30000;
        long defaultValue = pulsar.getConfiguration().getBrokerShutdownTimeoutMs();
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(initValue);
        // (1) try to update dynamic field
        final long shutdownTime = 10;
        // update configuration
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", Long.toString(shutdownTime));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getBrokerShutdownTimeoutMs() != initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getBrokerShutdownTimeoutMs() != shutdownTime) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }
        // verify value is updated
        assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);

        // (2) try to update non-dynamic field
        try {
            admin.brokers().updateDynamicConfiguration("metadataStoreUrl", "zk:test-zk:1234");
        } catch (Exception e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // (3) try to update non-existent field
        try {
            admin.brokers().updateDynamicConfiguration("test", Long.toString(shutdownTime));
        } catch (Exception e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(defaultValue);
    }

    /**
     * Verifies broker sets watch on dynamic-configuration map even with invalid init json data
     *
     * <pre>
     * 1. Set invalid json at dynamic-config znode
     * 2. Broker fails to deserialize znode content but sets the watch on znode
     * 3. Update znode with valid json map
     * 4. Broker should get watch and update the dynamic-config map
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testInvalidDynamicConfigContentInZK() throws Exception {
        final int newValue = 10;

        // set invalid data into dynamic-config znode so, broker startup fail to deserialize data
        pulsar.getLocalMetadataStore().put("/admin/configuration", "$".getBytes(),
                Optional.empty()).join();
        stopBroker();

        // start broker: it should have set watch even if with failure of deserialization
        startBroker();
        Assert.assertNotEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), newValue);
        // update zk with config-value which should fire watch and broker should update the config value
        Map<String, String> configMap = new HashMap<>();
        configMap.put("brokerShutdownTimeoutMs", Integer.toString(newValue));
        pulsar.getLocalMetadataStore().put("/admin/configuration",
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(configMap),
                Optional.empty()).join();
        // wait config to be updated
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getBrokerShutdownTimeoutMs() != newValue) {
                Thread.sleep(100 + (i * 10));
            } else {
                break;
            }
        }
        // verify value is updated
        assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), newValue);

        cleanup();
        setup();
    }

    /**
     * <pre>
     * verifies: that registerListener updates pulsar.config value with newly updated zk-dynamic config
     * 1.start pulsar
     * 2.update zk-config with admin api
     * 3. trigger watch and listener
     * 4. verify that config is updated
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testUpdateDynamicLocalConfiguration() throws Exception {
        // (1) try to update dynamic field
        final long initValue = 30000;
        final long shutdownTime = 10;
        long defaultValue = pulsar.getConfiguration().getBrokerShutdownTimeoutMs();
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(initValue);
        // update configuration
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", Long.toString(shutdownTime));
        // sleep incrementally as zk-watch notification is async and may take some time
        for (int i = 0; i < 5; i++) {
            if (pulsar.getConfiguration().getBrokerShutdownTimeoutMs() == initValue) {
                Thread.sleep(50 + (i * 10));
            }
        }

        // verify value is updated
        assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);

        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(defaultValue);
    }

    @Test
    public void testUpdatableConfigurationName() throws Exception {
        // (1) try to update dynamic field
        final String configName = "brokerShutdownTimeoutMs";
        assertTrue(admin.brokers().getDynamicConfigurationNames().contains(configName));
    }

    @Test
    public void testGetDynamicLocalConfiguration() throws Exception {
        // (1) try to update dynamic field
        final String configName = "brokerShutdownTimeoutMs";
        final long shutdownTime = 10;
        long defaultValue = pulsar.getConfiguration().getBrokerShutdownTimeoutMs();
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(30000);
        Map<String, String> configs = admin.brokers().getAllDynamicConfigurations();
        assertTrue(configs.isEmpty());
        assertNotEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);
        // update configuration
        admin.brokers().updateDynamicConfiguration(configName, Long.toString(shutdownTime));
        // Now, znode is created: updateConfigurationAndregisterListeners and check if configuration updated
        assertEquals(Long.parseLong(admin.brokers().getAllDynamicConfigurations().get(configName)), shutdownTime);

        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(defaultValue);
    }

    @Test
    public void testTenant() throws Exception {
        Set<String> allowedClusters = Set.of("use");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), allowedClusters);
        admin.tenants().createTenant("prop-xyz2", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz2/use/ns1");

        assertEquals(admin.tenants().getTenants(), List.of("prop-xyz", "prop-xyz2"));

        assertEquals(admin.tenants().getTenantInfo("prop-xyz2"), tenantInfo);

        TenantInfoImpl newPropertyAdmin = new TenantInfoImpl(Set.of("role3", "role4"), allowedClusters);
        admin.tenants().updateTenant("prop-xyz2", newPropertyAdmin);

        assertEquals(admin.tenants().getTenantInfo("prop-xyz2"), newPropertyAdmin);

        admin.namespaces().deleteNamespace("prop-xyz2/use/ns1");
        admin.tenants().deleteTenant("prop-xyz2");
        assertEquals(admin.tenants().getTenants(), List.of("prop-xyz"));

        // Check name validation
        try {
            admin.tenants().createTenant("prop-xyz&", tenantInfo);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    @Test
    public void namespaces() throws Exception {
        admin.clusters().createCluster("usw", ClusterData.builder().build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"),
                Set.of("use", "usw"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);

        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1").bundles, PoliciesUtil.defaultBundle());

        admin.namespaces().createNamespace("prop-xyz/use/ns2");

        admin.namespaces().createNamespace("prop-xyz/use/ns3", 4);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns3").bundles.getNumBundles(), 4);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns3").bundles.getBoundaries().size(), 5);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns3");

        try {
            admin.namespaces().createNamespace("non-existing/usw/ns1");
            fail("Should not have passed");
        } catch (NotFoundException e) {
            // Ok
        }

        assertEquals(admin.namespaces().getNamespaces("prop-xyz"),
                List.of("prop-xyz/use/ns1", "prop-xyz/use/ns2"));
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"),
                List.of("prop-xyz/use/ns1", "prop-xyz/use/ns2"));

        try {
            admin.namespaces().createNamespace("prop-xyz/usc/ns1");
            fail("Should not have passed");
        } catch (NotAuthorizedException e) {
            // Ok, got the non authorized exception since usc cluster is not in the allowed clusters list.
        }

        // no need to clear cache once authorization-provide also start using metadata-store
        clearCache();
        admin.namespaces().grantPermissionOnNamespace("prop-xyz/use/ns1", "my-role", EnumSet.allOf(AuthAction.class));

        Policies policies = new Policies();
        policies.bundles = PoliciesUtil.defaultBundle();
        policies.auth_policies.getNamespaceAuthentication().put("my-role", EnumSet.allOf(AuthAction.class));
        policies.is_allow_auto_update_schema = conf.isAllowAutoUpdateSchemaEnabled();

        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1"), policies);
        assertEquals(admin.namespaces().getPermissions("prop-xyz/use/ns1"), policies.auth_policies.getNamespaceAuthentication());

        assertEquals(admin.namespaces().getTopics("prop-xyz/use/ns1"), new ArrayList<>());

        admin.namespaces().revokePermissionsOnNamespace("prop-xyz/use/ns1", "my-role");
        policies.auth_policies.getNamespaceAuthentication().remove("my-role");
        policies.is_allow_auto_update_schema = conf.isAllowAutoUpdateSchemaEnabled();
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1"), policies);

        assertNull(admin.namespaces().getPersistence("prop-xyz/use/ns1"));
        admin.namespaces().setPersistence("prop-xyz/use/ns1", new PersistencePolicies(3, 2, 1, 10.0));
        assertEquals(admin.namespaces().getPersistence("prop-xyz/use/ns1"), new PersistencePolicies(3, 2, 1, 10.0));

        // Force topic creation and namespace being loaded
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES).topic("persistent://prop-xyz/use/ns1/my-topic").create();
        producer.close();
        admin.topics().delete("persistent://prop-xyz/use/ns1/my-topic");

        admin.namespaces().unloadNamespaceBundle("prop-xyz/use/ns1", "0x00000000_0xffffffff");
        NamespaceName ns = NamespaceName.get("prop-xyz/use/ns1");
        // Now, w/ bundle policies, we will use default bundle
        NamespaceBundle defaultBundle = bundleFactory.getFullBundle(ns);
        int i = 0;
        for (; i < 10; i++) {
            Optional<NamespaceEphemeralData> data1 = pulsar.getNamespaceService().getOwnershipCache()
                    .getOwnerAsync(defaultBundle).get();
            if (!data1.isPresent()) {
                // Already unloaded
                break;
            }
            LOG.info("Waiting for unload namespace {} to complete. Current service unit isDisabled: {}", defaultBundle,
                    data1.get().isDisabled());
            Thread.sleep(1000);
        }
        assertTrue(i < 10);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"), List.of("prop-xyz/use/ns2"));

        try {
            admin.namespaces().unload("prop-xyz/use/ns1");
            fail("should have raised exception");
        } catch (Exception e) {
            // OK excepted
        }

        // Force topic creation and namespace being loaded
        producer = pulsarClient.newProducer(Schema.BYTES).topic("persistent://prop-xyz/use/ns2/my-topic").create();
        producer.close();
        admin.topics().delete("persistent://prop-xyz/use/ns2/my-topic");

        // both unload and delete should succeed for ns2 on other broker with a redirect
        // otheradmin.namespaces().unload("prop-xyz/use/ns2");
        tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);
    }

    @Test(dataProvider = "topicName")
    public void persistentTopics(String topicName) throws Exception {
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());

        final String persistentTopicName = "persistent://prop-xyz/use/ns1/" + topicName;
        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/" + topicName, 0);
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"),
                List.of("persistent://prop-xyz/use/ns1/" + topicName));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(persistentTopicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(persistentTopicName), List.of("my-sub"));

        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/" + topicName, 10);

        TopicStats topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), new TreeSet<>(List.of("my-sub")));
        assertEquals(topicStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 10);
        assertEquals(topicStats.getPublishers().size(), 0);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(persistentTopicName, false);
        assertEquals(internalStats.cursors.keySet(), new TreeSet<>(List.of("my-sub")));

        List<Message<byte[]>> messages = admin.topics().peekMessages(persistentTopicName, "my-sub", 3);
        assertEquals(messages.size(), 3);
        for (int i = 0; i < 3; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        messages = admin.topics().peekMessages(persistentTopicName, "my-sub", 15);
        assertEquals(messages.size(), 10);
        for (int i = 0; i < 10; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        admin.topics().skipMessages(persistentTopicName, "my-sub", 5);
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 5);

        admin.topics().skipAllMessages(persistentTopicName, "my-sub");
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 0);

        consumer.close();
        client.close();

        admin.topics().deleteSubscription(persistentTopicName, "my-sub");

        assertEquals(admin.topics().getSubscriptions(persistentTopicName), new ArrayList<>());
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), new TreeSet<>());
        assertEquals(topicStats.getPublishers().size(), 0);

        try {
            admin.topics().skipAllMessages(persistentTopicName, "my-sub");
        } catch (NotFoundException e) {
        }

        admin.topics().delete(persistentTopicName);

        try {
            admin.topics().delete(persistentTopicName);
            fail("Should have received 404");
        } catch (NotFoundException e) {
        }

        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());
    }

    @Test(dataProvider = "topicName")
    public void partitionedTopics(String topicName) throws Exception {
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/use/ns1"), new ArrayList<>());
        final String partitionedTopicName = "persistent://prop-xyz/use/ns1/" + topicName;
        admin.topics().createPartitionedTopic(partitionedTopicName, 4);
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/use/ns1"),
                List.of(partitionedTopicName));

        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 4);

        List<String> topics = admin.topics().getList("prop-xyz/use/ns1");
        assertEquals(topics.size(), 4);

        try {
            admin.topics().getPartitionedTopicMetadata("persistent://prop-xyz/use/ns1/ds2");
            fail("getPartitionedTopicMetadata of persistent://prop-xyz/use/ns1/ds2 should not succeed");
        } catch (NotFoundException expected) {
        }

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(partitionedTopicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), List.of("my-sub"));

        try {
            admin.topics().deleteSubscription(partitionedTopicName, "my-sub");
            fail("should have failed");
        } catch (PulsarAdminException.PreconditionFailedException e) {
            // ok
        } catch (Exception e) {
            fail(e.getMessage());
        }

        List<String> subscriptions = admin.topics().getSubscriptions(partitionedTopicName);
        assertEquals(subscriptions.size(), 1);

        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName("my-sub-1")
                .subscribe();

        assertEquals(new HashSet<>(admin.topics().getSubscriptions(partitionedTopicName)),
                Set.of("my-sub", "my-sub-1"));

        consumer1.close();
        admin.topics().deleteSubscription(partitionedTopicName, "my-sub-1");
        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), List.of("my-sub"));

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic(partitionedTopicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();

        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        assertEquals(new HashSet<>(admin.topics().getList("prop-xyz/use/ns1")),
                Set.of(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.getSubscriptions().keySet(), new TreeSet<>(List.of("my-sub")));
        assertEquals(topicStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 10);
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.getPartitions(), new HashMap<>());

        // test per partition stats for partitioned topic
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.getMetadata().partitions, 4);
        assertEquals(topicStats.getPartitions().keySet(),
                Set.of(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));
        TopicStats partitionStats = topicStats.getPartitions().get(partitionedTopicName + "-partition-0");
        assertEquals(partitionStats.getPublishers().size(), 1);
        assertEquals(partitionStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
        assertEquals(partitionStats.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);

        try {
            admin.topics().skipMessages(partitionedTopicName, "my-sub", 5);
            fail("skip messages for partitioned topics should fail");
        } catch (Exception e) {
            // ok
        }

        admin.topics().skipAllMessages(partitionedTopicName, "my-sub");
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 0);

        producer.close();
        consumer.close();

        admin.topics().deleteSubscription(partitionedTopicName, "my-sub");

        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), new ArrayList<>());

        try {
            admin.topics().createPartitionedTopic(partitionedTopicName, 32);
            fail("Should have failed as the partitioned topic exists with its partition created");
        } catch (ConflictException ignore) {
        }

        producer = client.newProducer(Schema.BYTES)
            .topic(partitionedTopicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        topics = admin.topics().getList("prop-xyz/use/ns1");
        assertEquals(topics.size(), 4);

        try {
            admin.topics().deletePartitionedTopic(partitionedTopicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException pfe) {
            // ok
        }

        producer.close();
        client.close();

        admin.topics().deletePartitionedTopic(partitionedTopicName);

        try {
            admin.topics().getPartitionedTopicMetadata(partitionedTopicName);
            fail("getPartitionedTopicMetadata of " + partitionedTopicName + " should not succeed");
        } catch (NotFoundException expected) {
        }

        admin.topics().createPartitionedTopic(partitionedTopicName, 32);

        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 32);

        try {
            admin.topics().deletePartitionedTopic("persistent://prop-xyz/use/ns1/ds2");
            fail("Should have failed as the partitioned topic was not created");
        } catch (NotFoundException nfe) {
        }

        admin.topics().deletePartitionedTopic(partitionedTopicName);

        // delete a partitioned topic in a global namespace
        admin.topics().createPartitionedTopic(partitionedTopicName, 4);
        admin.topics().deletePartitionedTopic(partitionedTopicName);
    }

    @Test(dataProvider = "numBundles")
    public void testDeleteNamespaceBundle(Integer numBundles) throws Exception {
        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // since we have 2 brokers running, we try to let both of them acquire bundle ownership
        admin.lookups().lookupTopic("persistent://prop-xyz/use/ns1-bundles/ds1");
        admin.lookups().lookupTopic("persistent://prop-xyz/use/ns1-bundles/ds2");
        admin.lookups().lookupTopic("persistent://prop-xyz/use/ns1-bundles/ds3");
        admin.lookups().lookupTopic("persistent://prop-xyz/use/ns1-bundles/ds4");

        assertEquals(admin.namespaces().getTopics("prop-xyz/use/ns1-bundles"), new ArrayList<>());

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1-bundles");
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"), new ArrayList<>());
    }

    @Test
    public void testNamespaceSplitBundle() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/use/ns1";
        final String topicName = "persistent://" + namespace + "/ds2";
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), List.of(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        producer.close();
    }

    @Test
    public void testNamespaceSplitBundleConcurrent() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/use/ns1";
        final String topicName = "persistent://" + namespace + "/ds2";
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), List.of(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", false, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        String[] splitRange = {namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff"};
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newCachedThreadPool();


        try {
            executorService.invokeAll(
                Arrays.asList(
                    () ->
                    {
                        log.info("split 2 bundles at the same time. spilt: 0x00000000_0x7fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x7fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 2 bundles at the same time. spilt: 0x7fffffff_0xffffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xffffffff", false, null);
                        return null;
                    }
                )
            );
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange4 = {
            namespace + "/0x00000000_0x3fffffff",
            namespace + "/0x3fffffff_0x7fffffff",
            namespace + "/0x7fffffff_0xbfffffff",
            namespace + "/0xbfffffff_0xffffffff"};
        bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 4);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange4[i]);
        }

        try {
            executorService.invokeAll(
                Arrays.asList(
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x00000000_0x3fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x3fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x3fffffff_0x7fffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x3fffffff_0x7fffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0x7fffffff_0xbfffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xbfffffff", false, null);
                        return null;
                    },
                    () ->
                    {
                        log.info("split 4 bundles at the same time. spilt: 0xbfffffff_0xffffffff ");
                        admin.namespaces().splitNamespaceBundle(namespace, "0xbfffffff_0xffffffff", false, null);
                        return null;
                    }
                )
            );
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange8 = {
            namespace + "/0x00000000_0x1fffffff",
            namespace + "/0x1fffffff_0x3fffffff",
            namespace + "/0x3fffffff_0x5fffffff",
            namespace + "/0x5fffffff_0x7fffffff",
            namespace + "/0x7fffffff_0x9fffffff",
            namespace + "/0x9fffffff_0xbfffffff",
            namespace + "/0xbfffffff_0xdfffffff",
            namespace + "/0xdfffffff_0xffffffff"};
        bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 8);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange8[i]);
        }

        producer.close();
    }

    @Test
    public void testNamespaceUnloadBundle() throws Exception {
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());

        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"),
                List.of("persistent://prop-xyz/use/ns1/ds2"));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1/ds2")
                .subscriptionName("my-sub").subscribe();
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1/ds2"),
                List.of("my-sub"));

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://prop-xyz/use/ns1/ds2")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        consumer.close();
        producer.close();

        try {
            admin.namespaces().unloadNamespaceBundle("prop-xyz/use/ns1", "0x00000000_0xffffffff");
        } catch (Exception e) {
            fail("Unload shouldn't have throw exception");
        }

        // check that no one owns the namespace
        NamespaceBundle bundle = bundleFactory.getBundle(NamespaceName.get("prop-xyz/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        assertFalse(pulsar.getNamespaceService().isServiceUnitOwned(bundle));
        assertFalse(otherPulsar.getNamespaceService().isServiceUnitOwned(bundle));
        pulsarClient.shutdown();

        LOG.info("--- RELOAD ---");

        // Force reload of namespace and wait for topic to be ready
        for (int i = 0; i < 30; i++) {
            try {
                admin.topics().getStats("persistent://prop-xyz/use/ns1/ds2");
                break;
            } catch (PulsarAdminException e) {
                LOG.warn("Failed to get topic stats.. {}", e.getMessage());
                Thread.sleep(1000);
            }
        }

        admin.topics().deleteSubscription("persistent://prop-xyz/use/ns1/ds2", "my-sub");
        admin.topics().delete("persistent://prop-xyz/use/ns1/ds2");
    }

    @Test(dataProvider = "numBundles")
    public void testNamespaceBundleUnload(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        assertEquals(admin.topics().getList("prop-xyz/use/ns1-bundles"), new ArrayList<>());

        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1-bundles/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/use/ns1-bundles"),
                List.of("persistent://prop-xyz/use/ns1-bundles/ds2"));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub").subscribe();
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                List.of("my-sub"));

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://prop-xyz/use/ns1-bundles/ds2")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        NamespaceBundle bundle = pulsar.getNamespaceService()
                .getBundle(TopicName.get("persistent://prop-xyz/use/ns1-bundles/ds2"));

        consumer.close();
        producer.close();

        admin.namespaces().unloadNamespaceBundle("prop-xyz/use/ns1-bundles", bundle.getBundleRange());

        // check that no one owns the namespace bundle
        assertFalse(pulsar.getNamespaceService().isServiceUnitOwned(bundle));
        assertFalse(otherPulsar.getNamespaceService().isServiceUnitOwned(bundle));

        LOG.info("--- RELOAD ---");

        // Force reload of namespace and wait for topic to be ready
        for (int i = 0; i < 30; i++) {
            try {
                admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2");
                break;
            } catch (PulsarAdminException e) {
                LOG.warn("Failed to get topic stats.. {}", e.getMessage());
                Thread.sleep(1000);
            }
        }

        admin.topics().deleteSubscription("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub");
        admin.topics().delete("persistent://prop-xyz/use/ns1-bundles/ds2");
    }

    @Test(dataProvider = "bundling")
    public void testClearBacklogOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // create consumer and subscription
        @Cleanup
        Consumer<byte[]> subscribe =
                pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2").subscriptionName("my-sub")
                        .subscribe();
        @Cleanup
        Consumer<byte[]> subscribe1 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub-1")
                .subscribe();
        @Cleanup
        Consumer<byte[]> subscribe2 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub-2")
                .subscribe();
        @Cleanup
        Consumer<byte[]> subscribe3 =
                pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds1").subscriptionName("my-sub")
                        .subscribe();
        @Cleanup
        Consumer<byte[]> subscribe4 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds1")
                .subscriptionName("my-sub-1")
                .subscribe();

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://prop-xyz/use/ns1-bundles/ds2")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();

        // Create producer
        Producer<byte[]> producer1 = pulsarClient.newProducer(Schema.BYTES)
            .topic("persistent://prop-xyz/use/ns1-bundles/ds1")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer1.send(message.getBytes());
        }

        producer1.close();

        admin.namespaces().clearNamespaceBacklogForSubscription("prop-xyz/use/ns1-bundles", "my-sub");

        long backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 10);

        admin.namespaces().clearNamespaceBacklog("prop-xyz/use/ns1-bundles");

        backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub-2").getMsgBacklog();
        assertEquals(backlog, 0);
    }

    @Test(dataProvider = "bundling")
    public void testUnsubscribeOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // create consumer and subscription
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub").subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub-1").subscribe();
        /* Consumer consumer3 = */ pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds2")
                .subscriptionName("my-sub-2").subscribe().close();
        Consumer<byte[]> consumer4 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds1")
                .subscriptionName("my-sub").subscribe();
        Consumer<byte[]> consumer5 = pulsarClient.newConsumer().topic("persistent://prop-xyz/use/ns1-bundles/ds1")
                .subscriptionName("my-sub-1").subscribe();

        try {
            admin.namespaces().unsubscribeNamespace("prop-xyz/use/ns1-bundles", "my-sub");
            fail("should have failed");
        } catch (PulsarAdminException.PreconditionFailedException e) {
            // ok
        }

        consumer1.close();

        try {
            admin.namespaces().unsubscribeNamespace("prop-xyz/use/ns1-bundles", "my-sub");
            fail("should have failed");
        } catch (PulsarAdminException.PreconditionFailedException e) {
            // ok
        }

        consumer4.close();

        admin.namespaces().unsubscribeNamespace("prop-xyz/use/ns1-bundles", "my-sub");

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                List.of("my-sub-1", "my-sub-2"));
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds1"),
                List.of("my-sub-1"));

        consumer2.close();
        consumer5.close();

        admin.namespaces().unsubscribeNamespace("prop-xyz/use/ns1-bundles", "my-sub-1");

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                List.of("my-sub-2"));
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds1"),
                new ArrayList<>());
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages) throws Exception {
        publishMessagesOnPersistentTopic(topicName, messages, 0);
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }

    @Test
    public void backlogQuotas() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1"),
                new HashMap<>());

        Map<BacklogQuotaType, BacklogQuota> quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 0);
        assertNull(quotaMap.get(BacklogQuotaType.destination_storage));

        admin.namespaces().setBacklogQuota("prop-xyz/use/ns1",
                BacklogQuota.builder()
                        .limitSize(1 * 1024 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build());
        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 1);
        assertEquals(quotaMap.get(BacklogQuotaType.destination_storage), BacklogQuota.builder()
                .limitSize(1 * 1024 * 1024)
                .retentionPolicy(RetentionPolicy.producer_exception)
                .build());

        admin.namespaces().removeBacklogQuota("prop-xyz/use/ns1");

        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 0);
        assertNull(quotaMap.get(BacklogQuotaType.destination_storage));
    }

    @Test
    public void statsOnNonExistingTopics() throws Exception {
        try {
            admin.topics().getStats("persistent://prop-xyz/use/ns1/ghostTopic");
            fail("The topic doesn't exist");
        } catch (NotFoundException e) {
            // OK
        }
    }

    @Test
    public void testDeleteFailedReturnCode() throws Exception {
        String topicName = "persistent://prop-xyz/use/ns1/my-topic";
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
            .topic(topicName)
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.SinglePartition)
            .create();

        try {
            admin.topics().delete(topicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // OK
        }

        producer.close();

        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName).subscriptionName("sub").subscribe();

        try {
            admin.topics().delete(topicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // OK
        }

        try {
            admin.topics().deleteSubscription(topicName, "sub");
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // Ok
        }

        consumer.close();

        // Now should succeed
        admin.topics().delete(topicName);
    }

    private static class IncompatiblePropertyAdmin {
        public Set<String> allowedClusters;
        public int someNewIntField;
        public String someNewString;
    }

    @Test
    public void testJacksonWithTypeDifferencies() throws Exception {
        String expectedJson = "{\"adminRoles\":[\"role1\",\"role2\"],\"allowedClusters\":[\"usw\",\"use\"]}";
        IncompatiblePropertyAdmin r1 = ObjectMapperFactory.getMapper().reader().forType(IncompatiblePropertyAdmin.class)
                .readValue(expectedJson);
        assertEquals(r1.allowedClusters, Set.of("use", "usw"));
        assertEquals(r1.someNewIntField, 0);
        assertNull(r1.someNewString);
    }

    @Test
    public void testBackwardCompatiblity() throws Exception {
        Set<String> allowedClusters = Set.of("use");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"), allowedClusters);
        admin.tenants().createTenant("prop-xyz2", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz2/use/ns1");

        assertEquals(admin.tenants().getTenants(), List.of("prop-xyz", "prop-xyz2"));
        assertEquals(admin.tenants().getTenantInfo("prop-xyz2").getAdminRoles(),
                List.of("role1", "role2"));
        assertEquals(admin.tenants().getTenantInfo("prop-xyz2").getAllowedClusters(), Set.of("use"));

        // Try to deserialize property JSON with IncompatiblePropertyAdmin format
        // it should succeed ignoring missing fields
        TenantsImpl properties = (TenantsImpl) admin.tenants();
        IncompatiblePropertyAdmin result = properties.request(properties.getWebTarget().path("prop-xyz2"))
                .get(IncompatiblePropertyAdmin.class);

        assertEquals(result.allowedClusters, Set.of("use"));
        assertEquals(result.someNewIntField, 0);
        assertNull(result.someNewString);

        admin.namespaces().deleteNamespace("prop-xyz2/use/ns1");
        admin.tenants().deleteTenant("prop-xyz2");
        assertEquals(admin.tenants().getTenants(), Set.of("prop-xyz"));
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());

        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), List.of("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long messageTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 5, 5);

        List<Message<byte[]>> messages = admin.topics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
        }
        // messages should still be available due to retention

        admin.topics().resetCursor(topicName, "my-sub", messageTimestamp);

        int receivedAfterReset = 0;

        for (int i = 5; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(message.getData(), expected.getBytes());
        }
        assertEquals(receivedAfterReset, 5);

        consumer.close();

        admin.topics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.topics().getSubscriptions(topicName), new ArrayList<>());
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorResetAfterReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());

        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), List.of("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long firstTimestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 3, 5);

        Thread.sleep(1);
        long secondTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 2, 8);

        List<Message<byte[]>> messages = admin.topics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);
        messages.forEach(message -> {
            LOG.info("Peeked message: {}", new String(message.getData()));
        });

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
        }

        admin.topics().resetCursor(topicName, "my-sub", firstTimestamp);

        int receivedAfterReset = 0;

        // Should received messages from 5-9
        for (int i = 5; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(new String(message.getData()), expected);
        }
        assertEquals(receivedAfterReset, 5);

        // Reset at 2nd timestamp
        receivedAfterReset = 0;
        admin.topics().resetCursor(topicName, "my-sub", secondTimestamp);

        // Should received messages from 8-9
        for (int i = 8; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(new String(message.getData()), expected);
        }
        assertEquals(receivedAfterReset, 2);

        consumer.close();
        admin.topics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.topics().getSubscriptions(topicName), new ArrayList<>());
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void partitionedTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));
        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        admin.topics().createPartitionedTopic(topicName, 4);

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        List<String> topics = admin.topics().getList("prop-xyz/use/ns1");
        assertEquals(topics.size(), 4);

        assertEquals(admin.topics().getSubscriptions(topicName), List.of("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);
        Thread.sleep(1);

        long timestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 5, 5);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
        }
        // messages should still be available due to retention

        admin.topics().resetCursor(topicName, "my-sub", timestamp);

        Set<String> expectedMessages = new HashSet<>();
        Set<String> receivedMessages = new HashSet<>();
        for (int i = 5; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
            expectedMessages.add("message-" + i);
            receivedMessages.add(new String(message.getData()));
        }

        receivedMessages.removeAll(expectedMessages);
        assertEquals(receivedMessages.size(), 0);

        consumer.close();
        admin.topics().deleteSubscription(topicName, "my-sub");
        admin.topics().deletePartitionedTopic(topicName);
    }

    @Test
    public void persistentTopicsInvalidCursorReset() throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), new ArrayList<>());

        String topicName = "persistent://prop-xyz/use/ns1/invalidcursorreset";
        // Force to create a topic
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"), List.of(topicName));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), List.of("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 10);

        List<Message<byte[]>> messages = admin.topics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
        }
        // use invalid timestamp
        try {
            admin.topics().resetCursor(topicName, "my-sub", System.currentTimeMillis() - 190000);
        } catch (Exception e) {
            // fail the test
            throw e;
        }

        admin.topics().resetCursor(topicName, "my-sub", System.currentTimeMillis() + 90000);
        consumer = client.newConsumer().topic(topicName).subscriptionName("my-sub").subscribe();
        consumer.close();
        client.close();

        admin.topics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.topics().getSubscriptions(topicName), new ArrayList<>());
        admin.topics().delete(topicName);
    }

    @Value
    @Builder
    static class CustomTenantAdmin implements TenantInfo {
        private final int newTenant;
        private final Set<String> adminRoles;
        private final Set<String> allowedClusters;
    }

    @Test
    public void testObjectWithUnknownProperties() {
        TenantInfo pa = TenantInfo.builder()
                .adminRoles(Set.of("test_appid1", "test_appid2"))
                .allowedClusters(Set.of("use"))
                .build();
        CustomTenantAdmin cpa = CustomTenantAdmin.builder()
                .adminRoles(pa.getAdminRoles())
                .allowedClusters(pa.getAllowedClusters())
                .newTenant(100)
                .build();

        try {
            admin.tenants().createTenant("test-property", cpa);
        } catch (Exception e) {
            fail("Should not happen : ", e);
        }
    }

    /**
     * <pre>
     * Verify: PersistentTopicsBase.expireMessages()/expireMessagesForAllSubscriptions()
     * 1. Created multiple shared subscriptions and publisher on topic
     * 2. Publish messages on the topic
     * 3. expire message on sub-1 : backlog for sub-1 must be 0
     * 4. expire message on all subscriptions: backlog for all subscription must be 0
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testPersistentTopicsExpireMessages() throws Exception {
        cleanup();
        setup();

        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/use/ns1"),
                List.of("persistent://prop-xyz/use/ns1/ds2"));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer().topic("persistent://prop-xyz/use/ns1/ds2")
                .subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer1 = consumerBuilder.clone().subscriptionName("my-sub1").subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone().subscriptionName("my-sub2").subscribe();
        Consumer<byte[]> consumer3 = consumerBuilder.clone().subscriptionName("my-sub3").subscribe();

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/use/ns1/ds2").size(), 3);

        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 10);

        TopicStats topicStats = admin.topics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.getSubscriptions().get("my-sub1").getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get("my-sub2").getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get("my-sub3").getMsgBacklog(), 10);

        Thread.sleep(1000); // wait for 1 seconds to expire message
        admin.topics().expireMessages("persistent://prop-xyz/use/ns1/ds2", "my-sub1", 1);
        Thread.sleep(1000); // wait for 1 seconds to execute expire message as it is async

        topicStats = admin.topics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.getSubscriptions().get("my-sub1").getMsgBacklog(), 0);
        assertEquals(topicStats.getSubscriptions().get("my-sub2").getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get("my-sub3").getMsgBacklog(), 10);

        try {
            admin.topics().expireMessagesForAllSubscriptions("persistent://prop-xyz/use/ns1/ds2", 1);
        } catch (Exception e) {
            // my-sub1 has no msg backlog, so expire message won't be issued on that subscription
            assertTrue(e.getMessage().startsWith("Expire message by timestamp not issued on topic"));
        }
        Thread.sleep(1000); // wait for 1 seconds to execute expire message as it is async

        topicStats = admin.topics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.getSubscriptions().get("my-sub1").getMsgBacklog(), 0);
        assertEquals(topicStats.getSubscriptions().get("my-sub2").getMsgBacklog(), 0);
        assertEquals(topicStats.getSubscriptions().get("my-sub3").getMsgBacklog(), 0);

        consumer1.close();
        consumer2.close();
        consumer3.close();

    }

    /**
     * Verify: PersistentTopicsBase.expireMessages()/expireMessagesForAllSubscriptions() for PartitionTopic
     *
     * @throws Exception
     */
    @Test
    public void testPersistentTopicExpireMessageOnPartitionTopic() throws Exception {

        admin.topics().createPartitionedTopic("persistent://prop-xyz/use/ns1/ds1", 4);

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic("persistent://prop-xyz/use/ns1/ds1")
                .subscriptionName("my-sub").subscribe();

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
            .topic("persistent://prop-xyz/use/ns1/ds1")
            .enableBatching(false)
            .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
            .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        PartitionedTopicStats topicStats = admin.topics()
                .getPartitionedStats("persistent://prop-xyz/use/ns1/ds1", true);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 10);

        TopicStats partitionStatsPartition0 = topicStats.getPartitions()
                .get("persistent://prop-xyz/use/ns1/ds1-partition-0");
        TopicStats partitionStatsPartition1 = topicStats.getPartitions()
                .get("persistent://prop-xyz/use/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);
        assertEquals(partitionStatsPartition1.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);

        Thread.sleep(1000);
        admin.topics().expireMessagesForAllSubscriptions("persistent://prop-xyz/use/ns1/ds1", 1);
        Thread.sleep(1000);

        topicStats = admin.topics().getPartitionedStats("persistent://prop-xyz/use/ns1/ds1", true);
        partitionStatsPartition0 = topicStats.getPartitions().get("persistent://prop-xyz/use/ns1/ds1-partition-0");
        partitionStatsPartition1 = topicStats.getPartitions().get("persistent://prop-xyz/use/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.getSubscriptions().get("my-sub").getMsgBacklog(), 0);
        assertEquals(partitionStatsPartition1.getSubscriptions().get("my-sub").getMsgBacklog(), 0);

        producer.close();
        consumer.close();
    }

    /**
     * This test-case verifies that broker should support both url/uri encoding for topic-name. It calls below api with
     * url-encoded and also uri-encoded topic-name in http request: a. PartitionedMetadataLookup b. TopicLookupBase c. Topic
     * Stats
     *
     * @param topicName
     * @throws Exception
     */
    @Test(dataProvider = "topicName")
    public void testPulsarAdminForUriAndUrlEncoding(String topicName) throws Exception {
        final String ns1 = "prop-xyz/use/ns1";
        final String topic1 = "persistent://" + ns1 + "/" + topicName;
        final String urlEncodedTopic = Codec.encode(topicName);
        final String uriEncodedTopic = urlEncodedTopic.replaceAll("\\+", "%20");
        final int numOfPartitions = 4;
        admin.topics().createPartitionedTopic(topic1, numOfPartitions);
        // Create a consumer to get stats on this topic
        pulsarClient.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe().close();

        TopicsImpl persistent = (TopicsImpl) admin.topics();
        Field field = TopicsImpl.class.getDeclaredField("adminTopics");
        field.setAccessible(true);
        WebTarget persistentTopics = ((WebTarget) field.get(persistent)).path("persistent");

        // (1) Get PartitionedMetadata : with Url and Uri encoding
        final CompletableFuture<PartitionedTopicMetadata> urlEncodedPartitionedMetadata = new CompletableFuture<>();
        // (a) Url encoding
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(urlEncodedTopic).path("partitions"),
                new InvocationCallback<PartitionedTopicMetadata>() {
                    @Override
                    public void completed(PartitionedTopicMetadata response) {
                        urlEncodedPartitionedMetadata.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        urlEncodedPartitionedMetadata.completeExceptionally(e);
                    }
                });
        final CompletableFuture<PartitionedTopicMetadata> uriEncodedPartitionedMetadata = new CompletableFuture<>();
        // (b) Uri encoding
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(uriEncodedTopic).path("partitions"),
                new InvocationCallback<PartitionedTopicMetadata>() {
                    @Override
                    public void completed(PartitionedTopicMetadata response) {
                        uriEncodedPartitionedMetadata.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        uriEncodedPartitionedMetadata.completeExceptionally(e);
                    }
                });
        assertEquals(urlEncodedPartitionedMetadata.get().partitions, numOfPartitions);
        assertEquals(urlEncodedPartitionedMetadata.get().partitions, (uriEncodedPartitionedMetadata.get().partitions));

        // (2) Get Topic Lookup
        LookupImpl lookup = (LookupImpl) admin.lookups();
        Field field2 = LookupImpl.class.getDeclaredField("v2lookup");
        field2.setAccessible(true);
        WebTarget target2 = (WebTarget) field2.get(lookup);
        // (a) Url encoding
        LookupData urlEncodedLookupData = lookup
                .request(target2.path("/destination/persistent").path(ns1 + "/" + urlEncodedTopic))
                .get(LookupData.class);
        // (b) Uri encoding
        LookupData uriEncodedLookupData = lookup
                .request(target2.path("/destination/persistent").path(ns1 + "/" + uriEncodedTopic))
                .get(LookupData.class);
        Assert.assertNotNull(urlEncodedLookupData.getBrokerUrl());
        assertEquals(urlEncodedLookupData.getBrokerUrl(), uriEncodedLookupData.getBrokerUrl());

        // (3) Get Topic Stats
        final CompletableFuture<TopicStats> urlStats = new CompletableFuture<>();
        // (a) Url encoding
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(urlEncodedTopic + "-partition-1").path("stats"),
                new InvocationCallback<TopicStats>() {
                    @Override
                    public void completed(TopicStats response) {
                        urlStats.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        urlStats.completeExceptionally(e);
                    }
                });
        // (b) Uri encoding
        final CompletableFuture<TopicStats> uriStats = new CompletableFuture<>();
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(uriEncodedTopic + "-partition-1").path("stats"),
                new InvocationCallback<TopicStats>() {
                    @Override
                    public void completed(TopicStats response) {
                        uriStats.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        uriStats.completeExceptionally(e);
                    }
                });
        assertEquals(urlStats.get().getSubscriptions().size(), 1);
        assertEquals(uriStats.get().getSubscriptions().size(), 1);
    }

    static class MockedPulsarService extends MockedPulsarServiceBaseTest {

        private final ServiceConfiguration conf;

        public MockedPulsarService(ServiceConfiguration conf) {
            super();
            this.conf = conf;
        }

        @Override
        protected void setup() throws Exception {
            super.conf.setLoadManagerClassName(conf.getLoadManagerClassName());
            super.internalSetup();
        }

        @Override
        protected void cleanup() throws Exception {
            super.internalCleanup();
        }

        public PulsarService getPulsar() {
            return pulsar;
        }

        public PulsarAdmin getAdmin() {
            return admin;
        }
    }

    @Test
    public void testTopicBundleRangeLookup() throws Exception {
        admin.clusters().createCluster("usw", ClusterData.builder().build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Set.of("role1", "role2"),
                Set.of("use", "usw"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/use/getBundleNs", 100);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/getBundleNs").bundles.getNumBundles(), 100);

        // (1) create a topic
        final String topicName = "persistent://prop-xyz/use/getBundleNs/topic1";
        String bundleRange = admin.lookups().getBundleRange(topicName);
        assertEquals(bundleRange, pulsar.getNamespaceService().getBundle(TopicName.get(topicName)).getBundleRange());

        admin.tenants().updateTenant("prop-xyz", new TenantInfoImpl(Set.of("role1", "role2"),
                Set.of("use")));
    }

    @Test
    public void testTriggerCompaction() throws Exception {
        String topicName = "persistent://prop-xyz/use/ns1/topic1";

        // create a topic by creating a producer
        pulsarClient.newProducer(Schema.BYTES).topic(topicName).create().close();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // mock actual compaction, we don't need to really run it
        CompletableFuture<Long> promise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(promise).when(compactor).compact(topicName);
        admin.topics().triggerCompaction(topicName);

        // verify compact called once
        verify(compactor).compact(topicName);
        try {
            admin.topics().triggerCompaction(topicName);

            fail("Shouldn't be able to run while already running");
        } catch (ConflictException e) {
            // expected
        }
        // compact shouldn't have been called again
        verify(compactor).compact(topicName);

        // complete first compaction, and trigger again
        promise.complete(1L);
        admin.topics().triggerCompaction(topicName);

        // verify compact was called again
        verify(compactor, times(2)).compact(topicName);
    }

    @Test
    public void testCompactionStatus() throws Exception {
        String topicName = "persistent://prop-xyz/use/ns1/topic1";

        // create a topic by creating a producer
        pulsarClient.newProducer(Schema.BYTES).topic(topicName).create().close();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        assertEquals(admin.topics().compactionStatus(topicName).status,
            LongRunningProcessStatus.Status.NOT_RUN);

        // mock actual compaction, we don't need to really run it
        CompletableFuture<Long> promise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(promise).when(compactor).compact(topicName);
        admin.topics().triggerCompaction(topicName);

        assertEquals(admin.topics().compactionStatus(topicName).status,
            LongRunningProcessStatus.Status.RUNNING);

        promise.complete(1L);

        assertEquals(admin.topics().compactionStatus(topicName).status,
            LongRunningProcessStatus.Status.SUCCESS);

        CompletableFuture<Long> errorPromise = new CompletableFuture<>();
        doReturn(errorPromise).when(compactor).compact(topicName);
        admin.topics().triggerCompaction(topicName);
        errorPromise.completeExceptionally(new Exception("Failed at something"));

        assertEquals(admin.topics().compactionStatus(topicName).status,
            LongRunningProcessStatus.Status.ERROR);
        assertTrue(admin.topics().compactionStatus(topicName)
            .lastError.contains("Failed at something"));
    }

    private void clearCache() {
        ((MetadataCacheImpl) pulsar.getPulsarResources().getNamespaceResources().getCache()).invalidateAll();
    }
}
