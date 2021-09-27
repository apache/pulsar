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
package org.apache.pulsar.broker.admin;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response.Status;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.impl.SimpleLoadManagerImpl;
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
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceBundles;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.PartitionedManagedLedgerInfo;
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
import org.apache.pulsar.common.policies.data.BrokerInfo;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ConsumerStats;
import org.apache.pulsar.common.policies.data.NamespaceIsolationData;
import org.apache.pulsar.common.policies.data.NamespaceIsolationDataImpl;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.policies.data.PartitionedTopicInternalStats;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistencePolicies;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PoliciesUtil;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.SubscriptionStats;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


@Slf4j
@Test(groups = "broker")
public class AdminApiTest extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiTest.class);

    private final String TLS_SERVER_CERT_FILE_PATH = "./src/test/resources/certificate/server.crt";
    private final String TLS_SERVER_KEY_FILE_PATH = "./src/test/resources/certificate/server.key";

    private MockedPulsarService mockPulsarSetup;

    private PulsarService otherPulsar;

    private PulsarAdmin adminTls;
    private PulsarAdmin otheradmin;

    private NamespaceBundleFactory bundleFactory;

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(TLS_SERVER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(TLS_SERVER_KEY_FILE_PATH);
        conf.setMessageExpiryCheckIntervalInMinutes(1);
        conf.setSubscriptionExpiryCheckIntervalInMinutes(1);
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
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
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test"));
        admin.tenants().createTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/ns1", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        adminTls.close();
        super.internalCleanup();
        mockPulsarSetup.cleanup();
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
        return new Object[][] { { "topic_+&*%{}() \\$@#^%" }, { "simple-topicName" } };
    }

    @DataProvider(name = "topicType")
    public Object[][] topicTypeProvider() {
        return new Object[][] { { TopicDomain.persistent.value() }, { TopicDomain.non_persistent.value() } };
    }

    @DataProvider(name = "topicNamesForAllTypes")
    public Object[][] topicNamesForAllTypesProvider() {
        final List<Object[]> topicNames = new ArrayList<>();
        for (int i = 0; i < topicTypeProvider().length; i++) {
            for (int j = 0; j < topicNamesProvider().length; j++) {
                topicNames.add(new Object[]{ topicTypeProvider()[i][0], topicNamesProvider()[j][0] });
            }
        }
        return topicNames.toArray(new Object[topicNamesProvider().length * topicTypeProvider().length][]);
    }

    @Test
    public void clusters() throws Exception {
        admin.clusters().createCluster("usw",
                ClusterData.builder().serviceUrl("http://broker.messaging.use.example.com:8080").build());
        // "test" cluster is part of config-default cluster and it's znode gets created when PulsarService creates
        // failure-domain znode of this default cluster
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("test", "usw"));

        assertEquals(admin.clusters().getCluster("test"),
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        admin.clusters().updateCluster("usw",
                ClusterData.builder().serviceUrl("http://new-broker.messaging.usw.example.com:8080").build());
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("test", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                ClusterData.builder().serviceUrl("http://new-broker.messaging.usw.example.com:8080").build());

        admin.clusters().updateCluster("usw",
                ClusterData.builder()
                        .serviceUrl("http://new-broker.messaging.usw.example.com:8080")
                        .serviceUrlTls("https://new-broker.messaging.usw.example.com:4443")
                        .build());
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("test", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                ClusterData.builder()
                        .serviceUrl("http://new-broker.messaging.usw.example.com:8080")
                        .serviceUrlTls("https://new-broker.messaging.usw.example.com:4443")
                        .build());

        admin.clusters().deleteCluster("usw");
        Awaitility.await()
                .untilAsserted(() -> assertEquals(admin.clusters().getClusters(), Lists.newArrayList("test")));

        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        admin.clusters().deleteCluster("test");
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList());

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
                    .namespaces(Collections.singletonList("other/use/other.*"))
                    .primary(Lists.newArrayList("prod1-broker[4-6].messaging.use.example.com"))
                    .secondary(Lists.newArrayList("prod1-broker.*.messaging.use.example.com"))
                    .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                            .policyType(AutoFailoverPolicyType.min_available)
                            .parameters(parameters1)
                            .build())
                    .build();

            admin.clusters().createNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);

            String policyName2 = "policy-2";
            Map<String, String> parameters2 = new HashMap<>();
            parameters2.put("min_limit", "1");
            parameters2.put("usage_threshold", "100");

            NamespaceIsolationData nsPolicyData2 = NamespaceIsolationData.builder()
                    .namespaces(Collections.singletonList("other/use/other.*"))
                    .primary(Collections.singletonList("prod1-broker[4-6].messaging.use.example.com"))
                    .secondary(Collections.singletonList("prod1-broker.*.messaging.use.example.com"))
                    .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                            .policyType(AutoFailoverPolicyType.min_available)
                            .parameters(parameters1)
                            .build())
                    .build();
            admin.clusters().createNamespaceIsolationPolicy("test", policyName2, nsPolicyData2);

            // verify create indirectly with get
            Map<String, ? extends NamespaceIsolationData> policiesMap = admin.clusters().getNamespaceIsolationPolicies("test");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);
            assertEquals(policiesMap.get(policyName2), nsPolicyData2);

            // verify update of primary
            nsPolicyData1.getPrimary().remove(0);
            nsPolicyData1.getPrimary().add("prod1-broker[1-2].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);

            // verify primary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("test");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of secondary
            nsPolicyData1.getSecondary().remove(0);
            nsPolicyData1.getSecondary().add("prod1-broker[3-4].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);

            // verify secondary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("test");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover policy limit
            nsPolicyData1.getAutoFailoverPolicy().getParameters().put("min_limit", "10");
            admin.clusters().updateNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);

            // verify min_limit change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("test");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover usage_threshold limit
            nsPolicyData1.getAutoFailoverPolicy().getParameters().put("usage_threshold", "80");
            admin.clusters().updateNamespaceIsolationPolicy("test", policyName1, nsPolicyData1);

            // verify usage_threshold change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("test");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify single get
            NamespaceIsolationDataImpl policy1Data = (NamespaceIsolationDataImpl) admin.clusters().getNamespaceIsolationPolicy("test", policyName1);
            assertEquals(policy1Data, nsPolicyData1);

            // verify creation of more than one policy
            admin.clusters().createNamespaceIsolationPolicy("test", policyName2, nsPolicyData1);

            try {
                admin.clusters().getNamespaceIsolationPolicy("test", "no-such-policy");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }
            // verify delete cluster failed
            try {
                admin.clusters().deleteCluster("test");
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof PreconditionFailedException);
            }

            // verify delete
            admin.clusters().deleteNamespaceIsolationPolicy("test", policyName1);
            admin.clusters().deleteNamespaceIsolationPolicy("test", policyName2);

            try {
                admin.clusters().getNamespaceIsolationPolicy("test", policyName1);
                fail("should have raised exception");
            } catch (PulsarAdminException e) {
                assertTrue(e instanceof NotFoundException);
            }

            try {
                admin.clusters().getNamespaceIsolationPolicy("test", policyName2);
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

        // validate regex: invlid regex for primary and seconday
        Map<String, String> parameters = new HashMap<>();
        parameters.put("min_limit", "1");
        parameters.put("usage_threshold", "100");

        NamespaceIsolationData.Builder nsRegexPolicy = NamespaceIsolationData.builder()
                .namespaces(Collections.singletonList("other/use/other.*"))
                .primary(Lists.newArrayList("prod1-broker[45-46].messaging.use.example.com"))
                .autoFailoverPolicy(AutoFailoverPolicyData.builder()
                        .policyType(AutoFailoverPolicyType.min_available)
                        .parameters(parameters)
                        .build());
        try {
            admin.clusters().createNamespaceIsolationPolicy("test", "invalid_primary", nsRegexPolicy.build());
            fail("should have failed with invalid regex");
        }catch (PulsarAdminException e) {
            //Ok
        }

        nsRegexPolicy.primary(Lists.newArrayList("prod1-broker[45-46].messaging.use.example.com",
                "prod1-broker[4-5].messaging.use.example.com"))
                .secondary(Collections.singletonList("prod1-broker[45-46].messaging.use.example.com"));
        try {
            admin.clusters().createNamespaceIsolationPolicy("test", "invalid_primary", nsRegexPolicy.build());
            fail("should have failed with invalid regex");
        } catch (PulsarAdminException e) {
            // Ok
        }

    }

    @Test
    public void brokers() throws Exception {
        List<String> list = admin.brokers().getActiveBrokers("test");
        Assert.assertNotNull(list);
        Assert.assertEquals(list.size(), 1);

        List<String> list2 = otheradmin.brokers().getActiveBrokers("test");
        Assert.assertNotNull(list2);
        Assert.assertEquals(list2.size(), 1);

        BrokerInfo leaderBroker = admin.brokers().getLeaderBroker();
        Assert.assertEquals(leaderBroker.getServiceUrl(), pulsar.getLeaderElectionService().getCurrentLeader().map(LeaderBroker::getServiceUrl).get());

        Map<String, NamespaceOwnershipStatus> nsMap = admin.brokers().getOwnedNamespaces("test", list.get(0));
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
        Map<String, NamespaceOwnershipStatus> nsMap2 = adminTls.brokers().getOwnedNamespaces("test",
                String.format("%s:%d", parts[0], pulsar.getListenPortHTTPS().get()));
        Assert.assertEquals(nsMap2.size(), 2);

        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        admin.clusters().deleteCluster("test");
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList());
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
            admin.brokers().updateDynamicConfiguration("zookeeperServers", "test-zk:1234");
        } catch (Exception e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // (3) try to update non-existent field
        try {
            admin.brokers().updateDynamicConfiguration("test", Long.toString(shutdownTime));
        } catch (Exception e) {
            assertTrue(e instanceof PreconditionFailedException);
        }

        // (4) try to update dynamic-field with special char "/" and "%"
        String user1 = "test/test%&$*/^";
        String user2 = "user2/password";
        final String configValue = user1 + "," + user2;
        admin.brokers().updateDynamicConfiguration("superUserRoles", configValue);
        String storedValue = admin.brokers().getAllDynamicConfigurations().get("superUserRoles");
        assertEquals(configValue, storedValue);
        retryStrategically((test) -> pulsar.getConfiguration().getSuperUserRoles().size() == 2, 5, 200);
        assertTrue(pulsar.getConfiguration().getSuperUserRoles().contains(user1));
        assertTrue(pulsar.getConfiguration().getSuperUserRoles().contains(user2));


        admin.brokers().updateDynamicConfiguration("loadManagerClassName", SimpleLoadManagerImpl.class.getName());
        retryStrategically((test) -> pulsar.getConfiguration().getLoadManagerClassName()
                .equals(SimpleLoadManagerImpl.class.getName()), 150, 5);
        assertEquals(pulsar.getConfiguration().getLoadManagerClassName(), SimpleLoadManagerImpl.class.getName());
        admin.brokers().deleteDynamicConfiguration("loadManagerClassName");
        assertFalse(admin.brokers().getAllDynamicConfigurations().containsKey("loadManagerClassName"));
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
    public void testInvalidDynamicConfigContentInMetadata() throws Exception {
        final int newValue = 10;
        // set invalid data into dynamic-config node so, broker startup fail to deserialize data
        pulsar.getLocalMetadataStore().put("/admin/configuration", "$".getBytes(),
                Optional.empty()).join();
        stopBroker();

        // start broker: it should have set watch even if with failure of deserialization
        startBroker();
        Assert.assertNotEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), newValue);
        // update zk with config-value which should fire watch and broker should update the config value
        Map<String, String> configMap = Maps.newHashMap();
        configMap.put("brokerShutdownTimeoutMs", Integer.toString(newValue));

        pulsar.getLocalMetadataStore().put("/admin/configuration",
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(configMap),
                Optional.empty()).join();
        // wait config to be updated
        Awaitility.await().until(() -> pulsar.getConfiguration().getBrokerShutdownTimeoutMs() == newValue);
        // verify value is updated
        assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), newValue);
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
    @Test(timeOut = 30000)
    public void testUpdateDynamicLocalConfiguration() throws Exception {
        // (1) try to update dynamic field
        final long initValue = 30000;
        final long shutdownTime = 10;
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(initValue);
        // update configuration
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", Long.toString(shutdownTime));
        // verify value is updated
        Awaitility.waitAtMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);
        });
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
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(30000);
        Map<String, String> configs = admin.brokers().getAllDynamicConfigurations();
        assertTrue(configs.isEmpty());
        assertNotEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);
        // update configuration
        admin.brokers().updateDynamicConfiguration(configName, Long.toString(shutdownTime));
        // Now, znode is created: updateConfigurationAndRegisterListeners and check if configuration updated
        assertEquals(Long.parseLong(admin.brokers().getAllDynamicConfigurations().get(configName)), shutdownTime);
    }

    @Test
    public void properties() throws PulsarAdminException {
        try {
            admin.tenants().getTenantInfo("does-not-exist");
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof NotFoundException);
        }

        Set<String> allowedClusters = Sets.newHashSet("test");
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"), allowedClusters);
        admin.tenants().updateTenant("prop-xyz", tenantInfo);

        assertEquals(admin.tenants().getTenants(), Lists.newArrayList("prop-xyz"));

        assertEquals(admin.tenants().getTenantInfo("prop-xyz"), tenantInfo);

        TenantInfoImpl newTenantAdmin = new TenantInfoImpl(Sets.newHashSet("role3", "role4"), allowedClusters);
        admin.tenants().updateTenant("prop-xyz", newTenantAdmin);

        assertEquals(admin.tenants().getTenantInfo("prop-xyz"), newTenantAdmin);

        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        admin.tenants().deleteTenant("prop-xyz");
        assertEquals(admin.tenants().getTenants(), Lists.newArrayList());

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
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet("test", "usw"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);

        assertEquals(admin.namespaces().getPolicies("prop-xyz/ns1").bundles, PoliciesUtil.defaultBundle());

        admin.namespaces().createNamespace("prop-xyz/ns2", Sets.newHashSet("test"));

        admin.namespaces().createNamespace("prop-xyz/ns3", 4);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns3", Sets.newHashSet("test"));
        assertEquals(admin.namespaces().getPolicies("prop-xyz/ns3").bundles.getNumBundles(), 4);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/ns3").bundles.getBoundaries().size(), 5);

        admin.namespaces().deleteNamespace("prop-xyz/ns3");

        try {
            admin.namespaces().createNamespace("non-existing/ns1");
            fail("Should not have passed");
        } catch (NotFoundException e) {
            // Ok
        }

        assertEquals(admin.namespaces().getNamespaces("prop-xyz"), Lists.newArrayList("prop-xyz/ns1", "prop-xyz/ns2"));
        assertEquals(admin.namespaces().getNamespaces("prop-xyz"), Lists.newArrayList("prop-xyz/ns1", "prop-xyz/ns2"));

        try {
            admin.namespaces().createNamespace("prop-xyz/ns4", Sets.newHashSet("usc"));
            fail("Should not have passed");
        } catch (NotAuthorizedException e) {
            // Ok, got the non authorized exception since usc cluster is not in the allowed clusters list.
        }

        // test with url style role.
        admin.namespaces().grantPermissionOnNamespace("prop-xyz/ns1",
                "spiffe://developer/passport-role", EnumSet.allOf(AuthAction.class));
        admin.namespaces().grantPermissionOnNamespace("prop-xyz/ns1", "my-role", EnumSet.allOf(AuthAction.class));

        Policies policies = new Policies();
        policies.replication_clusters = Sets.newHashSet("test");
        policies.bundles = PoliciesUtil.defaultBundle();
        policies.auth_policies.getNamespaceAuthentication().put("spiffe://developer/passport-role", EnumSet.allOf(AuthAction.class));
        policies.auth_policies.getNamespaceAuthentication().put("my-role", EnumSet.allOf(AuthAction.class));

        assertEquals(admin.namespaces().getPolicies("prop-xyz/ns1"), policies);
        assertEquals(admin.namespaces().getPermissions("prop-xyz/ns1"), policies.auth_policies.getNamespaceAuthentication());

        assertEquals(admin.namespaces().getTopics("prop-xyz/ns1"), Lists.newArrayList());

        admin.namespaces().revokePermissionsOnNamespace("prop-xyz/ns1", "spiffe://developer/passport-role");
        admin.namespaces().revokePermissionsOnNamespace("prop-xyz/ns1", "my-role");
        policies.auth_policies.getNamespaceAuthentication().remove("spiffe://developer/passport-role");
        policies.auth_policies.getNamespaceAuthentication().remove("my-role");
        assertEquals(admin.namespaces().getPolicies("prop-xyz/ns1"), policies);

        assertEquals(admin.namespaces().getPersistence("prop-xyz/ns1"), null);
        admin.namespaces().setPersistence("prop-xyz/ns1", new PersistencePolicies(3, 2, 1, 10.0));
        assertEquals(admin.namespaces().getPersistence("prop-xyz/ns1"), new PersistencePolicies(3, 2, 1, 10.0));

        // Force topic creation and namespace being loaded
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic("persistent://prop-xyz/ns1/my-topic")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.close();
        admin.topics().delete("persistent://prop-xyz/ns1/my-topic");

        admin.namespaces().unloadNamespaceBundle("prop-xyz/ns1", "0x00000000_0xffffffff");
        NamespaceName ns = NamespaceName.get("prop-xyz/ns1");
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

        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        assertEquals(admin.namespaces().getNamespaces("prop-xyz"), Lists.newArrayList("prop-xyz/ns2"));

        try {
            admin.namespaces().unload("prop-xyz/ns1");
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
    }

    @Test(dataProvider = "topicName")
    public void persistentTopics(String topicName) throws Exception {
        final String subName = topicName;
        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());

        final String persistentTopicName = "persistent://prop-xyz/ns1/" + topicName;
        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/" + topicName, 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1"),
                Lists.newArrayList("persistent://prop-xyz/ns1/" + topicName));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(persistentTopicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(persistentTopicName), Lists.newArrayList(subName));

        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/" + topicName, 10);

        TopicStats topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet(Lists.newArrayList(subName)));
        assertEquals(topicStats.getSubscriptions().get(subName).getConsumers().size(), 1);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);
        assertEquals(topicStats.getPublishers().size(), 0);

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(persistentTopicName, false);
        assertEquals(internalStats.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList(Codec.encode(subName))));

        List<Message<byte[]>> messages = admin.topics().peekMessages(persistentTopicName, subName, 3);
        assertEquals(messages.size(), 3);
        for (int i = 0; i < 3; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        messages = admin.topics().peekMessages(persistentTopicName, subName, 15);
        assertEquals(messages.size(), 10);
        for (int i = 0; i < 10; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        admin.topics().skipMessages(persistentTopicName, subName, 5);
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 5);

        admin.topics().skipAllMessages(persistentTopicName, subName);
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 0);

        publishNullValueMessageOnPersistentTopic(persistentTopicName, 10);
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().get(subName).getMsgBacklog(), 10);
        messages = admin.topics().peekMessages(persistentTopicName, subName, 10);
        assertEquals(messages.size(), 10);
        for (int i = 0; i < 10; i++) {
            assertNull(messages.get(i).getData());
            assertNull(messages.get(i).getValue());
        }
        admin.topics().skipAllMessages(persistentTopicName, subName);

        consumer.close();
        client.close();

        admin.topics().deleteSubscription(persistentTopicName, subName);

        assertEquals(admin.topics().getSubscriptions(persistentTopicName), Lists.newArrayList());
        topicStats = admin.topics().getStats(persistentTopicName);
        assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet());
        assertEquals(topicStats.getPublishers().size(), 0);

        try {
            admin.topics().skipAllMessages(persistentTopicName, subName);
        } catch (NotFoundException e) {
        }

        admin.topics().delete(persistentTopicName);

        try {
            admin.topics().delete(persistentTopicName);
            fail("Should have received 404");
        } catch (NotFoundException e) {
        }

        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());
    }

    @Test(dataProvider = "topicNamesForAllTypes")
    public void partitionedTopics(String topicType, String topicName) throws Exception {
        final String namespace = "prop-xyz/ns1";
        final String partitionedTopicName = topicType + "://" + namespace + "/" + topicName;
        final String anotherTopic = topicType + "://" + namespace + "/ds2";
        // TODO: there're some gaps between non-persistent topics and persistent topics, so some checks will be skipped
        //   for non-persistent topics. After the gaps were filled, we can remove this check.
        final boolean isPersistent = topicType.equals(TopicDomain.persistent.value());

        assertEquals(admin.topics().getPartitionedTopicList(namespace), Lists.newArrayList());

        try {
            admin.topics().getPartitionedTopicMetadata(partitionedTopicName);
            fail("getPartitionedTopicMetadata of " + partitionedTopicName + " should not succeed");
        } catch (NotFoundException expected) {
        }

        admin.topics().createPartitionedTopic(partitionedTopicName, 4);
        assertEquals(admin.topics().getPartitionedTopicList(namespace),
                Lists.newArrayList(partitionedTopicName));

        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 4);

        List<String> topics;
        if (isPersistent) {
            // TODO: for non-persistent topics getList will return 0
            topics = admin.topics().getList(namespace);
            assertEquals(topics.size(), 4);
        }

        try {
            admin.topics().getPartitionedTopicMetadata(anotherTopic);
            fail("getPartitionedTopicMetadata of " + anotherTopic + " should not succeed");
        } catch (NotFoundException expected) {
        }

        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats(partitionedTopicName,false);

                // check the getPartitionedStats for PartitionedTopic returns only partitions metadata, and no partitions info
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions,
                topicStats.getMetadata().partitions);

        assertEquals(topicStats.getPartitions().size(), 0);

        List<String> subscriptions = admin.topics().getSubscriptions(partitionedTopicName);
        assertEquals(subscriptions.size(), 0);

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(partitionedTopicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(partitionedTopicName), Lists.newArrayList("my-sub"));

        try {
            if (isPersistent) {
                // TODO: for non-persistent topics, deleteSubscription might throw NotFoundException
                admin.topics().deleteSubscription(partitionedTopicName, "my-sub");
                // TODO: for non-persistent topics, deleteSubscription won't fail
                fail("should have failed");
            }
        } catch (PreconditionFailedException e) {
            // ok
        } catch (Exception e) {
            fail(e.getMessage());
        }

        Consumer<byte[]> consumer1 = client.newConsumer().topic(partitionedTopicName).subscriptionName("my-sub-1")
                .subscribe();

        if (isPersistent) {
            // TODO: for non-persistent topics, getSubscriptions will return a empty set
            assertEquals(Sets.newHashSet(admin.topics().getSubscriptions(partitionedTopicName)),
                    Sets.newHashSet("my-sub", "my-sub-1"));
        }

        consumer1.close();
        if (isPersistent) {
            // TODO: for non-persistent topics, deleteSubscription might throw NotFoundException
            admin.topics().deleteSubscription(partitionedTopicName, "my-sub-1");
            // TODO: for non-persistent topics, getSubscriptions will return a empty set
            assertEquals(admin.topics().getSubscriptions(partitionedTopicName), Lists.newArrayList("my-sub"));
        }

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic(partitionedTopicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();

        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        assertEquals(Sets.newHashSet(admin.topics().getList(namespace)),
                Sets.newHashSet(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));

        // test cumulative stats for partitioned topic
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName,false);
        if (isPersistent) {
            // TODO: for non-persistent topics, the subscription doesn't exist
            assertEquals(topicStats.getSubscriptions().keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
            assertEquals(topicStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
            assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 10);
        }
        assertEquals(topicStats.getPublishers().size(), 1);
        assertEquals(topicStats.getPartitions(), Maps.newHashMap());

        // test per partition stats for partitioned topic
        topicStats = admin.topics().getPartitionedStats(partitionedTopicName,true);
        assertEquals(topicStats.getMetadata().partitions, 4);
        assertEquals(topicStats.getPartitions().keySet(),
                Sets.newHashSet(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));
        TopicStats partitionStats = topicStats.getPartitions().get(partitionedTopicName + "-partition-0");
        assertEquals(partitionStats.getPublishers().size(), 1);
        if (isPersistent) {
            // TODO: for non-persistent topics, the subscription doesn't exist
            assertEquals(partitionStats.getSubscriptions().get("my-sub").getConsumers().size(), 1);
            assertEquals(partitionStats.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);
        }

        try {
            admin.topics().skipMessages(partitionedTopicName, "my-sub", 5);
            fail("skip messages for partitioned topics should fail");
        } catch (Exception e) {
            // ok
        }

        if (isPersistent) {
            // TODO: for non-persistent topics, skilAllMessages will cause 500 internal error
            admin.topics().skipAllMessages(partitionedTopicName, "my-sub");
            topicStats = admin.topics().getPartitionedStats(partitionedTopicName, false);
            assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 0);
        }

        producer.close();
        consumer.close();

        if (isPersistent) {
            // TODO: for non-persistent topics, deleteSubscription might throw NotFoundException
            admin.topics().deleteSubscription(partitionedTopicName, "my-sub");
            assertEquals(admin.topics().getSubscriptions(partitionedTopicName), Lists.newArrayList());
        }

        try {
            admin.topics().createPartitionedTopic(partitionedTopicName, 32);
            fail("Should have failed as the partitioned topic already exists");
        } catch (ConflictException ignore) {
        }

        producer = client.newProducer(Schema.BYTES)
                .topic(partitionedTopicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        if (isPersistent) {
            // TODO: for non-persistent topics getList will return 0
            topics = admin.topics().getList(namespace);
            assertEquals(topics.size(), 4);
        }

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
            admin.topics().deletePartitionedTopic(anotherTopic);
            fail("Should have failed as the partitioned topic was not created");
        } catch (NotFoundException nfe) {
        }

        admin.topics().deletePartitionedTopic(partitionedTopicName);

        // delete a partitioned topic in a global namespace
        admin.topics().createPartitionedTopic(partitionedTopicName, 4);
        admin.topics().deletePartitionedTopic(partitionedTopicName);
    }

    @Test
    public void testGetPartitionedInternalInfo() throws Exception {
        String partitionedTopic = "my-topic" + UUID.randomUUID().toString();
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/ns1"), Lists.newArrayList());
        final String partitionedTopicName = "persistent://prop-xyz/ns1/" + partitionedTopic;
        admin.topics().createPartitionedTopic(partitionedTopicName, 2);
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/ns1"), Lists.newArrayList(partitionedTopicName));
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 2);

        String partitionTopic0 = partitionedTopicName + "-partition-0";
        String partitionTopic1 = partitionedTopicName + "-partition-1";

        String partitionTopic0InfoResponse = admin.topics().getInternalInfo(partitionTopic0);
        String partitionTopic1InfoResponse = admin.topics().getInternalInfo(partitionTopic1);

        // expected managed info
        PartitionedManagedLedgerInfo partitionedManagedLedgerInfo = new PartitionedManagedLedgerInfo();
        partitionedManagedLedgerInfo.version = 0L;
        partitionedManagedLedgerInfo.partitions.put(partitionTopic0,
            ObjectMapperFactory.getThreadLocal().readValue(partitionTopic0InfoResponse, ManagedLedgerInfo.class));
        partitionedManagedLedgerInfo.partitions.put(partitionTopic1,
            ObjectMapperFactory.getThreadLocal().readValue(partitionTopic1InfoResponse, ManagedLedgerInfo.class));

        String expectedResult = ObjectMapperFactory.getThreadLocal().writeValueAsString(partitionedManagedLedgerInfo);

        String partitionTopicInfoResponse = admin.topics().getInternalInfo(partitionedTopicName);
        assertEquals(partitionTopicInfoResponse, expectedResult);
    }

    @Test
    public void testGetPartitionedStatsInternal() throws Exception {
        String partitionedTopic = "my-topic" + UUID.randomUUID().toString();
        String subName = "my-sub";
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/ns1"), Lists.newArrayList());
        final String partitionedTopicName = "persistent://prop-xyz/ns1/" + partitionedTopic;
        admin.topics().createPartitionedTopic(partitionedTopicName, 2);
        assertEquals(admin.topics().getPartitionedTopicList("prop-xyz/ns1"), Lists.newArrayList(partitionedTopicName));
        assertEquals(admin.topics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 2);

        // create consumer and subscription
        pulsarClient.newConsumer().topic(partitionedTopicName).subscriptionName(subName).subscribe();

        // publish several messages
        publishMessagesOnPersistentTopic(partitionedTopicName, 10);

        String partitionTopic0 = partitionedTopicName + "-partition-0";
        String partitionTopic1 = partitionedTopicName + "-partition-1";

        Thread.sleep(1000);

        PersistentTopicInternalStats internalStats0 = admin.topics().getInternalStats(partitionTopic0, false);
        assertEquals(internalStats0.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList(Codec.encode(subName))));

        PersistentTopicInternalStats internalStats1 = admin.topics().getInternalStats(partitionTopic1, false);
        assertEquals(internalStats1.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList(Codec.encode(subName))));

        // expected internal stats
        PartitionedTopicMetadata partitionedTopicMetadata = new PartitionedTopicMetadata(2);
        PartitionedTopicInternalStats expectedInternalStats = new PartitionedTopicInternalStats(partitionedTopicMetadata);
        expectedInternalStats.partitions.put(partitionTopic0, internalStats0);
        expectedInternalStats.partitions.put(partitionTopic1, internalStats1);

        // partitioned internal stats
        PartitionedTopicInternalStats partitionedInternalStats = admin.topics().getPartitionedInternalStats(partitionedTopicName);

        String expectedResult = ObjectMapperFactory.getThreadLocal().writeValueAsString(expectedInternalStats);
        String result = ObjectMapperFactory.getThreadLocal().writeValueAsString(partitionedInternalStats);

        assertEquals(result, expectedResult);
    }

    @Test(dataProvider = "numBundles")
    public void testDeleteNamespaceBundle(Integer numBundles) throws Exception {
        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        admin.namespaces().createNamespace("prop-xyz/ns1-bundles", numBundles);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns1-bundles", Sets.newHashSet("test"));

        // since we have 2 brokers running, we try to let both of them acquire bundle ownership
        admin.lookups().lookupTopic("persistent://prop-xyz/ns1-bundles/ds1");
        admin.lookups().lookupTopic("persistent://prop-xyz/ns1-bundles/ds2");
        admin.lookups().lookupTopic("persistent://prop-xyz/ns1-bundles/ds3");
        admin.lookups().lookupTopic("persistent://prop-xyz/ns1-bundles/ds4");

        assertEquals(admin.namespaces().getTopics("prop-xyz/ns1-bundles"), Lists.newArrayList());

        admin.namespaces().deleteNamespace("prop-xyz/ns1-bundles");
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "test"), Lists.newArrayList());
    }

    @Test
    public void testDeleteTenantForcefully() throws Exception {
        // allow forced deletion of tenants
        pulsar.getConfiguration().setForceDeleteTenantAllowed(true);

        String tenant = "my-tenant";
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test")));

        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace
        String namespace = tenant + "/my-ns";
        admin.namespaces().createNamespace("my-tenant/my-ns", Sets.newHashSet("test"));

        assertEquals(admin.namespaces().getNamespaces(tenant), Lists.newArrayList("my-tenant/my-ns"));

        // create topic
        String topic = namespace + "/my-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        assertFalse(admin.topics().getList(namespace).isEmpty());

        try {
            admin.tenants().deleteTenant(tenant, false);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete non-empty tenant
        }

        // allow forced deletion of namespaces
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        // delete tenant forcefully
        admin.tenants().deleteTenant(tenant, true);
        Awaitility.await().untilAsserted(() -> {
                    assertFalse(admin.tenants().getTenants().contains(tenant));
                });

        final String managedLedgerPathForTenant = "/managed-ledgers/" + tenant;
        assertFalse(pulsar.getLocalMetadataStore().exists(managedLedgerPathForTenant).join());

        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test")));
        assertTrue(admin.tenants().getTenants().contains(tenant));
        assertTrue(admin.namespaces().getNamespaces(tenant).isEmpty());

        // reset back to false
        pulsar.getConfiguration().setForceDeleteTenantAllowed(false);
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);

    }

    @Test
    public void testDeleteNamespaceForcefully() throws Exception {
        // allow forced deletion of namespaces
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(true);

        String tenant = "my-tenant";
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test")));

        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace
        String namespace = tenant + "/my-ns";
        admin.namespaces().createNamespace("my-tenant/my-ns", Sets.newHashSet("test"));

        assertEquals(admin.namespaces().getNamespaces(tenant), Lists.newArrayList("my-tenant/my-ns"));

        // create topic
        String topic = namespace + "/my-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        assertFalse(admin.topics().getList(namespace).isEmpty());

        try {
            admin.namespaces().deleteNamespace(namespace, false);
            fail("should have failed due to namespace not empty");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete non-empty tenant
        }

        // delete namespace forcefully
        admin.namespaces().deleteNamespace(namespace, true);
        assertFalse(admin.namespaces().getNamespaces(tenant).contains(namespace));
        assertTrue(admin.namespaces().getNamespaces(tenant).isEmpty());

        final String managedLedgerPath = "/managed-ledgers/" + namespace;
        final String persistentDomain = managedLedgerPath + "/" + TopicDomain.persistent.value();
        final String nonPersistentDomain = managedLedgerPath + "/" + TopicDomain.non_persistent.value();

        assertFalse(pulsar.getLocalMetadataStore().exists(managedLedgerPath).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(persistentDomain).join());
        assertFalse(pulsar.getLocalMetadataStore().exists(nonPersistentDomain).join());

        // reset back to false
        pulsar.getConfiguration().setForceDeleteNamespaceAllowed(false);
    }

    @Test
    public void testForceDeleteTenantNotAllowed() throws Exception {
        assertFalse(pulsar.getConfiguration().isForceDeleteTenantAllowed());

        String tenant = "my-tenant";
        assertFalse(admin.tenants().getTenants().contains(tenant));

        // create tenant
        admin.tenants().createTenant(tenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet("test")));

        assertTrue(admin.tenants().getTenants().contains(tenant));

        // create namespace
        String namespace = tenant + "/my-ns";
        admin.namespaces().createNamespace("my-tenant/my-ns", Sets.newHashSet("test"));

        assertEquals(admin.namespaces().getNamespaces(tenant), Lists.newArrayList("my-tenant/my-ns"));

        // create topic
        String topic = namespace + "/my-topic";
        admin.topics().createPartitionedTopic(topic, 10);

        assertFalse(admin.topics().getList(namespace).isEmpty());

        try {
            admin.tenants().deleteTenant(tenant, false);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete non-empty tenant
        }

        try {
            admin.tenants().deleteTenant(tenant, true);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            // Expected: cannot delete due to broker is not allowed
        }

        assertTrue(admin.tenants().getTenants().contains(tenant));
    }

    @Test
    public void testNamespaceSplitBundle() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/ns1";
        final String topicName = (new StringBuilder("persistent://")).append(namespace).append("/ds2").toString();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), Lists.newArrayList(topicName));

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
    public void testNamespaceSplitBundleWithTopicCountEquallyDivideAlgorithm() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/ns1";
        List<String> topicNames = Lists.newArrayList(
                (new StringBuilder("persistent://")).append(namespace).append("/topicCountEquallyDivideAlgorithum-1").toString(),
                (new StringBuilder("persistent://")).append(namespace).append("/topicCountEquallyDivideAlgorithum-2").toString());

        List<Producer<byte[]>> producers = new ArrayList<>(2);
        for (String topicName : topicNames) {
            Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                    .topic(topicName)
                    .enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create();
            producers.add(producer);
            producer.send("message".getBytes());
        }

        assertTrue(admin.topics().getList(namespace).containsAll(topicNames));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true,
                    NamespaceBundleSplitAlgorithm.TOPIC_COUNT_EQUALLY_DIVIDE);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }
        NamespaceBundles bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        NamespaceBundle bundle1 = pulsar.getNamespaceService().getBundle(TopicName.get(topicNames.get(0)));
        NamespaceBundle bundle2 = pulsar.getNamespaceService().getBundle(TopicName.get(topicNames.get(1)));
        assertNotEquals(bundle1, bundle2);
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertNotEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }
        producers.forEach(Producer::closeAsync);
    }

    @Test
    public void testNamespaceSplitBundleWithInvalidAlgorithm() {
        // Force to create a topic
        final String namespace = "prop-xyz/ns1";
        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true,
                    "invalid_test");
            fail("unsupported namespace bundle split algorithm");
        } catch (PulsarAdminException ignored) {
        }
    }

    @Test
    public void testNamespaceSplitBundleWithDefaultTopicCountEquallyDivideAlgorithm() throws Exception {
        conf.setDefaultNamespaceBundleSplitAlgorithm(NamespaceBundleSplitAlgorithm.TOPIC_COUNT_EQUALLY_DIVIDE);
        // Force to create a topic
        final String namespace = "prop-xyz/ns1";
        List<String> topicNames = Lists.newArrayList(
                (new StringBuilder("persistent://")).append(namespace).append("/topicCountEquallyDivideAlgorithum-1").toString(),
                (new StringBuilder("persistent://")).append(namespace).append("/topicCountEquallyDivideAlgorithum-2").toString());

        List<Producer<byte[]>> producers = new ArrayList<>(2);
        for (String topicName : topicNames) {
            Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                    .topic(topicName)
                    .enableBatching(false)
                    .messageRoutingMode(MessageRoutingMode.SinglePartition)
                    .create();
            producers.add(producer);
            producer.send("message".getBytes());
        }

        assertTrue(admin.topics().getList(namespace).containsAll(topicNames));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", true, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }
        NamespaceBundles bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        NamespaceBundle bundle1 = pulsar.getNamespaceService().getBundle(TopicName.get(topicNames.get(0)));
        NamespaceBundle bundle2 = pulsar.getNamespaceService().getBundle(TopicName.get(topicNames.get(1)));
        assertNotEquals(bundle1, bundle2);
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertNotEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }
        producers.forEach(Producer::closeAsync);
        conf.setDefaultNamespaceBundleSplitAlgorithm(NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_NAME);
    }

    @Test
    public void testNamespaceSplitBundleConcurrent() throws Exception {
        // Force to create a topic
        final String namespace = "prop-xyz/ns1";
        final String topicName = (new StringBuilder("persistent://")).append(namespace).append("/ds2").toString();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList(namespace), Lists.newArrayList(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff", false, null);
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        @Cleanup("shutdownNow")
        ExecutorService executorService = Executors.newCachedThreadPool();

        try {
            List<Future<Void>> futures = executorService.invokeAll(Arrays.asList(() -> {
                log.info("split 2 bundles at the same time. spilt: 0x00000000_0x7fffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x7fffffff", false, null);
                return null;
            }, () -> {
                log.info("split 2 bundles at the same time. spilt: 0x7fffffff_0xffffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xffffffff", false, null);
                return null;
            }));

            for (Future<?> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange4 = { namespace + "/0x00000000_0x3fffffff", namespace + "/0x3fffffff_0x7fffffff",
                namespace + "/0x7fffffff_0xbfffffff", namespace + "/0xbfffffff_0xffffffff" };
        bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 4);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange4[i]);
        }

        try {
            List<Future<Void>> futures = executorService.invokeAll(Arrays.asList(() -> {
                log.info("split 4 bundles at the same time. spilt: 0x00000000_0x3fffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0x3fffffff", false, null);
                return null;
            }, () -> {
                log.info("split 4 bundles at the same time. spilt: 0x3fffffff_0x7fffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0x3fffffff_0x7fffffff", false, null);
                return null;
            }, () -> {
                log.info("split 4 bundles at the same time. spilt: 0x7fffffff_0xbfffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0x7fffffff_0xbfffffff", false, null);
                return null;
            }, () -> {
                log.info("split 4 bundles at the same time. spilt: 0xbfffffff_0xffffffff ");
                admin.namespaces().splitNamespaceBundle(namespace, "0xbfffffff_0xffffffff", false, null);
                return null;
            }));

            for (Future<?> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        String[] splitRange8 = { namespace + "/0x00000000_0x1fffffff", namespace + "/0x1fffffff_0x3fffffff",
                namespace + "/0x3fffffff_0x5fffffff", namespace + "/0x5fffffff_0x7fffffff",
                namespace + "/0x7fffffff_0x9fffffff", namespace + "/0x9fffffff_0xbfffffff",
                namespace + "/0xbfffffff_0xdfffffff", namespace + "/0xdfffffff_0xffffffff" };
        bundles = bundleFactory.getBundles(NamespaceName.get(namespace));
        assertEquals(bundles.getBundles().size(), 8);
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange8[i]);
        }

        producer.close();
    }

    @Test
    public void testNamespaceUnloadBundle() throws Exception {
        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());

        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1"),
                Lists.newArrayList("persistent://prop-xyz/ns1/ds2"));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1/ds2")
                .subscriptionName("my-sub").subscribe();
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1/ds2"),
                Lists.newArrayList("my-sub"));

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic("persistent://prop-xyz/ns1/ds2")
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
            admin.namespaces().unloadNamespaceBundle("prop-xyz/ns1", "0x00000000_0xffffffff");
        } catch (Exception e) {
            fail("Unload shouldn't have throw exception");
        }

        // check that no one owns the namespace
        NamespaceBundle bundle = bundleFactory.getBundle(NamespaceName.get("prop-xyz/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        assertFalse(pulsar.getNamespaceService().isServiceUnitOwned(bundle));
        assertFalse(otherPulsar.getNamespaceService().isServiceUnitOwned(bundle));
        pulsarClient.shutdown();

        LOG.info("--- RELOAD ---");

        // Force reload of namespace and wait for topic to be ready
        Awaitility.await().timeout(30, TimeUnit.SECONDS).ignoreExceptionsInstanceOf(PulsarAdminException.class)
                .until(() -> admin.topics().getStats("persistent://prop-xyz/ns1/ds2") != null);

        admin.topics().deleteSubscription("persistent://prop-xyz/ns1/ds2", "my-sub");
        admin.topics().delete("persistent://prop-xyz/ns1/ds2");
    }

    @Test(dataProvider = "numBundles")
    public void testNamespaceBundleUnload(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/ns1-bundles", numBundles);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns1-bundles", Sets.newHashSet("test"));

        assertEquals(admin.topics().getList("prop-xyz/ns1-bundles"), Lists.newArrayList());

        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1-bundles/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1-bundles"),
                Lists.newArrayList("persistent://prop-xyz/ns1-bundles/ds2"));

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2")
                .subscriptionName("my-sub").subscribe();
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub"));

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic("persistent://prop-xyz/ns1-bundles/ds2")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        NamespaceBundle bundle = pulsar.getNamespaceService()
                .getBundle(TopicName.get("persistent://prop-xyz/ns1-bundles/ds2"));

        consumer.close();
        producer.close();

        admin.namespaces().unloadNamespaceBundle("prop-xyz/ns1-bundles", bundle.getBundleRange());

        // check that no one owns the namespace bundle
        assertFalse(pulsar.getNamespaceService().isServiceUnitOwned(bundle));
        assertFalse(otherPulsar.getNamespaceService().isServiceUnitOwned(bundle));

        LOG.info("--- RELOAD ---");

        // Force reload of namespace and wait for topic to be ready
        Awaitility.await().timeout(30, TimeUnit.SECONDS).ignoreExceptionsInstanceOf(PulsarAdminException.class)
                .until(() -> admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds2") != null);

        admin.topics().deleteSubscription("persistent://prop-xyz/ns1-bundles/ds2", "my-sub");
        admin.topics().delete("persistent://prop-xyz/ns1-bundles/ds2");
    }

    @Test
    public void testDeleteSubscription() throws Exception {
        final String subName = "test-sub";
        final String persistentTopicName = "persistent://prop-xyz/ns1/test-sub-topic";

        // disable auto subscription creation
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(false);

        // create a topic and produce some messages
        publishMessagesOnPersistentTopic(persistentTopicName, 5);
        assertEquals(admin.topics().getList("prop-xyz/ns1"),
                Lists.newArrayList(persistentTopicName));

        // create the subscription by PulsarAdmin
        admin.topics().createSubscription(persistentTopicName, subName, MessageId.earliest);

        assertEquals(admin.topics().getSubscriptions(persistentTopicName), Lists.newArrayList(subName));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(persistentTopicName).subscriptionName(subName)
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        // try to delete the subscription with a connected consumer
        try {
            admin.topics().deleteSubscription(persistentTopicName, subName);
            fail("should have failed");
        } catch (PreconditionFailedException e) {
            assertEquals(e.getStatusCode(), Status.PRECONDITION_FAILED.getStatusCode());
        }

        // failed to delete the subscription
        assertEquals(admin.topics().getSubscriptions(persistentTopicName), Lists.newArrayList(subName));

        // try to delete the subscription with a connected consumer forcefully
        admin.topics().deleteSubscription(persistentTopicName, subName, true);

        // delete the subscription successfully
        assertEquals(admin.topics().getSubscriptions(persistentTopicName).size(), 0);

        // reset to default
        pulsar.getConfiguration().setAllowAutoSubscriptionCreation(true);

        client.close();
    }

    @Test(dataProvider = "bundling")
    public void testClearBacklogOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/ns1-bundles", numBundles);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns1-bundles", Sets.newHashSet("test"));

        // create consumer and subscription
        pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2").subscriptionName("my-sub")
                .subscribe();
        pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2").subscriptionName("my-sub-1")
                .subscribe();
        pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2").subscriptionName("my-sub-2")
                .subscribe();
        pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds1").subscriptionName("my-sub")
                .subscribe();
        pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds1").subscriptionName("my-sub-1")
                .subscribe();

        // Create producer
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic("persistent://prop-xyz/ns1-bundles/ds2")
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
                .topic("persistent://prop-xyz/ns1-bundles/ds1")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer1.send(message.getBytes());
        }

        producer1.close();

        admin.namespaces().clearNamespaceBacklogForSubscription("prop-xyz/ns1-bundles", "my-sub");

        long backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 10);

        admin.namespaces().clearNamespaceBacklog("prop-xyz/ns1-bundles");

        backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds1").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub-1").getMsgBacklog();
        assertEquals(backlog, 0);
        backlog = admin.topics().getStats("persistent://prop-xyz/ns1-bundles/ds2").getSubscriptions()
                .get("my-sub-2").getMsgBacklog();
        assertEquals(backlog, 0);
    }

    @Test(dataProvider = "bundling")
    public void testUnsubscribeOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/ns1-bundles", numBundles);
        admin.namespaces().setNamespaceReplicationClusters("prop-xyz/ns1-bundles", Sets.newHashSet("test"));

        // create consumer and subscription
        Consumer<byte[]> consumer1 = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2")
                .subscriptionName("my-sub").subscribe();
        Consumer<byte[]> consumer2 = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2")
                .subscriptionName("my-sub-1").subscribe();
        /* Consumer consumer3 = */ pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds2")
                .subscriptionName("my-sub-2").subscribe();
        Consumer<byte[]> consumer4 = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds1")
                .subscriptionName("my-sub").subscribe();
        Consumer<byte[]> consumer5 = pulsarClient.newConsumer().topic("persistent://prop-xyz/ns1-bundles/ds1")
                .subscriptionName("my-sub-1").subscribe();

        try {
            admin.namespaces().unsubscribeNamespace("prop-xyz/ns1-bundles", "my-sub");
            fail("should have failed");
        } catch (PreconditionFailedException e) {
            // ok
        }

        consumer1.close();

        try {
            admin.namespaces().unsubscribeNamespace("prop-xyz/ns1-bundles", "my-sub");
            fail("should have failed");
        } catch (PreconditionFailedException e) {
            // ok
        }

        consumer4.close();

        admin.namespaces().unsubscribeNamespace("prop-xyz/ns1-bundles", "my-sub");

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub-1", "my-sub-2"));
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1-bundles/ds1"),
                Lists.newArrayList("my-sub-1"));

        consumer2.close();
        consumer5.close();

        admin.namespaces().unsubscribeNamespace("prop-xyz/ns1-bundles", "my-sub-1");

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub-2"));
        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1-bundles/ds1"),
                Lists.newArrayList());
    }

    private List<MessageId> publishMessagesOnPersistentTopic(String topicName, int messages) throws Exception {
        return publishMessagesOnPersistentTopic(topicName, messages, 0, false);
    }

    private List<MessageId> publishNullValueMessageOnPersistentTopic(String topicName, int messages) throws Exception {
        return publishMessagesOnPersistentTopic(topicName, messages, 0, true);
    }

    private List<MessageId> publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx,
                                                             boolean nullValue) throws Exception {
        List<MessageId> messageIds = new ArrayList<>();
        Producer<byte[]> producer = pulsarClient.newProducer(Schema.BYTES)
                .topic(topicName)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        for (int i = startIdx; i < (messages + startIdx); i++) {
            if (nullValue) {
                messageIds.add(producer.send(null));
            } else {
                String message = "message-" + i;
                messageIds.add(producer.send(message.getBytes()));
            }
        }

        producer.close();
        return messageIds;
    }

    @Test
    public void backlogQuotas() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop-xyz/ns1"),
                Maps.newHashMap());

        Map<BacklogQuotaType, BacklogQuota> quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/ns1");
        assertEquals(quotaMap.size(), 0);
        assertNull(quotaMap.get(BacklogQuotaType.destination_storage));

        admin.namespaces().setBacklogQuota("prop-xyz/ns1",
                BacklogQuota.builder()
                        .limitSize(1 * 1024 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build());
        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/ns1");
        assertEquals(quotaMap.size(), 1);
        assertEquals(quotaMap.get(BacklogQuotaType.destination_storage),
                BacklogQuota.builder()
                        .limitSize(1 * 1024 * 1024)
                        .retentionPolicy(RetentionPolicy.producer_exception)
                        .build());

        admin.namespaces().removeBacklogQuota("prop-xyz/ns1");

        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/ns1");
        assertEquals(quotaMap.size(), 0);
        assertNull(quotaMap.get(BacklogQuotaType.destination_storage));
    }

    @Test
    public void statsOnNonExistingTopics() throws Exception {
        try {
            admin.topics().getStats("persistent://prop-xyz/ns1/ghostTopic");
            fail("The topic doesn't exist");
        } catch (NotFoundException e) {
            // OK
        }
    }

    @Test
    public void testDeleteFailedReturnCode() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/my-topic";
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

    private static class IncompatibleTenantAdmin {
        public Set<String> allowedClusters;
        public int someNewIntField;
        public String someNewString;
    }

    @Test
    public void testJacksonWithTypeDifferences() throws Exception {
        String expectedJson = "{\"adminRoles\":[\"role1\",\"role2\"],\"allowedClusters\":[\"usw\",\"test\"]}";
        IncompatibleTenantAdmin r1 = ObjectMapperFactory.getThreadLocal().readerFor(IncompatibleTenantAdmin.class)
                .readValue(expectedJson);
        assertEquals(r1.allowedClusters, Sets.newHashSet("test", "usw"));
        assertEquals(r1.someNewIntField, 0);
        assertNull(r1.someNewString);
    }

    @Test
    public void testBackwardCompatibility() throws Exception {
        assertEquals(admin.tenants().getTenants(), Lists.newArrayList("prop-xyz"));
        assertEquals(admin.tenants().getTenantInfo("prop-xyz").getAdminRoles(),
                Lists.newArrayList("role1", "role2"));
        assertEquals(admin.tenants().getTenantInfo("prop-xyz").getAllowedClusters(), Sets.newHashSet("test"));

        // Try to deserialize property JSON with IncompatibleTenantAdmin format
        // it should succeed ignoring missing fields
        TenantsImpl properties = (TenantsImpl) admin.tenants();
        IncompatibleTenantAdmin result = properties.request(properties.getWebTarget().path("prop-xyz"))
                .get(IncompatibleTenantAdmin.class);

        assertEquals(result.allowedClusters, Sets.newHashSet("test"));
        assertEquals(result.someNewIntField, 0);
        assertNull(result.someNewString);

        admin.namespaces().deleteNamespace("prop-xyz/ns1");
        admin.tenants().deleteTenant("prop-xyz");
        assertEquals(admin.tenants().getTenants(), Lists.newArrayList());
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());

        topicName = "persistent://prop-xyz/ns1/" + topicName;

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0, false);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long messageTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 5, 5, false);

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

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList());
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorResetAfterReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));
        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());

        topicName = "persistent://prop-xyz/ns1/" + topicName;

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0, false);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long firstTimestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 3, 5, false);

        Thread.sleep(1);
        long secondTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 2, 8, false);

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

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList());
        admin.topics().delete(topicName);
    }

    @Test
    public void persistentTopicsCursorResetAndFailover() throws Exception {
        final String namespace = "prop-xyz/ns1";
        final String topicName = "persistent://" + namespace + "/reset-cursor-and-failover";
        final String subName = "sub1";

        admin.namespaces().setRetention(namespace, new RetentionPolicies(10, 10));

        // Create consumer and failover subscription
        Consumer<byte[]> consumerA = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName(subName).startMessageIdInclusive()
                .consumerName("consumerA").subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        publishMessagesOnPersistentTopic(topicName, 5, 0, false);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long messageTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 5, 5, false);

        // Currently the active consumer is consumerA
        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumerA.receive(5, TimeUnit.SECONDS);
            consumerA.acknowledge(message);
        }

        admin.topics().resetCursor(topicName, subName, messageTimestamp);

        // In v2.5 or later, the first connected consumer is active.
        // So consumerB connected later will not be active.
        // cf. https://github.com/apache/pulsar/pull/4604
        Thread.sleep(1000);
        Consumer<byte[]> consumerB = pulsarClient.newConsumer().topic(topicName).subscriptionName(subName)
                .consumerName("consumerB").subscriptionType(SubscriptionType.Failover)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        int receivedAfterReset = 0;
        for (int i = 5; i < 10; i++) {
            Message<byte[]> message = consumerA.receive(5, TimeUnit.SECONDS);
            consumerA.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(message.getData(), expected.getBytes());
        }
        assertEquals(receivedAfterReset, 5);

        // Closing consumerA activates consumerB
        consumerA.close();

        publishMessagesOnPersistentTopic(topicName, 5, 10, false);

        int receivedAfterFailover = 0;
        for (int i = 10; i < 15; i++) {
            Message<byte[]> message = consumerB.receive(5, TimeUnit.SECONDS);
            consumerB.acknowledge(message);
            ++receivedAfterFailover;
            String expected = "message-" + i;
            assertEquals(message.getData(), expected.getBytes());
        }
        assertEquals(receivedAfterFailover, 5);

        consumerB.close();
        admin.topics().deleteSubscription(topicName, subName);
        admin.topics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void partitionedTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));
        topicName = "persistent://prop-xyz/ns1/" + topicName;

        admin.topics().createPartitionedTopic(topicName, 4);

        // create consumer and subscription
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub").startMessageIdInclusive()
                .subscriptionType(SubscriptionType.Exclusive)
                .acknowledgmentGroupTime(0, TimeUnit.SECONDS).subscribe();

        List<String> topics = admin.topics().getList("prop-xyz/ns1");
        assertEquals(topics.size(), 4);

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0, false);
        Thread.sleep(1);

        long timestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 5, 5, false);

        for (int i = 0; i < 10; i++) {
            Message<byte[]> message = consumer.receive();
            consumer.acknowledge(message);
        }
        // messages should still be available due to retention

        admin.topics().resetCursor(topicName, "my-sub", timestamp);

        Set<String> expectedMessages = Sets.newHashSet();
        Set<String> receivedMessages = Sets.newHashSet();
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
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList());

        String topicName = "persistent://prop-xyz/ns1/invalidcursorreset";
        // Force to create a topic
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1"), Lists.newArrayList(topicName));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic(topicName).subscriptionName("my-sub")
                .subscriptionType(SubscriptionType.Exclusive).subscribe();

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

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

        assertEquals(admin.topics().getSubscriptions(topicName), Lists.newArrayList());
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
                .adminRoles(Sets.newHashSet("test_appid1", "test_appid2"))
                .allowedClusters(Sets.newHashSet("test"))
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
        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/ds2", 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1"),
                Lists.newArrayList("persistent://prop-xyz/ns1/ds2"));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer().topic("persistent://prop-xyz/ns1/ds2")
                .subscriptionType(SubscriptionType.Shared);
        Consumer<byte[]> consumer1 = consumerBuilder.clone().subscriptionName("my-sub1").subscribe();
        Consumer<byte[]> consumer2 = consumerBuilder.clone().subscriptionName("my-sub2").subscribe();
        Consumer<byte[]> consumer3 = consumerBuilder.clone().subscriptionName("my-sub3").subscribe();

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1/ds2").size(), 3);

        List<MessageId> messageIds = publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/ds2", 10);

        TopicStats topicStats = admin.topics().getStats("persistent://prop-xyz/ns1/ds2");
        assertEquals(topicStats.getSubscriptions().get("my-sub1").getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get("my-sub2").getMsgBacklog(), 10);
        assertEquals(topicStats.getSubscriptions().get("my-sub3").getMsgBacklog(), 10);

        Thread.sleep(1000);
        admin.topics().expireMessages("persistent://prop-xyz/ns1/ds2", "my-sub1", 1);
        // Wait at most 2 seconds for sub1's message to expire.
        Awaitility.await().untilAsserted(() -> assertTrue(
                admin.topics().getStats("persistent://prop-xyz/ns1/ds2").getSubscriptions().get("my-sub1").getLastMarkDeleteAdvancedTimestamp() > 0L));
        topicStats = admin.topics().getStats("persistent://prop-xyz/ns1/ds2");
        SubscriptionStats subStats1 = topicStats.getSubscriptions().get("my-sub1");
        assertEquals(subStats1.getMsgBacklog(), 0);
        SubscriptionStats subStats2 = topicStats.getSubscriptions().get("my-sub2");
        assertEquals(subStats2.getMsgBacklog(), 10);
        assertEquals(subStats2.getLastMarkDeleteAdvancedTimestamp(), 0L);
        SubscriptionStats subStats3 = topicStats.getSubscriptions().get("my-sub3");
        assertEquals(subStats3.getMsgBacklog(), 10);
        assertEquals(subStats3.getLastMarkDeleteAdvancedTimestamp(), 0L);

        admin.topics().expireMessages("persistent://prop-xyz/ns1/ds2", "my-sub2",
                messageIds.get(4), false);
        // Wait at most 2 seconds for sub2's message to expire.
        Awaitility.await().untilAsserted(() -> assertTrue(
                admin.topics().getStats("persistent://prop-xyz/ns1/ds2").getSubscriptions().get("my-sub2").getLastMarkDeleteAdvancedTimestamp() > 0L));
        topicStats = admin.topics().getStats("persistent://prop-xyz/ns1/ds2");
        subStats1 = topicStats.getSubscriptions().get("my-sub1");
        assertEquals(subStats1.getMsgBacklog(), 0);
        assertTrue(subStats1.getLastMarkDeleteAdvancedTimestamp() > 0L);
        long sub2lastMarkDeleteAdvancedTimestamp = subStats1.getLastMarkDeleteAdvancedTimestamp();
        subStats2 = topicStats.getSubscriptions().get("my-sub2");
        assertEquals(subStats2.getMsgBacklog(), 5);
        subStats3 = topicStats.getSubscriptions().get("my-sub3");
        assertEquals(subStats3.getMsgBacklog(), 10);
        assertEquals(subStats3.getLastMarkDeleteAdvancedTimestamp(), 0L);

        try {
            admin.topics().expireMessagesForAllSubscriptions("persistent://prop-xyz/ns1/ds2", 1);
        } catch (Exception e) {
            // my-sub1 has no msg backlog, so expire message won't be issued on that subscription
            assertTrue(e.getMessage().startsWith("Expire message by timestamp not issued on topic"));
        }
        // Wait at most 2 seconds for sub3's message to expire.
        Awaitility.await().untilAsserted(() -> assertTrue(
                admin.topics().getStats("persistent://prop-xyz/ns1/ds2").getSubscriptions().get("my-sub3").getLastMarkDeleteAdvancedTimestamp() > 0L));
        topicStats = admin.topics().getStats("persistent://prop-xyz/ns1/ds2");
        subStats1 = topicStats.getSubscriptions().get("my-sub1");
        assertEquals(subStats1.getMsgBacklog(), 0);
        assertEquals(subStats1.getLastMarkDeleteAdvancedTimestamp(), subStats1.getLastMarkDeleteAdvancedTimestamp());
        // Wait at most 2 seconds for rest of sub2's message to expire.
        subStats2 = topicStats.getSubscriptions().get("my-sub2");
        assertEquals(subStats2.getMsgBacklog(), 0);
        assertTrue(subStats2.getLastMarkDeleteAdvancedTimestamp() > sub2lastMarkDeleteAdvancedTimestamp);
        subStats3 = topicStats.getSubscriptions().get("my-sub3");
        assertEquals(subStats3.getMsgBacklog(), 0);

        consumer1.close();
        consumer2.close();
        consumer3.close();
    }

    @Test
    public void testPersistentTopicsExpireMessagesInvalidPartitionIndex() throws Exception {
        // Force to create a topic
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/ds2-partition-2", 0);
        assertEquals(admin.topics().getList("prop-xyz/ns1"),
                Lists.newArrayList("persistent://prop-xyz/ns1/ds2-partition-2"));

        // create consumer and subscription
        @Cleanup
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getWebServiceAddress())
                .statsInterval(0, TimeUnit.SECONDS)
                .build();
        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                .topic("persistent://prop-xyz/ns1/ds2-partition-2")
                .subscriptionType(SubscriptionType.Shared);
        @Cleanup
        Consumer<byte[]> consumer = consumerBuilder.clone().subscriptionName("my-sub").subscribe();

        assertEquals(admin.topics().getSubscriptions("persistent://prop-xyz/ns1/ds2-partition-2").size(), 1);
        publishMessagesOnPersistentTopic("persistent://prop-xyz/ns1/ds2-partition-2", 10);
        try {
            admin.topics().expireMessages("persistent://prop-xyz/ns1/ds2-partition-2", "my-sub",
                    new MessageIdImpl(1, 1, 1), false);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Invalid parameter for expire message by position"));
        }
    }

    /**
     * Verify: PersistentTopicsBase.expireMessages()/expireMessagesForAllSubscriptions() for PartitionTopic
     *
     * @throws Exception
     */
    @Test
    public void testPersistentTopicExpireMessageOnPartitionTopic() throws Exception {

        admin.topics().createPartitionedTopic("persistent://prop-xyz/ns1/ds1", 4);

        // create consumer and subscription
        URL pulsarUrl = new URL(pulsar.getWebServiceAddress());
        @Cleanup
        PulsarClient client = PulsarClient.builder().serviceUrl(pulsarUrl.toString()).statsInterval(0, TimeUnit.SECONDS)
                .build();
        Consumer<byte[]> consumer = client.newConsumer().topic("persistent://prop-xyz/ns1/ds1")
                .subscriptionName("my-sub").subscribe();

        Producer<byte[]> producer = client.newProducer(Schema.BYTES)
                .topic("persistent://prop-xyz/ns1/ds1")
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .create();
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        PartitionedTopicStats topicStats = admin.topics().getPartitionedStats("persistent://prop-xyz/ns1/ds1",
                true);
        assertEquals(topicStats.getSubscriptions().get("my-sub").getMsgBacklog(), 10);

        TopicStats partitionStatsPartition0 = topicStats.getPartitions()
                .get("persistent://prop-xyz/ns1/ds1-partition-0");
        TopicStats partitionStatsPartition1 = topicStats.getPartitions()
                .get("persistent://prop-xyz/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);
        assertEquals(partitionStatsPartition1.getSubscriptions().get("my-sub").getMsgBacklog(), 3, 1);

        Thread.sleep(1000);
        admin.topics().expireMessagesForAllSubscriptions("persistent://prop-xyz/ns1/ds1", 1);
        Thread.sleep(1000);

        topicStats = admin.topics().getPartitionedStats("persistent://prop-xyz/ns1/ds1", true);
        partitionStatsPartition0 = topicStats.getPartitions().get("persistent://prop-xyz/ns1/ds1-partition-0");
        partitionStatsPartition1 = topicStats.getPartitions().get("persistent://prop-xyz/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.getSubscriptions().get("my-sub").getMsgBacklog(), 0);
        assertEquals(partitionStatsPartition1.getSubscriptions().get("my-sub").getMsgBacklog(), 0);

        producer.close();
        consumer.close();
        client.close();

    }

    @Test
    public void testNamespaceNotExist() {
        final String nonPartitionedtopic = "persistent://prop-xyz/no-exist/non-partitioned-topic";
        try {
            admin.topics().createNonPartitionedTopic(nonPartitionedtopic);
            fail("should falied for namespaces not exist");
        } catch (Exception e) {
            assertTrue(e instanceof NotFoundException);
            assertTrue(e.getMessage().equals("Namespace not found"));
        }
    }

    @Test
    public void testPersistentTopicCreation() throws Exception {
        final String nonPartitionedtopic = "persistent://prop-xyz/ns1/non-partitioned-topic";
        final String partitionedtopic = "persistent://prop-xyz/ns1/partitioned-topic";

        admin.topics().createNonPartitionedTopic(nonPartitionedtopic);
        try {
            admin.topics().createNonPartitionedTopic(nonPartitionedtopic);
            fail("should not be able to create an existed non-partitioned topic");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof ConflictException);
        }

        admin.topics().createPartitionedTopic(partitionedtopic, 2);
        try {
            admin.topics().createPartitionedTopic(partitionedtopic, 1);
            fail("should not be able to create an existed partitioned topic");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof ConflictException);
        }

        try {
            admin.topics().createPartitionedTopic(nonPartitionedtopic, 2);
            fail("should not be able to create a partitioned topic with the same name");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof ConflictException);
        }

        try {
            admin.topics().createNonPartitionedTopic(partitionedtopic);
            fail("should not be able to create a non-partitioned topic with the same name");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof ConflictException);
        }

        // Check create partitioned topic with substring topic name
        admin.topics().createPartitionedTopic("persistent://prop-xyz/ns1/create_substring_topic", 1);
        admin.topics().createPartitionedTopic("persistent://prop-xyz/ns1/substring_topic", 1);
    }

    /**
     * This test-case verifies that broker should support both url/uri encoding for topic-name. It calls below api with
     * url-encoded and also uri-encoded topic-name in http request: a. PartitionedMetadataLookup b. TopicLookupBase c.
     * Topic Stats
     *
     * @param topicName
     * @throws Exception
     */
    @Test(dataProvider = "topicName")
    public void testPulsarAdminForUriAndUrlEncoding(String topicName) throws Exception {
        final String ns1 = "prop-xyz/ns1";
        final String topic1 = "persistent://" + ns1 + "/" + topicName;
        final String urlEncodedTopic = Codec.encode(topicName);
        final String uriEncodedTopic = urlEncodedTopic.replaceAll("\\+", "%20");
        final int numOfPartitions = 4;
        admin.topics().createPartitionedTopic(topic1, numOfPartitions);
        // Create a consumer to get stats on this topic
        pulsarClient.newConsumer().topic(topic1).subscriptionName("my-subscriber-name").subscribe();

        TopicsImpl persistent = (TopicsImpl) admin.topics();
        Field field = TopicsImpl.class.getDeclaredField("adminV2Topics");
        field.setAccessible(true);
        WebTarget persistentTopics = (WebTarget) field.get(persistent);

        // (1) Get PartitionedMetadata : with Url and Uri encoding
        final CompletableFuture<PartitionedTopicMetadata> urlEncodedPartitionedMetadata = new CompletableFuture<>();
        // (a) Url encoding
        persistent.asyncGetRequest(
                persistentTopics.path("persistent").path(ns1).path(urlEncodedTopic).path("partitions"),
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
        persistent.asyncGetRequest(
                persistentTopics.path("persistent").path(ns1).path(uriEncodedTopic).path("partitions"),
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
                .request(target2.path("/topic/persistent").path(ns1 + "/" + urlEncodedTopic))
                .get(LookupData.class);
        // (b) Uri encoding
        LookupData uriEncodedLookupData = lookup
                .request(target2.path("/topic/persistent").path(ns1 + "/" + uriEncodedTopic))
                .get(LookupData.class);
        Assert.assertNotNull(urlEncodedLookupData.getBrokerUrl());
        assertEquals(urlEncodedLookupData.getBrokerUrl(), uriEncodedLookupData.getBrokerUrl());

        // partitioned topic lookup
        Map<String, String> lookupDataList = lookup.lookupPartitionedTopic(topic1);
        assertEquals(numOfPartitions, lookupDataList.keySet().size());

        // (3) Get Topic Stats
        final CompletableFuture<TopicStats> urlStats = new CompletableFuture<>();
        // (a) Url encoding
        persistent.asyncGetRequest(persistentTopics.path("persistent").path(ns1).path(urlEncodedTopic + "-partition-1").path("stats"),
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
        persistent.asyncGetRequest(
                persistentTopics.path("persistent").path(ns1).path(uriEncodedTopic + "-partition-1").path("stats"),
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

        private ServiceConfiguration conf;

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
    public void testTopicBundleRangeLookup() throws PulsarAdminException, PulsarServerException, Exception {
        admin.clusters().createCluster("usw", ClusterData.builder().build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet("test", "usw"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);
        admin.namespaces().createNamespace("prop-xyz/getBundleNs", 100);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/getBundleNs").bundles.getNumBundles(), 100);

        // (1) create a topic
        final String topicName = "persistent://prop-xyz/getBundleNs/topic1";
        String bundleRange = admin.lookups().getBundleRange(topicName);
        assertEquals(bundleRange, pulsar.getNamespaceService().getBundle(TopicName.get(topicName)).getBundleRange());
    }

    @Test
    public void testTriggerCompaction() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";

        // create a topic by creating a producer
        pulsarClient.newProducer(Schema.BYTES).topic(topicName).create().close();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // mock actual compaction, we don't need to really run it
        CompletableFuture<Long> promise = new CompletableFuture<Long>();
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
    public void testTriggerCompactionPartitionedTopic() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/test-part";
        int numPartitions = 2;
        admin.topics().createPartitionedTopic(topicName, numPartitions);

        // create a partitioned topic by creating a producer
        pulsarClient.newProducer(Schema.BYTES).topic(topicName).create().close();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        // mock actual compaction, we don't need to really run it
        CompletableFuture<Long> promise = new CompletableFuture<>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(promise).when(compactor).compact(topicName + "-partition-0");

        CompletableFuture<Long> promise1 = new CompletableFuture<>();
        doReturn(promise1).when(compactor).compact(topicName + "-partition-1");
        admin.topics().triggerCompaction(topicName);

        // verify compact called once by each partition topic
        verify(compactor).compact(topicName + "-partition-0");
        verify(compactor).compact(topicName + "-partition-1");
        try {
            admin.topics().triggerCompaction(topicName);

            fail("Shouldn't be able to run while already running");
        } catch (PulsarAdminException e) {
            // expected
        }
        // compact shouldn't have been called again
        verify(compactor).compact(topicName + "-partition-0");
        verify(compactor).compact(topicName + "-partition-1");

        // complete first compaction, and trigger again
        promise.complete(1L);
        promise1.complete(1L);
        admin.topics().triggerCompaction(topicName);

        // verify compact was called again
        verify(compactor, times(2)).compact(topicName + "-partition-0");
        verify(compactor, times(2)).compact(topicName + "-partition-1");
    }

    @Test
    public void testCompactionStatus() throws Exception {
        String topicName = "persistent://prop-xyz/ns1/topic1";

        // create a topic by creating a producer
        pulsarClient.newProducer(Schema.BYTES).topic(topicName).create().close();
        assertNotNull(pulsar.getBrokerService().getTopicReference(topicName));

        assertEquals(admin.topics().compactionStatus(topicName).status,
                LongRunningProcessStatus.Status.NOT_RUN);

        // mock actual compaction, we don't need to really run it
        CompletableFuture<Long> promise = new CompletableFuture<Long>();
        Compactor compactor = pulsar.getCompactor();
        doReturn(promise).when(compactor).compact(topicName);
        admin.topics().triggerCompaction(topicName);

        assertEquals(admin.topics().compactionStatus(topicName).status,
                LongRunningProcessStatus.Status.RUNNING);

        promise.complete(1L);

        assertEquals(admin.topics().compactionStatus(topicName).status,
                LongRunningProcessStatus.Status.SUCCESS);

        CompletableFuture<Long> errorPromise = new CompletableFuture<Long>();
        doReturn(errorPromise).when(compactor).compact(topicName);
        admin.topics().triggerCompaction(topicName);
        errorPromise.completeExceptionally(new Exception("Failed at something"));

        assertEquals(admin.topics().compactionStatus(topicName).status,
                LongRunningProcessStatus.Status.ERROR);
        assertTrue(admin.topics().compactionStatus(topicName).lastError.contains("Failed at something"));
    }

    @Test(timeOut = 20000)
    public void testTopicStatsLastExpireTimestampForSubscription() throws PulsarAdminException, PulsarClientException, InterruptedException {
        admin.namespaces().setNamespaceMessageTTL("prop-xyz/ns1", 10);
        final String topic = "persistent://prop-xyz/ns1/testTopicStatsLastExpireTimestampForSubscription";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();
        for (int i = 0; i < 10; i++) {
            producer.send(new byte[1024 * i * 5]);
        }
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().size(), 1);
        Assert.assertEquals(admin.topics().getStats(topic).getSubscriptions().values().iterator().next().getLastExpireTimestamp(), 0L);
        Thread.sleep(10000);
        // Update policy to trigger message expiry check.
        admin.namespaces().setNamespaceMessageTTL("prop-xyz/ns1", 5);
        Awaitility.await().until(() -> admin.topics().getStats(topic).getSubscriptions().values().iterator().next().getLastExpireTimestamp() > 0L);
    }

    @Test(timeOut = 150000)
    public void testSubscriptionExpiry() throws Exception {
        final String namespace1 = "prop-xyz/sub-gc1";
        final String namespace2 = "prop-xyz/sub-gc2";
        final String namespace3 = "prop-xyz/sub-gc3";
        final String topic1 = "persistent://" + namespace1 + "/testSubscriptionExpiry";
        final String topic2 = "persistent://" + namespace2 + "/testSubscriptionExpiry";
        final String topic3 = "persistent://" + namespace3 + "/testSubscriptionExpiry";
        final String sub = "sub1";

        admin.namespaces().createNamespace(namespace1, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(namespace2, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(namespace3, Sets.newHashSet("test"));
        admin.topics().createSubscription(topic1, sub, MessageId.latest);
        admin.topics().createSubscription(topic2, sub, MessageId.latest);
        admin.topics().createSubscription(topic3, sub, MessageId.latest);
        admin.namespaces().setSubscriptionExpirationTime(namespace1, 0);
        admin.namespaces().setSubscriptionExpirationTime(namespace2, 1);
        admin.namespaces().setSubscriptionExpirationTime(namespace3, 1);
        admin.namespaces().removeSubscriptionExpirationTime(namespace3);

        Assert.assertEquals((int) admin.namespaces().getSubscriptionExpirationTime(namespace1), 0);
        Assert.assertEquals((int) admin.namespaces().getSubscriptionExpirationTime(namespace2), 1);
        Assert.assertNull(admin.namespaces().getSubscriptionExpirationTime(namespace3));


        Awaitility.await().timeout(120, TimeUnit.SECONDS)
                .until(() -> admin.topics().getSubscriptions(topic2).size() == 0);
        Assert.assertEquals(admin.topics().getSubscriptions(topic1).size(), 1);
        Assert.assertEquals(admin.topics().getSubscriptions(topic2).size(), 0);
        Assert.assertEquals(admin.topics().getSubscriptions(topic3).size(), 1);

        admin.topics().delete(topic1);
        admin.topics().delete(topic2);
        admin.topics().delete(topic3);
        admin.namespaces().deleteNamespace(namespace1);
        admin.namespaces().deleteNamespace(namespace2);
        admin.namespaces().deleteNamespace(namespace3);
    }

    @Test
    public void testCreateAndDeleteNamespaceWithBundles() throws Exception {
        admin.clusters().createCluster("usw", ClusterData.builder().build());
        TenantInfoImpl tenantInfo = new TenantInfoImpl(Sets.newHashSet("role1", "role2"),
                Sets.newHashSet("test", "usw"));
        admin.tenants().updateTenant("prop-xyz", tenantInfo);

        String ns = BrokerTestUtil.newUniqueName("prop-xyz/ns");

        admin.namespaces().createNamespace(ns, 24);
        admin.namespaces().deleteNamespace(ns);

        // Re-create and re-delete
        admin.namespaces().createNamespace(ns, 32);
        admin.namespaces().deleteNamespace(ns);
    }

    @Test
    public void testBacklogSizeShouldBeZeroWhenConsumerAckedAllMessages() throws Exception {
        final String topic = "persistent://prop-xyz/ns1/testBacklogSizeShouldBeZeroWhenConsumerAckedAllMessages";
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topic)
                .subscriptionName("sub-1")
                .subscribe();
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .create();

        final int messages = 33;
        for (int i = 0; i < messages; i++) {
            producer.send(new byte[1024 * i * 5]);
        }

        TopicStats topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getSubscriptions().get("sub-1").getLastMarkDeleteAdvancedTimestamp(), 0L);

        for (int i = 0; i < messages; i++) {
            consumer.acknowledgeCumulative(consumer.receive());
        }

        // Wait ack send
        Thread.sleep(1000);

        topicStats = admin.topics().getStats(topic);
        assertEquals(topicStats.getBacklogSize(), 0);
        assertTrue(topicStats.getSubscriptions().get("sub-1").getLastMarkDeleteAdvancedTimestamp() > 0L);
    }

    @Test
    public void testGetTtlDurationDefaultInSeconds() throws Exception {
        conf.setTtlDurationDefaultInSeconds(3600);
        Integer seconds = admin.namespaces().getPolicies("prop-xyz/ns1").message_ttl_in_seconds;
        assertNull(seconds);
    }

    @Test
    public void testGetReadPositionWhenJoining() throws Exception {
        final String topic = "persistent://prop-xyz/ns1/testGetReadPositionWhenJoining-" + UUID.randomUUID().toString();
        final String subName = "my-sub";
        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        final int messages = 10;
        MessageIdImpl messageId = null;
        for (int i = 0; i < messages; i++) {
            messageId = (MessageIdImpl) producer.send(("Hello Pulsar - " + i).getBytes());
        }

        for (int i = 0; i < 2; i++) {
            pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Key_Shared)
                    .subscriptionName(subName)
                    .subscribe();
        }

        TopicStats stats = admin.topics().getStats(topic);
        Assert.assertEquals(stats.getSubscriptions().size(), 1);
        SubscriptionStats subStats = stats.getSubscriptions().get(subName);
        Assert.assertNotNull(subStats);
        Assert.assertEquals(subStats.getConsumers().size(), 2);
        ConsumerStats consumerStats = subStats.getConsumers().get(0);
        Assert.assertEquals(consumerStats.getReadPositionWhenJoining(),
                PositionImpl.get(messageId.getLedgerId(), messageId.getEntryId() + 1).toString());
    }

    @Test
    public void testPartitionedTopicMsgDelayedAggregated() throws Exception {
        final String topic = "persistent://prop-xyz/ns1/testPartitionedTopicMsgDelayedAggregated-" + UUID.randomUUID().toString();
        final String subName = "my-sub";
        final int numPartitions = 2;
        conf.setSubscriptionRedeliveryTrackerEnabled(true);
        conf.setDelayedDeliveryEnabled(true);
        admin.topics().createPartitionedTopic(topic, numPartitions);

        for (int i = 0; i < 2; i++) {
            pulsarClient.newConsumer()
                    .topic(topic)
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName(subName)
                    .subscribe();
        }

        Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topic)
                .enableBatching(false)
                .create();

        final int messages = 100;
        for (int i = 0; i < messages; i++) {
            String msg = "Hello Pulsar - " + i;
            producer.send(msg.getBytes());
            producer.newMessage().deliverAfter(1L, TimeUnit.HOURS).value(msg.getBytes()).send();
        }
        PartitionedTopicStats partitionedTopicStats = admin.topics().getPartitionedStats(topic, false);
        Assert.assertNotNull(partitionedTopicStats);
        SubscriptionStats subStats = partitionedTopicStats.getSubscriptions().get(subName);
        Assert.assertNotNull(subStats);
        Assert.assertEquals(subStats.getMsgBacklog(), subStats.getMsgBacklogNoDelayed() + subStats.getMsgDelayed());

        partitionedTopicStats = admin.topics().getPartitionedStats(topic, true);
        Assert.assertNotNull(partitionedTopicStats);
        subStats = partitionedTopicStats.getSubscriptions().get(subName);
        Assert.assertNotNull(subStats);
        Assert.assertEquals(subStats.getMsgBacklog(), subStats.getMsgBacklogNoDelayed() + subStats.getMsgDelayed());
        Assert.assertNotNull(partitionedTopicStats.getPartitions());
        Assert.assertEquals(partitionedTopicStats.getPartitions().size(), numPartitions);

        long sumMsgBacklog = 0;
        long sumMsgBacklogNoDelayed = 0;
        long sumMsgDelayed = 0;
        for(TopicStats stats: partitionedTopicStats.getPartitions().values()){
            Assert.assertNotNull(stats);
            SubscriptionStats partitionedSubStats = stats.getSubscriptions().get(subName);
            Assert.assertNotNull(partitionedSubStats);
            sumMsgBacklog += partitionedSubStats.getMsgBacklog();
            sumMsgBacklogNoDelayed += partitionedSubStats.getMsgBacklogNoDelayed();
            sumMsgDelayed += partitionedSubStats.getMsgDelayed();
        }
        Assert.assertEquals(sumMsgBacklog, sumMsgBacklogNoDelayed + sumMsgDelayed);
        Assert.assertEquals(sumMsgBacklog, subStats.getMsgBacklog());
        Assert.assertEquals(sumMsgBacklogNoDelayed, subStats.getMsgBacklogNoDelayed());
        Assert.assertEquals(sumMsgDelayed, subStats.getMsgDelayed());
    }

    @Test(timeOut = 20000)
    public void testPartitionedTopicTruncate() throws Exception {
        final String topicName = "persistent://prop-xyz/ns1/testTruncateTopic-" + UUID.randomUUID().toString();
        final String subName = "my-sub";
        this.conf.setTopicLevelPoliciesEnabled(true);
        this.conf.setSystemTopicEnabled(true);
        admin.topics().createPartitionedTopic(topicName,6);
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(60, 50));
        List<MessageId> messageIds = publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().createSubscription(topicName, subName, messageIds.get(0));
        admin.topics().unload(topicName);
        publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().unload(topicName);
        publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().truncate(topicName);
        PartitionedTopicInternalStats stats = admin.topics().getPartitionedInternalStats(topicName);
        for (Map.Entry<String, PersistentTopicInternalStats> statsEntry : stats.partitions.entrySet()) {
            assertTrue(statsEntry.getValue().ledgers.size() <= 2);
        }

    }

    @Test(timeOut = 20000)
    public void testNonPartitionedTopicTruncate() throws Exception {
        final String topicName = "persistent://prop-xyz/ns1/testTruncateTopic-" + UUID.randomUUID().toString();
        final String subName = "my-sub";
        this.conf.setTopicLevelPoliciesEnabled(true);
        admin.topics().createNonPartitionedTopic(topicName);
        admin.namespaces().setRetention("prop-xyz/ns1", new RetentionPolicies(60, 50));
        List<MessageId> messageIds = publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().createSubscription(topicName, subName, messageIds.get(0));
        admin.topics().unload(topicName);
        publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().unload(topicName);
        publishMessagesOnPersistentTopic(topicName, 10);
        admin.topics().truncate(topicName);
        PersistentTopicInternalStats stats = admin.topics().getInternalStats(topicName);
        assertTrue(stats.ledgers.size() <= 1);
    }

    @Test(timeOut = 20000)
    public void testNonPersistentTopicTruncate() throws Exception {
        final String topicName = "non-persistent://prop-xyz/ns1/testTruncateTopic-" + UUID.randomUUID().toString();
        admin.topics().createNonPartitionedTopic(topicName);
        assertThrows(() -> {admin.topics().truncate(topicName);});
    }

    @Test(timeOut = 20000)
    public void testPeekEncryptedMessages() throws Exception {
        final String topicName = "persistent://prop-xyz/ns1/testPeekEncryptedMessages-" + UUID.randomUUID().toString();
        final String subName = "my-sub";

        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, subName, MessageId.latest);

        final Producer<byte[]> producer = pulsarClient.newProducer()
                .topic(topicName)
                .enableBatching(true)
                .addEncryptionKey("my-app-key")
                .defaultCryptoKeyReader("file:./src/test/resources/certificate/public-key.client-rsa.pem")
                .create();

        for (int i = 0; i < 5; i++) {
            producer.send(("message-" + i).getBytes());
        }
        producer.close();

        final List<Message<byte[]>> peekedMessages = admin.topics().peekMessages(topicName, subName, 5);
        assertEquals(peekedMessages.size(), 5);

        final Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName(subName)
                .cryptoFailureAction(ConsumerCryptoFailureAction.CONSUME)
                .subscribe();

        final List<Message<byte[]>> receivedMessages = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
            receivedMessages.add(msg);
            consumer.acknowledge(msg);
        }
        consumer.unsubscribe();

        for (int i = 0; i < 5; i++) {
            assertEquals(peekedMessages.get(i).getMessageId(), receivedMessages.get(i).getMessageId());
            assertEquals(peekedMessages.get(i).getData(), receivedMessages.get(i).getData());
        }
    }
}
