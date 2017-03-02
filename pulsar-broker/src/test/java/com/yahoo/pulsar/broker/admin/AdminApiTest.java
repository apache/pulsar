/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.broker.admin;

import static com.yahoo.pulsar.broker.service.BrokerService.BROKER_SERVICE_CONFIGURATION_PATH;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;

import org.apache.bookkeeper.test.PortManager;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.hash.Hashing;
import com.yahoo.pulsar.broker.PulsarServerException;
import com.yahoo.pulsar.broker.PulsarService;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import com.yahoo.pulsar.broker.namespace.NamespaceEphemeralData;
import com.yahoo.pulsar.broker.namespace.NamespaceService;
import com.yahoo.pulsar.broker.service.BrokerService;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.ConflictException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.NotAuthorizedException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.NotFoundException;
import com.yahoo.pulsar.client.admin.PulsarAdminException.PreconditionFailedException;
import com.yahoo.pulsar.client.admin.internal.LookupImpl;
import com.yahoo.pulsar.client.admin.internal.PersistentTopicsImpl;
import com.yahoo.pulsar.client.admin.internal.PropertiesImpl;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.Producer;
import com.yahoo.pulsar.client.api.ProducerConfiguration;
import com.yahoo.pulsar.client.api.ProducerConfiguration.MessageRoutingMode;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.SubscriptionType;
import com.yahoo.pulsar.common.lookup.data.LookupData;
import com.yahoo.pulsar.common.naming.DestinationName;
import com.yahoo.pulsar.common.naming.NamespaceBundle;
import com.yahoo.pulsar.common.naming.NamespaceBundleFactory;
import com.yahoo.pulsar.common.naming.NamespaceBundles;
import com.yahoo.pulsar.common.naming.NamespaceName;
import com.yahoo.pulsar.common.partition.PartitionedTopicMetadata;
import com.yahoo.pulsar.common.policies.data.AuthAction;
import com.yahoo.pulsar.common.policies.data.AutoFailoverPolicyData;
import com.yahoo.pulsar.common.policies.data.AutoFailoverPolicyType;
import com.yahoo.pulsar.common.policies.data.BacklogQuota;
import com.yahoo.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import com.yahoo.pulsar.common.policies.data.BacklogQuota.RetentionPolicy;
import com.yahoo.pulsar.common.policies.data.BrokerAssignment;
import com.yahoo.pulsar.common.policies.data.ClusterData;
import com.yahoo.pulsar.common.policies.data.NamespaceIsolationData;
import com.yahoo.pulsar.common.policies.data.NamespaceOwnershipStatus;
import com.yahoo.pulsar.common.policies.data.PartitionedTopicStats;
import com.yahoo.pulsar.common.policies.data.PersistencePolicies;
import com.yahoo.pulsar.common.policies.data.PersistentTopicInternalStats;
import com.yahoo.pulsar.common.policies.data.PersistentTopicStats;
import com.yahoo.pulsar.common.policies.data.Policies;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;
import com.yahoo.pulsar.common.policies.data.RetentionPolicies;
import com.yahoo.pulsar.common.util.Codec;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;

public class AdminApiTest extends MockedPulsarServiceBaseTest {

    private static final Logger LOG = LoggerFactory.getLogger(AdminApiTest.class);

    private PulsarService otherPulsar;

    private PulsarAdmin otheradmin;

    private NamespaceBundleFactory bundleFactory;

    private final int SECONDARY_BROKER_PORT = PortManager.nextFreePort();
    private final int SECONDARY_BROKER_WEBSERVICE_PORT = PortManager.nextFreePort();

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        conf.setLoadBalancerEnabled(true);
        super.internalSetup();

        bundleFactory = new NamespaceBundleFactory(pulsar, Hashing.crc32());

        // create otherbroker to test redirect on calls that need
        // namespace ownership
        ServiceConfiguration otherconfig = new ServiceConfiguration();
        otherconfig.setBrokerServicePort(SECONDARY_BROKER_PORT);
        otherconfig.setWebServicePort(SECONDARY_BROKER_WEBSERVICE_PORT);
        otherconfig.setLoadBalancerEnabled(false);
        otherconfig.setClusterName("test");

        otherPulsar = startBroker(otherconfig);

        otheradmin = new PulsarAdmin(new URL("http://127.0.0.1" + ":" + SECONDARY_BROKER_WEBSERVICE_PORT),
                (Authentication) null);

        // Setup namespaces
        admin.clusters().createCluster("use", new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));
        PropertyAdmin propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"), Sets.newHashSet("use"));
        admin.properties().createProperty("prop-xyz", propertyAdmin);
        admin.namespaces().createNamespace("prop-xyz/use/ns1");
    }

    @AfterMethod
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        otheradmin.close();
        otherPulsar.close();
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

    @Test
    public void clusters() throws Exception {
        admin.clusters().createCluster("usw",
                new ClusterData("http://broker.messaging.use.example.com" + ":" + BROKER_WEBSERVICE_PORT));
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("use", "usw"));

        assertEquals(admin.clusters().getCluster("use"),
                new ClusterData("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT));

        admin.clusters().updateCluster("usw",
                new ClusterData("http://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT));
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("use", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                new ClusterData("http://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT));

        admin.clusters().updateCluster("usw",
                new ClusterData("http://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT,
                        "https://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT_TLS));
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("use", "usw"));
        assertEquals(admin.clusters().getCluster("usw"),
                new ClusterData("http://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT,
                        "https://new-broker.messaging.usw.example.com" + ":" + BROKER_WEBSERVICE_PORT_TLS));

        admin.clusters().deleteCluster("usw");
        Thread.sleep(300);

        assertEquals(admin.clusters().getClusters(), Lists.newArrayList("use"));

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.clusters().deleteCluster("use");
        assertEquals(admin.clusters().getClusters(), Lists.newArrayList());

        // Check name validation
        try {
            admin.clusters().createCluster("bf!", new ClusterData("http://dummy.messaging.example.com"));
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
            NamespaceIsolationData nsPolicyData1 = new NamespaceIsolationData();
            nsPolicyData1.namespaces = new ArrayList<String>();
            nsPolicyData1.namespaces.add("other/use/other.*");
            nsPolicyData1.primary = new ArrayList<String>();
            nsPolicyData1.primary.add("prod1-broker[4-6].messaging.use.example.com");
            nsPolicyData1.secondary = new ArrayList<String>();
            nsPolicyData1.secondary.add("prod1-broker.*.messaging.use.example.com");
            nsPolicyData1.auto_failover_policy = new AutoFailoverPolicyData();
            nsPolicyData1.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
            nsPolicyData1.auto_failover_policy.parameters = new HashMap<String, String>();
            nsPolicyData1.auto_failover_policy.parameters.put("min_limit", "1");
            nsPolicyData1.auto_failover_policy.parameters.put("usage_threshold", "100");
            admin.clusters().createNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            String policyName2 = "policy-2";
            NamespaceIsolationData nsPolicyData2 = new NamespaceIsolationData();
            nsPolicyData2.namespaces = new ArrayList<String>();
            nsPolicyData2.namespaces.add("other/use/other.*");
            nsPolicyData2.primary = new ArrayList<String>();
            nsPolicyData2.primary.add("prod1-broker[4-6].messaging.use.example.com");
            nsPolicyData2.secondary = new ArrayList<String>();
            nsPolicyData2.secondary.add("prod1-broker.*.messaging.use.example.com");
            nsPolicyData2.auto_failover_policy = new AutoFailoverPolicyData();
            nsPolicyData2.auto_failover_policy.policy_type = AutoFailoverPolicyType.min_available;
            nsPolicyData2.auto_failover_policy.parameters = new HashMap<String, String>();
            nsPolicyData2.auto_failover_policy.parameters.put("min_limit", "1");
            nsPolicyData2.auto_failover_policy.parameters.put("usage_threshold", "100");
            admin.clusters().createNamespaceIsolationPolicy("use", policyName2, nsPolicyData2);

            // verify create indirectly with get
            Map<String, NamespaceIsolationData> policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);
            assertEquals(policiesMap.get(policyName2), nsPolicyData2);

            // verify update of primary
            nsPolicyData1.primary.remove(0);
            nsPolicyData1.primary.add("prod1-broker[1-2].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify primary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of secondary
            nsPolicyData1.secondary.remove(0);
            nsPolicyData1.secondary.add("prod1-broker[3-4].messaging.use.example.com");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify secondary change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover policy limit
            nsPolicyData1.auto_failover_policy.parameters.put("min_limit", "10");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify min_limit change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify update of failover usage_threshold limit
            nsPolicyData1.auto_failover_policy.parameters.put("usage_threshold", "80");
            admin.clusters().updateNamespaceIsolationPolicy("use", policyName1, nsPolicyData1);

            // verify usage_threshold change
            policiesMap = admin.clusters().getNamespaceIsolationPolicies("use");
            assertEquals(policiesMap.get(policyName1), nsPolicyData1);

            // verify single get
            NamespaceIsolationData policy1Data = admin.clusters().getNamespaceIsolationPolicy("use", policyName1);
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
        Assert.assertEquals(list.size(), 2);

        Map<String, NamespaceOwnershipStatus> nsMap = admin.brokers().getOwnedNamespaces("use", list.get(0));
        // since sla-monitor ns is not created nsMap.size() == 1 (for HeartBeat Namespace)
        Assert.assertEquals(1, nsMap.size());
        for (String ns : nsMap.keySet()) {
            NamespaceOwnershipStatus nsStatus = nsMap.get(ns);
            if (ns.equals(NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), pulsar.getConfiguration())
                    + "/0x00000000_0xffffffff")) {
                assertEquals(nsStatus.broker_assignment, BrokerAssignment.shared);
                assertFalse(nsStatus.is_controlled);
                assertTrue(nsStatus.is_active);
            }
        }

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.clusters().deleteCluster("use");
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
        // create configuration znode
        ZkUtils.createFullPathOptimistic(mockZookKeeper, BROKER_SERVICE_CONFIGURATION_PATH, "{}".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // Now, znode is created: set the watch and listener on the znode
        Method updateConfigListenerMethod = BrokerService.class
                .getDeclaredMethod("updateConfigurationAndRegisterListeners");
        updateConfigListenerMethod.setAccessible(true);
        updateConfigListenerMethod.invoke(pulsar.getBrokerService());
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(30000);
        // (1) try to update dynamic field
        final long shutdownTime = 10;
        // update configuration
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", Long.toString(shutdownTime));
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

    }

    /**
     * <pre>
     * verifies: that registerListener updates pulsar.config value with newly updated zk-dynamic config
     * NOTE: pulsar can't set the watch on non-existing znode
     * So, when pulsar starts it is not able to set the watch on non-existing znode of dynamicConfiguration
     * So, here, after creating znode we will trigger register explicitly
     * 1.start pulsar
     * 2.update zk-config with admin api
     * 3. trigger watch and listener
     * 4. verify that config is updated
     * </pre>
     * @throws Exception
     */
    @Test
    public void testUpdateDynamicLocalConfiguration() throws Exception {
        // (1) try to update dynamic field
        final long shutdownTime = 10;
        pulsar.getConfiguration().setBrokerShutdownTimeoutMs(30000);
        // update configuration
        admin.brokers().updateDynamicConfiguration("brokerShutdownTimeoutMs", Long.toString(shutdownTime));
        // Now, znode is created: updateConfigurationAndregisterListeners and check if configuration updated
        Method getPermitZkNodeMethod = BrokerService.class.getDeclaredMethod("updateConfigurationAndRegisterListeners");
        getPermitZkNodeMethod.setAccessible(true);
        getPermitZkNodeMethod.invoke(pulsar.getBrokerService());
        // verify value is updated
        assertEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);
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
        try {
            admin.brokers().getAllDynamicConfigurations();
            fail("should have fail as configuration is not exist");
        } catch (PulsarAdminException.NotFoundException ne) {
            // ok : expected
        }
        assertNotEquals(pulsar.getConfiguration().getBrokerShutdownTimeoutMs(), shutdownTime);
        // update configuration
        admin.brokers().updateDynamicConfiguration(configName, Long.toString(shutdownTime));
        // Now, znode is created: updateConfigurationAndregisterListeners and check if configuration updated
        assertEquals(Long.parseLong(admin.brokers().getAllDynamicConfigurations().get(configName)), shutdownTime);
    }
    
    @Test(enabled = true)
    public void properties() throws PulsarAdminException {
        Set<String> allowedClusters = Sets.newHashSet("use");
        PropertyAdmin propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"), allowedClusters);
        admin.properties().updateProperty("prop-xyz", propertyAdmin);

        assertEquals(admin.properties().getProperties(), Lists.newArrayList("prop-xyz"));

        assertEquals(admin.properties().getPropertyAdmin("prop-xyz"), propertyAdmin);

        PropertyAdmin newPropertyAdmin = new PropertyAdmin(Lists.newArrayList("role3", "role4"), allowedClusters);
        admin.properties().updateProperty("prop-xyz", newPropertyAdmin);

        assertEquals(admin.properties().getPropertyAdmin("prop-xyz"), newPropertyAdmin);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.properties().deleteProperty("prop-xyz");
        assertEquals(admin.properties().getProperties(), Lists.newArrayList());

        // Check name validation
        try {
            admin.properties().createProperty("prop-xyz&", propertyAdmin);
            fail("should have failed");
        } catch (PulsarAdminException e) {
            assertTrue(e instanceof PreconditionFailedException);
        }
    }

    @Test(invocationCount = 1)
    public void namespaces() throws PulsarAdminException, PulsarServerException, Exception {
        admin.clusters().createCluster("usw", new ClusterData());
        PropertyAdmin propertyAdmin = new PropertyAdmin(Lists.newArrayList("role1", "role2"),
                Sets.newHashSet("use", "usw"));
        admin.properties().updateProperty("prop-xyz", propertyAdmin);

        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1").bundles, Policies.defaultBundle());

        admin.namespaces().createNamespace("prop-xyz/use/ns2");

        admin.namespaces().createNamespace("prop-xyz/use/ns3", 4);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns3").bundles.numBundles, 4);
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns3").bundles.boundaries.size(), 5);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns3");

        try {
            admin.namespaces().createNamespace("non-existing/usw/ns1");
            fail("Should not have passed");
        } catch (NotFoundException e) {
            // Ok
        }

        assertEquals(admin.namespaces().getNamespaces("prop-xyz"),
                Lists.newArrayList("prop-xyz/use/ns1", "prop-xyz/use/ns2"));
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"),
                Lists.newArrayList("prop-xyz/use/ns1", "prop-xyz/use/ns2"));

        try {
            admin.namespaces().createNamespace("prop-xyz/usc/ns1");
            fail("Should not have passed");
        } catch (NotAuthorizedException e) {
            // Ok, got the non authorized exception since usc cluster is not in the allowed clusters list.
        }

        admin.namespaces().grantPermissionOnNamespace("prop-xyz/use/ns1", "my-role", EnumSet.allOf(AuthAction.class));

        Policies policies = new Policies();
        policies.auth_policies.namespace_auth.put("my-role", EnumSet.allOf(AuthAction.class));

        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1"), policies);
        assertEquals(admin.namespaces().getPermissions("prop-xyz/use/ns1"), policies.auth_policies.namespace_auth);

        assertEquals(admin.namespaces().getDestinations("prop-xyz/use/ns1"), Lists.newArrayList());

        admin.namespaces().revokePermissionsOnNamespace("prop-xyz/use/ns1", "my-role");
        policies.auth_policies.namespace_auth.remove("my-role");
        assertEquals(admin.namespaces().getPolicies("prop-xyz/use/ns1"), policies);

        assertEquals(admin.namespaces().getPersistence("prop-xyz/use/ns1"), new PersistencePolicies(1, 1, 1, 0.0));
        admin.namespaces().setPersistence("prop-xyz/use/ns1", new PersistencePolicies(3, 2, 1, 10.0));
        assertEquals(admin.namespaces().getPersistence("prop-xyz/use/ns1"), new PersistencePolicies(3, 2, 1, 10.0));

        // Force topic creation and namespace being loaded
        Producer producer = pulsarClient.createProducer("persistent://prop-xyz/use/ns1/my-topic");
        producer.close();
        admin.persistentTopics().delete("persistent://prop-xyz/use/ns1/my-topic");

        admin.namespaces().unloadNamespaceBundle("prop-xyz/use/ns1", "0x00000000_0xffffffff");
        NamespaceName ns = new NamespaceName("prop-xyz/use/ns1");
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
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"), Lists.newArrayList("prop-xyz/use/ns2"));

        try {
            admin.namespaces().unload("prop-xyz/use/ns1");
            fail("should have raised exception");
        } catch (Exception e) {
            // OK excepted
        }

        // Force topic creation and namespace being loaded
        producer = pulsarClient.createProducer("persistent://prop-xyz/use/ns2/my-topic");
        producer.close();
        admin.persistentTopics().delete("persistent://prop-xyz/use/ns2/my-topic");

        // both unload and delete should succeed for ns2 on other broker with a redirect
        // otheradmin.namespaces().unload("prop-xyz/use/ns2");
    }

    @Test(dataProvider = "topicName")
    public void persistentTopics(String topicName) throws Exception {
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());

        final String persistentTopicName = "persistent://prop-xyz/use/ns1/" + topicName;
        // Force to create a destination
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/" + topicName, 0);
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"),
                Lists.newArrayList("persistent://prop-xyz/use/ns1/" + topicName));

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe(persistentTopicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(persistentTopicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/" + topicName, 10);

        PersistentTopicStats topicStats = admin.persistentTopics().getStats(persistentTopicName);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
        assertEquals(topicStats.subscriptions.get("my-sub").consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 10);
        assertEquals(topicStats.publishers.size(), 0);

        PersistentTopicInternalStats internalStats = admin.persistentTopics().getInternalStats(persistentTopicName);
        assertEquals(internalStats.cursors.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));

        List<Message> messages = admin.persistentTopics().peekMessages(persistentTopicName, "my-sub", 3);
        assertEquals(messages.size(), 3);
        for (int i = 0; i < 3; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        messages = admin.persistentTopics().peekMessages(persistentTopicName, "my-sub", 15);
        assertEquals(messages.size(), 10);
        for (int i = 0; i < 10; i++) {
            String expectedMessage = "message-" + i;
            assertEquals(messages.get(i).getData(), expectedMessage.getBytes());
        }

        admin.persistentTopics().skipMessages(persistentTopicName, "my-sub", 5);
        topicStats = admin.persistentTopics().getStats(persistentTopicName);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 5);

        admin.persistentTopics().skipAllMessages(persistentTopicName, "my-sub");
        topicStats = admin.persistentTopics().getStats(persistentTopicName);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 0);

        consumer.close();
        client.close();

        admin.persistentTopics().deleteSubscription(persistentTopicName, "my-sub");

        assertEquals(admin.persistentTopics().getSubscriptions(persistentTopicName), Lists.newArrayList());
        topicStats = admin.persistentTopics().getStats(persistentTopicName);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet());
        assertEquals(topicStats.publishers.size(), 0);

        try {
            admin.persistentTopics().skipAllMessages(persistentTopicName, "my-sub");
        } catch (NotFoundException e) {
        }

        admin.persistentTopics().delete(persistentTopicName);

        try {
            admin.persistentTopics().delete(persistentTopicName);
            fail("Should have received 404");
        } catch (NotFoundException e) {
        }

        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());
    }

    @Test(dataProvider = "topicName")
    public void partitionedTopics(String topicName) throws Exception {
        final String partitionedTopicName = "persistent://prop-xyz/use/ns1/" + topicName;
        admin.persistentTopics().createPartitionedTopic(partitionedTopicName, 4);

        assertEquals(admin.persistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 4);

        // check if the virtual topic doesn't get created
        List<String> destinations = admin.persistentTopics().getList("prop-xyz/use/ns1");
        assertEquals(destinations.size(), 0);

        assertEquals(
                admin.persistentTopics().getPartitionedTopicMetadata("persistent://prop-xyz/use/ns1/ds2").partitions,
                0);

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe(partitionedTopicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(partitionedTopicName), Lists.newArrayList("my-sub"));

        Consumer consumer1 = client.subscribe(partitionedTopicName, "my-sub-1", conf);

        assertEquals(Sets.newHashSet(admin.persistentTopics().getSubscriptions(partitionedTopicName)),
                Sets.newHashSet("my-sub", "my-sub-1"));

        consumer1.close();
        admin.persistentTopics().deleteSubscription(partitionedTopicName, "my-sub-1");
        assertEquals(admin.persistentTopics().getSubscriptions(partitionedTopicName), Lists.newArrayList("my-sub"));

        ProducerConfiguration prodConf = new ProducerConfiguration();
        prodConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = client.createProducer(partitionedTopicName, prodConf);

        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        assertEquals(Sets.newHashSet(admin.persistentTopics().getList("prop-xyz/use/ns1")),
                Sets.newHashSet(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));

        // test cumulative stats for partitioned topic
        PartitionedTopicStats topicStats = admin.persistentTopics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.subscriptions.keySet(), Sets.newTreeSet(Lists.newArrayList("my-sub")));
        assertEquals(topicStats.subscriptions.get("my-sub").consumers.size(), 1);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 10);
        assertEquals(topicStats.publishers.size(), 1);
        assertEquals(topicStats.partitions, Maps.newHashMap());

        // test per partition stats for partitioned topic
        topicStats = admin.persistentTopics().getPartitionedStats(partitionedTopicName, true);
        assertEquals(topicStats.metadata.partitions, 4);
        assertEquals(topicStats.partitions.keySet(),
                Sets.newHashSet(partitionedTopicName + "-partition-0", partitionedTopicName + "-partition-1",
                        partitionedTopicName + "-partition-2", partitionedTopicName + "-partition-3"));
        PersistentTopicStats partitionStats = topicStats.partitions.get(partitionedTopicName + "-partition-0");
        assertEquals(partitionStats.publishers.size(), 1);
        assertEquals(partitionStats.subscriptions.get("my-sub").consumers.size(), 1);
        assertEquals(partitionStats.subscriptions.get("my-sub").msgBacklog, 3, 1);

        try {
            admin.persistentTopics().skipMessages(partitionedTopicName, "my-sub", 5);
            fail("skip messages for partitioned topics should fail");
        } catch (Exception e) {
            // ok
        }

        admin.persistentTopics().skipAllMessages(partitionedTopicName, "my-sub");
        topicStats = admin.persistentTopics().getPartitionedStats(partitionedTopicName, false);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 0);

        producer.close();
        consumer.close();

        admin.persistentTopics().deleteSubscription(partitionedTopicName, "my-sub");

        assertEquals(admin.persistentTopics().getSubscriptions(partitionedTopicName), Lists.newArrayList());

        try {
            admin.persistentTopics().createPartitionedTopic(partitionedTopicName, 32);
            fail("Should have failed as the partitioned topic already exists");
        } catch (ConflictException ce) {
        }

        producer = client.createProducer(partitionedTopicName);

        destinations = admin.persistentTopics().getList("prop-xyz/use/ns1");
        assertEquals(destinations.size(), 4);

        try {
            admin.persistentTopics().deletePartitionedTopic(partitionedTopicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException pfe) {
            // ok
        }

        producer.close();
        client.close();

        admin.persistentTopics().deletePartitionedTopic(partitionedTopicName);

        assertEquals(admin.persistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 0);

        admin.persistentTopics().createPartitionedTopic(partitionedTopicName, 32);

        assertEquals(admin.persistentTopics().getPartitionedTopicMetadata(partitionedTopicName).partitions, 32);

        try {
            admin.persistentTopics().deletePartitionedTopic("persistent://prop-xyz/use/ns1/ds2");
            fail("Should have failed as the partitioned topic was not created");
        } catch (NotFoundException nfe) {
        }

        admin.persistentTopics().deletePartitionedTopic(partitionedTopicName);

        // delete a partitioned topic in a global namespace
        admin.persistentTopics().createPartitionedTopic(partitionedTopicName, 4);
        admin.persistentTopics().deletePartitionedTopic(partitionedTopicName);
    }

    @Test(dataProvider = "numBundles")
    public void testDeleteNamespaceBundle(Integer numBundles) throws Exception {
        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // since we have 2 brokers running, we try to let both of them acquire bundle ownership
        admin.lookups().lookupDestination("persistent://prop-xyz/use/ns1-bundles/ds1");
        admin.lookups().lookupDestination("persistent://prop-xyz/use/ns1-bundles/ds2");
        admin.lookups().lookupDestination("persistent://prop-xyz/use/ns1-bundles/ds3");
        admin.lookups().lookupDestination("persistent://prop-xyz/use/ns1-bundles/ds4");

        assertEquals(admin.namespaces().getDestinations("prop-xyz/use/ns1-bundles"), Lists.newArrayList());

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1-bundles");
        assertEquals(admin.namespaces().getNamespaces("prop-xyz", "use"), Lists.newArrayList());
    }

    @Test
    public void testNamespaceSplitBundle() throws Exception {
        // Force to create a destination
        final String namespace = "prop-xyz/use/ns1";
        final String topicName = (new StringBuilder("persistent://")).append(namespace).append("/ds2").toString();
        Producer producer = pulsarClient.createProducer(topicName);
        producer.send("message".getBytes());
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.persistentTopics().getList(namespace), Lists.newArrayList(topicName));

        try {
            admin.namespaces().splitNamespaceBundle(namespace, "0x00000000_0xffffffff");
        } catch (Exception e) {
            fail("split bundle shouldn't have thrown exception");
        }

        // bundle-factory cache must have updated split bundles
        NamespaceBundles bundles = bundleFactory.getBundles(new NamespaceName(namespace));
        String[] splitRange = { namespace + "/0x00000000_0x7fffffff", namespace + "/0x7fffffff_0xffffffff" };
        for (int i = 0; i < bundles.getBundles().size(); i++) {
            assertEquals(bundles.getBundles().get(i).toString(), splitRange[i]);
        }

        producer.close();
    }

    @Test
    public void testNamespaceUnloadBundle() throws Exception {
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());

        // Force to create a destination
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 0);
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"),
                Lists.newArrayList("persistent://prop-xyz/use/ns1/ds2"));

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe("persistent://prop-xyz/use/ns1/ds2", "my-sub", conf);
        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1/ds2"),
                Lists.newArrayList("my-sub"));

        // Create producer
        Producer producer = pulsarClient.createProducer("persistent://prop-xyz/use/ns1/ds2");
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
        NamespaceBundle bundle = bundleFactory.getBundle(new NamespaceName("prop-xyz/use/ns1"),
                Range.range(0L, BoundType.CLOSED, 0xffffffffL, BoundType.CLOSED));
        assertFalse(pulsar.getNamespaceService().isServiceUnitOwned(bundle));
        assertFalse(otherPulsar.getNamespaceService().isServiceUnitOwned(bundle));
        pulsarClient.shutdown();

        LOG.info("--- RELOAD ---");

        // Force reload of namespace and wait for topic to be ready
        for (int i = 0; i < 30; i++) {
            try {
                admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1/ds2");
                break;
            } catch (PulsarAdminException e) {
                LOG.warn("Failed to get topic stats.. {}", e.getMessage());
                Thread.sleep(1000);
            }
        }

        admin.persistentTopics().deleteSubscription("persistent://prop-xyz/use/ns1/ds2", "my-sub");
        admin.persistentTopics().delete("persistent://prop-xyz/use/ns1/ds2");
    }

    @Test(dataProvider = "numBundles")
    public void testNamespaceBundleUnload(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1-bundles"), Lists.newArrayList());

        // Force to create a destination
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1-bundles/ds2", 0);
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1-bundles"),
                Lists.newArrayList("persistent://prop-xyz/use/ns1-bundles/ds2"));

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        Consumer consumer = pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub", conf);
        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub"));

        // Create producer
        Producer producer = pulsarClient.createProducer("persistent://prop-xyz/use/ns1-bundles/ds2");
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        NamespaceBundle bundle = (NamespaceBundle) pulsar.getNamespaceService()
                .getBundle(DestinationName.get("persistent://prop-xyz/use/ns1-bundles/ds2"));

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
                admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2");
                break;
            } catch (PulsarAdminException e) {
                LOG.warn("Failed to get topic stats.. {}", e.getMessage());
                Thread.sleep(1000);
            }
        }

        admin.persistentTopics().deleteSubscription("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub");
        admin.persistentTopics().delete("persistent://prop-xyz/use/ns1-bundles/ds2");
    }

    @Test(dataProvider = "bundling")
    public void testClearBacklogOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // create consumer and subscription
        pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub");
        pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub-1");
        pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub-2");
        pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds1", "my-sub");
        pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds1", "my-sub-1");

        // Create producer
        Producer producer = pulsarClient.createProducer("persistent://prop-xyz/use/ns1-bundles/ds2");
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();

        // Create producer
        Producer producer1 = pulsarClient.createProducer("persistent://prop-xyz/use/ns1-bundles/ds1");
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer1.send(message.getBytes());
        }

        producer1.close();

        admin.namespaces().clearNamespaceBacklogForSubscription("prop-xyz/use/ns1-bundles", "my-sub");

        long backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").subscriptions
                .get("my-sub").msgBacklog;
        assertEquals(backlog, 0);
        backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").subscriptions
                .get("my-sub").msgBacklog;
        assertEquals(backlog, 0);
        backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").subscriptions
                .get("my-sub-1").msgBacklog;
        assertEquals(backlog, 10);

        admin.namespaces().clearNamespaceBacklog("prop-xyz/use/ns1-bundles");

        backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds1").subscriptions
                .get("my-sub-1").msgBacklog;
        assertEquals(backlog, 0);
        backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").subscriptions
                .get("my-sub-1").msgBacklog;
        assertEquals(backlog, 0);
        backlog = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1-bundles/ds2").subscriptions
                .get("my-sub-2").msgBacklog;
        assertEquals(backlog, 0);
    }

    @Test(dataProvider = "bundling")
    public void testUnsubscribeOnNamespace(Integer numBundles) throws Exception {
        admin.namespaces().createNamespace("prop-xyz/use/ns1-bundles", numBundles);

        // create consumer and subscription
        Consumer consumer1 = pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub");
        Consumer consumer2 = pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub-1");
        /* Consumer consumer3 = */ pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds2", "my-sub-2");
        Consumer consumer4 = pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds1", "my-sub");
        Consumer consumer5 = pulsarClient.subscribe("persistent://prop-xyz/use/ns1-bundles/ds1", "my-sub-1");

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

        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub-1", "my-sub-2"));
        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds1"),
                Lists.newArrayList("my-sub-1"));

        consumer2.close();
        consumer5.close();

        admin.namespaces().unsubscribeNamespace("prop-xyz/use/ns1-bundles", "my-sub-1");

        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds2"),
                Lists.newArrayList("my-sub-2"));
        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1-bundles/ds1"),
                Lists.newArrayList());
    }

    long messageTimestamp = System.currentTimeMillis();
    long secondTimestamp = System.currentTimeMillis();

    private void publishMessagesOnPersistentTopic(String topicName, int messages) throws Exception {
        publishMessagesOnPersistentTopic(topicName, messages, 0);
    }

    private void publishMessagesOnPersistentTopic(String topicName, int messages, int startIdx) throws Exception {
        Producer producer = pulsarClient.createProducer(topicName);

        for (int i = startIdx; i < (messages + startIdx); i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }

        producer.close();
    }

    @Test
    public void backlogQuotas() throws Exception {
        assertEquals(admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1"), Maps.newTreeMap());

        Map<BacklogQuotaType, BacklogQuota> quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 0);
        assertEquals(quotaMap.get(BacklogQuotaType.destination_storage), null);

        admin.namespaces().setBacklogQuota("prop-xyz/use/ns1",
                new BacklogQuota(1 * 1024 * 1024 * 1024, RetentionPolicy.producer_exception));
        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 1);
        assertEquals(quotaMap.get(BacklogQuotaType.destination_storage),
                new BacklogQuota(1 * 1024 * 1024 * 1024, RetentionPolicy.producer_exception));

        admin.namespaces().removeBacklogQuota("prop-xyz/use/ns1");

        quotaMap = admin.namespaces().getBacklogQuotaMap("prop-xyz/use/ns1");
        assertEquals(quotaMap.size(), 0);
        assertEquals(quotaMap.get(BacklogQuotaType.destination_storage), null);
    }

    @Test
    public void statsOnNonExistingDestinations() throws Exception {
        try {
            admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1/ghostTopic");
            fail("The topic doesn't exist");
        } catch (NotFoundException e) {
            // OK
        }
    }

    @Test
    public void testDeleteFailedReturnCode() throws Exception {
        String topicName = "persistent://prop-xyz/use/ns1/my-topic";
        Producer producer = pulsarClient.createProducer(topicName);

        try {
            admin.persistentTopics().delete(topicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // OK
        }

        producer.close();

        Consumer consumer = pulsarClient.subscribe(topicName, "sub");

        try {
            admin.persistentTopics().delete(topicName);
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // OK
        }

        try {
            admin.persistentTopics().deleteSubscription(topicName, "sub");
            fail("The topic is busy");
        } catch (PreconditionFailedException e) {
            // Ok
        }

        consumer.close();

        // Now should succeed
        admin.persistentTopics().delete(topicName);
    }

    private static class IncompatiblePropertyAdmin {
        public Set<String> allowedClusters;
        public int someNewIntField;
        public String someNewString;
    }

    @Test
    public void testJacksonWithTypeDifferencies() throws Exception {
        String expectedJson = "{\"adminRoles\":[\"role1\",\"role2\"],\"allowedClusters\":[\"usw\",\"use\"]}";
        IncompatiblePropertyAdmin r1 = ObjectMapperFactory.getThreadLocal().reader(IncompatiblePropertyAdmin.class)
                .readValue(expectedJson);
        assertEquals(r1.allowedClusters, Sets.newHashSet("use", "usw"));
        assertEquals(r1.someNewIntField, 0);
        assertEquals(r1.someNewString, null);
    }

    @Test
    public void testBackwardCompatiblity() throws Exception {
        assertEquals(admin.properties().getProperties(), Lists.newArrayList("prop-xyz"));
        assertEquals(admin.properties().getPropertyAdmin("prop-xyz").getAdminRoles(),
                Lists.newArrayList("role1", "role2"));
        assertEquals(admin.properties().getPropertyAdmin("prop-xyz").getAllowedClusters(), Sets.newHashSet("use"));

        // Try to deserialize property JSON with IncompatiblePropertyAdmin format
        // it should succeed ignoring missing fields
        PropertiesImpl properties = (PropertiesImpl) admin.properties();
        IncompatiblePropertyAdmin result = properties.request(properties.getWebTarget().path("prop-xyz"))
                .get(IncompatiblePropertyAdmin.class);

        assertEquals(result.allowedClusters, Sets.newHashSet("use"));
        assertEquals(result.someNewIntField, 0);
        assertEquals(result.someNewString, null);

        admin.namespaces().deleteNamespace("prop-xyz/use/ns1");
        admin.properties().deleteProperty("prop-xyz");
        assertEquals(admin.properties().getProperties(), Lists.newArrayList());
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());

        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long messageTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 5, 5);

        List<Message> messages = admin.persistentTopics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
        }
        // messages should still be available due to retention

        admin.persistentTopics().resetCursor(topicName, "my-sub", messageTimestamp);

        int receivedAfterReset = 0;

        for (int i = 4; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(message.getData(), expected.getBytes());
        }
        assertEquals(receivedAfterReset, 6);

        consumer.close();

        admin.persistentTopics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList());
        admin.persistentTopics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void persistentTopicsCursorResetAfterReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());

        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);

        // Allow at least 1ms for messages to have different timestamps
        Thread.sleep(1);
        long firstTimestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 3, 5);

        Thread.sleep(1);
        long secondTimestamp = System.currentTimeMillis();

        publishMessagesOnPersistentTopic(topicName, 2, 8);

        List<Message> messages = admin.persistentTopics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);
        messages.forEach(message -> {
            LOG.info("Peeked message: {}", new String(message.getData()));
        });

        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
        }

        admin.persistentTopics().resetCursor(topicName, "my-sub", firstTimestamp);

        int receivedAfterReset = 0;

        // Should received messages from 4-9
        for (int i = 4; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(new String(message.getData()), expected);
        }
        assertEquals(receivedAfterReset, 6);

        // Reset at 2nd timestamp
        receivedAfterReset = 0;
        admin.persistentTopics().resetCursor(topicName, "my-sub", secondTimestamp);

        // Should received messages from 7-9
        for (int i = 7; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
            ++receivedAfterReset;
            String expected = "message-" + i;
            assertEquals(new String(message.getData()), expected);
        }
        assertEquals(receivedAfterReset, 3);

        consumer.close();
        admin.persistentTopics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList());
        admin.persistentTopics().delete(topicName);
    }

    @Test(dataProvider = "topicName")
    public void partitionedTopicsCursorReset(String topicName) throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));
        topicName = "persistent://prop-xyz/use/ns1/" + topicName;

        admin.persistentTopics().createPartitionedTopic(topicName, 4);

        // create consumer and subscription
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = pulsarClient.subscribe(topicName, "my-sub", conf);

        List<String> destinations = admin.persistentTopics().getList("prop-xyz/use/ns1");
        assertEquals(destinations.size(), 4);

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 5, 0);
        Thread.sleep(1);

        long timestamp = System.currentTimeMillis();
        publishMessagesOnPersistentTopic(topicName, 5, 5);

        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
        }
        // messages should still be available due to retention

        admin.persistentTopics().resetCursor(topicName, "my-sub", timestamp);

        Set<String> expectedMessages = Sets.newHashSet();
        Set<String> receivedMessages = Sets.newHashSet();
        for (int i = 4; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
            expectedMessages.add("message-" + i);
            receivedMessages.add(new String(message.getData()));
        }

        receivedMessages.removeAll(expectedMessages);
        assertEquals(receivedMessages.size(), 0);

        consumer.close();
        admin.persistentTopics().deleteSubscription(topicName, "my-sub");
        admin.persistentTopics().deletePartitionedTopic(topicName);
    }

    @Test
    public void persistentTopicsInvalidCursorReset() throws Exception {
        admin.namespaces().setRetention("prop-xyz/use/ns1", new RetentionPolicies(10, 10));

        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList());

        String topicName = "persistent://prop-xyz/use/ns1/invalidcursorreset";
        // Force to create a destination
        publishMessagesOnPersistentTopic(topicName, 0);
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"), Lists.newArrayList(topicName));

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe(topicName, "my-sub", conf);

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList("my-sub"));

        publishMessagesOnPersistentTopic(topicName, 10);

        List<Message> messages = admin.persistentTopics().peekMessages(topicName, "my-sub", 10);
        assertEquals(messages.size(), 10);

        for (int i = 0; i < 10; i++) {
            Message message = consumer.receive();
            consumer.acknowledge(message);
        }
        // use invalid timestamp
        try {
            admin.persistentTopics().resetCursor(topicName, "my-sub", System.currentTimeMillis() - 190000);
        } catch (Exception e) {
            // fail the test
            throw e;
        }

        admin.persistentTopics().resetCursor(topicName, "my-sub", System.currentTimeMillis() + 90000);
        consumer = client.subscribe(topicName, "my-sub", conf);
        consumer.close();
        client.close();

        admin.persistentTopics().deleteSubscription(topicName, "my-sub");

        assertEquals(admin.persistentTopics().getSubscriptions(topicName), Lists.newArrayList());
        admin.persistentTopics().delete(topicName);
    }

    @Test
    public void testObjectWithUnknowProperties() {

        class CustomPropertyAdmin extends PropertyAdmin {
            @SuppressWarnings("unused")
            public int newProperty;
        }

        PropertyAdmin pa = new PropertyAdmin(Lists.newArrayList("test_appid1", "test_appid2"), Sets.newHashSet("use"));
        CustomPropertyAdmin cpa = new CustomPropertyAdmin();
        cpa.setAdminRoles(pa.getAdminRoles());
        cpa.setAllowedClusters(pa.getAllowedClusters());
        cpa.newProperty = 100;

        try {
            admin.properties().createProperty("test-property", cpa);
        } catch (Exception e) {
            fail("Should not happen.");
        }
    }

    /**
     * Verify: PersistentTopics.expireMessages()/expireMessagesForAllSubscriptions()
     * 1. Created multiple shared subscriptions and publisher on topic
     * 2. Publish messages on the topic
     * 3. expire message on sub-1 : backlog for sub-1 must be 0
     * 4. expire message on all subscriptions: backlog for all subscription must be 0
     *
     * @throws Exception
     */
    @Test
    public void testPersistentTopicsExpireMessages() throws Exception{

        // Force to create a destination
        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 0);
        assertEquals(admin.persistentTopics().getList("prop-xyz/use/ns1"),
                Lists.newArrayList("persistent://prop-xyz/use/ns1/ds2"));

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        Consumer consumer1 = client.subscribe("persistent://prop-xyz/use/ns1/ds2", "my-sub1", conf);
        Consumer consumer2 = client.subscribe("persistent://prop-xyz/use/ns1/ds2", "my-sub2", conf);
        Consumer consumer3 = client.subscribe("persistent://prop-xyz/use/ns1/ds2", "my-sub3", conf);

        assertEquals(admin.persistentTopics().getSubscriptions("persistent://prop-xyz/use/ns1/ds2").size(), 3);

        publishMessagesOnPersistentTopic("persistent://prop-xyz/use/ns1/ds2", 10);

        PersistentTopicStats topicStats = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.subscriptions.get("my-sub1").msgBacklog, 10);
        assertEquals(topicStats.subscriptions.get("my-sub2").msgBacklog, 10);
        assertEquals(topicStats.subscriptions.get("my-sub3").msgBacklog, 10);

        Thread.sleep(1000); // wait for 1 seconds to expire message
        admin.persistentTopics().expireMessages("persistent://prop-xyz/use/ns1/ds2", "my-sub1", 1);
        Thread.sleep(1000); // wait for 1 seconds to execute expire message as it is async

        topicStats = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.subscriptions.get("my-sub1").msgBacklog, 0);
        assertEquals(topicStats.subscriptions.get("my-sub2").msgBacklog, 10);
        assertEquals(topicStats.subscriptions.get("my-sub3").msgBacklog, 10);

        admin.persistentTopics().expireMessagesForAllSubscriptions("persistent://prop-xyz/use/ns1/ds2", 1);
        Thread.sleep(1000); // wait for 1 seconds to execute expire message as it is async

        topicStats = admin.persistentTopics().getStats("persistent://prop-xyz/use/ns1/ds2");
        assertEquals(topicStats.subscriptions.get("my-sub1").msgBacklog, 0);
        assertEquals(topicStats.subscriptions.get("my-sub2").msgBacklog, 0);
        assertEquals(topicStats.subscriptions.get("my-sub3").msgBacklog, 0);

        consumer1.close();
        consumer2.close();
        consumer3.close();

    }

    /**
     * Verify: PersistentTopics.expireMessages()/expireMessagesForAllSubscriptions() for PartitionTopic
     *
     * @throws Exception
     */
    @Test
    public void testPersistentTopicExpireMessageOnParitionTopic() throws Exception{

        admin.persistentTopics().createPartitionedTopic("persistent://prop-xyz/use/ns1/ds1", 4);

        // create consumer and subscription
        URL pulsarUrl = new URL("http://127.0.0.1" + ":" + BROKER_WEBSERVICE_PORT);
        ClientConfiguration clientConf = new ClientConfiguration();
        clientConf.setStatsInterval(0, TimeUnit.SECONDS);
        PulsarClient client = PulsarClient.create(pulsarUrl.toString(), clientConf);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe("persistent://prop-xyz/use/ns1/ds1", "my-sub", conf);

        ProducerConfiguration prodConf = new ProducerConfiguration();
        prodConf.setMessageRoutingMode(MessageRoutingMode.RoundRobinPartition);
        Producer producer = client.createProducer("persistent://prop-xyz/use/ns1/ds1", prodConf);
        for (int i = 0; i < 10; i++) {
            String message = "message-" + i;
            producer.send(message.getBytes());
        }


        PartitionedTopicStats topicStats = admin.persistentTopics()
                .getPartitionedStats("persistent://prop-xyz/use/ns1/ds1", true);
        assertEquals(topicStats.subscriptions.get("my-sub").msgBacklog, 10);

        PersistentTopicStats partitionStatsPartition0 = topicStats.partitions
                .get("persistent://prop-xyz/use/ns1/ds1-partition-0");
        PersistentTopicStats partitionStatsPartition1 = topicStats.partitions
                .get("persistent://prop-xyz/use/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.subscriptions.get("my-sub").msgBacklog, 3, 1);
        assertEquals(partitionStatsPartition1.subscriptions.get("my-sub").msgBacklog, 3, 1);

        Thread.sleep(1000);
        admin.persistentTopics().expireMessagesForAllSubscriptions("persistent://prop-xyz/use/ns1/ds1", 1);
        Thread.sleep(1000);

        topicStats = admin.persistentTopics()
                .getPartitionedStats("persistent://prop-xyz/use/ns1/ds1", true);
        partitionStatsPartition0 = topicStats.partitions
                .get("persistent://prop-xyz/use/ns1/ds1-partition-0");
        partitionStatsPartition1 = topicStats.partitions
                .get("persistent://prop-xyz/use/ns1/ds1-partition-1");
        assertEquals(partitionStatsPartition0.subscriptions.get("my-sub").msgBacklog, 0);
        assertEquals(partitionStatsPartition1.subscriptions.get("my-sub").msgBacklog, 0);

        producer.close();
        consumer.close();
        client.close();

    }

    /**
     * This test-case verifies that broker should support both url/uri encoding for topic-name. It calls below api with
     * url-encoded and also uri-encoded topic-name in http request: a. PartitionedMetadataLookup b. TopicLookup c. Topic
     * Stats
     * 
     * @param topicName
     * @throws Exception
     */
    @Test(dataProvider = "topicName")
    public void testPulsarAdminForUriAndUrlEncoding(String topicName) throws Exception {
        final String ns1 = "prop-xyz/use/ns1";
        final String dn1 = "persistent://" + ns1 + "/" + topicName;
        final String urlEncodedTopic = Codec.encode(topicName);
        final String uriEncodedTopic = urlEncodedTopic.replaceAll("\\+", "%20");
        final int numOfPartitions = 4;
        admin.persistentTopics().createPartitionedTopic(dn1, numOfPartitions);
        // Create a consumer to get stats on this topic
        Consumer consumer1 = pulsarClient.subscribe(dn1, "my-subscriber-name", new ConsumerConfiguration());

        PersistentTopicsImpl persistent = (PersistentTopicsImpl) admin.persistentTopics();
        Field field = PersistentTopicsImpl.class.getDeclaredField("persistentTopics");
        field.setAccessible(true);
        WebTarget persistentTopics = (WebTarget) field.get(persistent);

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
                        Assert.fail(e.getMessage());
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
        final CompletableFuture<PersistentTopicStats> urlStats = new CompletableFuture<>();
        // (a) Url encoding
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(urlEncodedTopic + "-partition-1").path("stats"),
                new InvocationCallback<PersistentTopicStats>() {
                    @Override
                    public void completed(PersistentTopicStats response) {
                        urlStats.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        urlStats.completeExceptionally(e);
                    }
                });
        // (b) Uri encoding
        final CompletableFuture<PersistentTopicStats> uriStats = new CompletableFuture<>();
        persistent.asyncGetRequest(persistentTopics.path(ns1).path(uriEncodedTopic + "-partition-1").path("stats"),
                new InvocationCallback<PersistentTopicStats>() {
                    @Override
                    public void completed(PersistentTopicStats response) {
                        uriStats.complete(response);
                    }

                    @Override
                    public void failed(Throwable e) {
                        uriStats.completeExceptionally(e);
                    }
                });
        assertEquals(urlStats.get().subscriptions.size(), 1);
        assertEquals(uriStats.get().subscriptions.size(), 1);
    }

}
