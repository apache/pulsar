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

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.MetadataSessionExpiredPolicy;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateChannelImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateMetadataStoreTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.channel.ServiceUnitStateTableViewImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.LookupService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

public abstract class ExtensibleLoadManagerImplBaseTest extends MockedPulsarServiceBaseTest {

    final String caCertPath = Resources.getResource("certificate-authority/certs/ca.cert.pem").getPath();
    final String brokerCertPath =
            Resources.getResource("certificate-authority/server-keys/broker.cert.pem").getPath();
    final String brokerKeyPath =
            Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").getPath();

    protected PulsarService pulsar1;
    protected PulsarService pulsar2;

    protected PulsarTestContext additionalPulsarTestContext;

    protected ExtensibleLoadManagerImpl primaryLoadManager;

    protected ExtensibleLoadManagerImpl secondaryLoadManager;

    protected ServiceUnitStateChannelImpl channel1;
    protected ServiceUnitStateChannelImpl channel2;

    protected final String defaultTestNamespace;

    protected LookupService lookupService;

    protected String serviceUnitStateTableViewClassName;

    @Override
    protected ServiceConfiguration getDefaultConf() {
        ServiceConfiguration conf = super.getDefaultConf();
        conf.setZookeeperSessionExpiredPolicy(MetadataSessionExpiredPolicy.shutdown);
        return conf;
    }

    /**
     * Create fresh PulsarClient instances for use within a single test method.
     * Each test creates and closes its own clients to avoid shared mutable state
     * that causes "Client already closed" flakiness.
     */
    protected List<PulsarClient> createTestClients(int count) throws Exception {
        List<PulsarClient> testClients = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            testClients.add(pulsarClient(lookupUrl.toString(), 100));
        }
        return testClients;
    }

    protected static void closeTestClients(List<PulsarClient> testClients) {
        for (PulsarClient client : testClients) {
            try {
                client.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @DataProvider(name = "serviceUnitStateTableViewClassName")
    public static Object[][] serviceUnitStateTableViewClassName() {
        return new Object[][]{
                {ServiceUnitStateTableViewImpl.class.getName()},
                {ServiceUnitStateMetadataStoreTableViewImpl.class.getName()}
        };
    }

    protected ExtensibleLoadManagerImplBaseTest(String defaultTestNamespace,
                                                String serviceUnitStateTableViewClassName) {
        this.defaultTestNamespace = defaultTestNamespace;
        this.serviceUnitStateTableViewClassName = serviceUnitStateTableViewClassName;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        updateConfig(conf);
    }


    protected ServiceConfiguration updateConfig(ServiceConfiguration conf) {
        conf.setForceDeleteNamespaceAllowed(true);
        conf.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        conf.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        conf.setLoadManagerServiceUnitStateTableViewClassName(serviceUnitStateTableViewClassName);
        conf.setLoadBalancerReportUpdateMaxIntervalMinutes(1);
        conf.setLoadBalancerSheddingEnabled(false);
        conf.setLoadBalancerDebugModeEnabled(true);
        conf.setWebServicePortTls(Optional.of(0));
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setTlsCertificateFilePath(brokerCertPath);
        conf.setTlsKeyFilePath(brokerKeyPath);
        conf.setTlsTrustCertsFilePath(caCertPath);
        return conf;
    }

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        super.internalSetup(conf);
        pulsar1 = pulsar;
        var conf2 = updateConfig(getDefaultConf());
        additionalPulsarTestContext = createAdditionalPulsarTestContext(conf2);
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
                Sets.newHashSet(this.conf.getClusterName()), false);

        admin.namespaces().createNamespace(defaultTestNamespace, 128);
        admin.namespaces().setNamespaceReplicationClusters(defaultTestNamespace,
                Sets.newHashSet(this.conf.getClusterName()), false);
        lookupService = (LookupService) FieldUtils.readDeclaredField(pulsarClient, "lookup", true);
    }

    @SuppressWarnings("deprecation")
    private static PulsarClient pulsarClient(String url, int intervalInMillis) throws PulsarClientException {
        return
                PulsarClient.builder()
                        .serviceUrl(url)
                        .statsInterval(intervalInMillis, TimeUnit.MILLISECONDS).build();
    }


    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        if (additionalPulsarTestContext != null) {
            additionalPulsarTestContext.close();
            additionalPulsarTestContext = null;
        }
        super.internalCleanup();
        pulsar1 = pulsar2 = null;
        primaryLoadManager = secondaryLoadManager = null;
        channel1 = channel2 = null;
        lookupService = null;
    }

    @BeforeMethod(alwaysRun = true)
    protected void initializeState() throws Exception {
        // Reset to a clean state before each test: reconcile each broker's role with the channel
        // ownership and unload the test namespace so no bundle ownership carries over. The unload
        // publishes a state change on the channel system topic.
        //
        // A prior role-churning test (e.g. the direct playLeader()/playFollower() calls in
        // testRoleChangeIdempotency) can leave the channel system topic owned by a broker that no
        // longer serves it ("not served by this instance, redo the lookup"), with the channel
        // producer stuck in escalating reconnect backoff, so the unload's channel publish keeps
        // failing. Each unload attempt force-serves the channel topic (an admin lookup re-assigns
        // the pulsar/system bundle and getStats makes the owner load it); if that still does not
        // recover, force a clean channel owner via leader re-election (which reassigns and
        // re-serves the channel topic and makes clients redo their lookups) and retry.
        try {
            awaitTestNamespaceUnloaded(30);
        } catch (ConditionTimeoutException channelWedged) {
            recoverChannelOwnership();
            awaitTestNamespaceUnloaded(60);
        }
        reset(primaryLoadManager, secondaryLoadManager);
        FieldUtils.writeDeclaredField(pulsarClient, "lookup", lookupService, true);
        pulsar1.getConfig().setLoadBalancerMultiPhaseBundleUnload(true);
        pulsar2.getConfig().setLoadBalancerMultiPhaseBundleUnload(true);
    }

    // Drive monitor() to reconcile roles and force-serve the channel topic (monitor() only
    // self-heals when there is *no* channel owner, not when an owner is recorded but the bundle is
    // not served), then unload. ignoreExceptions() retries transient channel-publish failures; each
    // unload attempt is bounded so a synchronous unload cannot block longer than the retry window.
    private void awaitTestNamespaceUnloaded(long atMostSeconds) {
        boolean systemTopicChannel =
                serviceUnitStateTableViewClassName.equals(ServiceUnitStateTableViewImpl.class.getName());
        Awaitility.await().atMost(atMostSeconds, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> {
                    primaryLoadManager.monitor();
                    secondaryLoadManager.monitor();
                    if (systemTopicChannel) {
                        admin.lookups().lookupTopic(ServiceUnitStateTableViewImpl.TOPIC);
                        admin.topics().getStats(ServiceUnitStateTableViewImpl.TOPIC);
                    }
                    admin.namespaces().unloadAsync(defaultTestNamespace).get(15, TimeUnit.SECONDS);
                });
    }

    /**
     * Force a clean channel owner via leader re-election. After heavy direct
     * playLeader()/playFollower() churn the channel system topic can be left owned by a broker
     * that no longer serves it, leaving the channel producer stuck on a stale lookup in
     * escalating reconnect backoff. Closing the current owner's LeaderElectionService moves
     * ownership to the other broker; its playLeader() re-creates and re-serves the channel
     * topic, and the ownership change makes clients redo their (stale) lookups.
     */
    private void recoverChannelOwnership() throws Exception {
        boolean pulsar1Owns;
        try {
            pulsar1Owns = channel1.isChannelOwner();
        } catch (Exception e) {
            // Owner can't be determined (e.g. no channel owner now); default to moving to pulsar2.
            pulsar1Owns = true;
        }
        PulsarService currentOwner = pulsar1Owns ? pulsar1 : pulsar2;
        ServiceUnitStateChannelImpl newOwnerChannel = pulsar1Owns ? channel2 : channel1;
        currentOwner.getLeaderElectionService().close();
        try {
            Awaitility.await().atMost(30, TimeUnit.SECONDS).ignoreExceptions()
                    .untilAsserted(() -> assertTrue(newOwnerChannel.isChannelOwner()));
        } catch (ConditionTimeoutException ignore) {
            // Best effort: the subsequent unload retry is the real backstop.
        } finally {
            currentOwner.getLeaderElectionService().start();
        }
    }

    protected void setPrimaryLoadManager() throws IllegalAccessException {
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

    protected static CompletableFuture<NamespaceBundle> getBundleAsync(PulsarService pulsar, TopicName topic) {
        return pulsar.getNamespaceService().getBundleAsync(topic);
    }

    protected Pair<TopicName, NamespaceBundle> getBundleIsNotOwnByChangeEventTopic(String topicNamePrefix)
            throws Exception {
        TopicName changeEventsTopicName =
                TopicName.get(defaultTestNamespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME);
        NamespaceBundle changeEventsBundle = getBundleAsync(pulsar1, changeEventsTopicName).get();
        int i = 0;
        while (true) {
            TopicName topicName = TopicName.get(defaultTestNamespace + "/" + topicNamePrefix + "-" + i);
            NamespaceBundle bundle = getBundleAsync(pulsar1, topicName).get();
            if (!bundle.equals(changeEventsBundle)) {
                return Pair.of(topicName, bundle);
            }
            i++;
        }
    }
}
