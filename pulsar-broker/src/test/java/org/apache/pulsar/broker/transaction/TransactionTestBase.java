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
package org.apache.pulsar.broker.transaction;

import static org.mockito.Mockito.spy;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.intercept.CounterBrokerInterceptor;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminBuilder;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.tests.TestRetrySupport;

@Slf4j
public abstract class TransactionTestBase extends TestRetrySupport {
    public static final String CLUSTER_NAME = "test";

    @Setter
    private int brokerCount = 3;
    @Getter
    private final List<ServiceConfiguration> serviceConfigurationList = new ArrayList<>();
    @Getter
    protected final List<PulsarService> pulsarServiceList = new ArrayList<>();
    protected List<PulsarTestContext> pulsarTestContexts = new ArrayList<>();

    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    public static final String TENANT = "tnx";
    protected static final String NAMESPACE1 = TENANT + "/ns1";
    protected ServiceConfiguration conf = new ServiceConfiguration();

    public void internalSetup() throws Exception {
        incrementSetupNumber();
        init();

        if (admin != null) {
            admin.close();
        }
        admin = spy(
                createNewPulsarAdmin(PulsarAdmin.builder().serviceHttpUrl(pulsarServiceList.get(0).getWebServiceAddress()))
        );

        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = PulsarClient.builder().serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl()).build();
    }

    private void init() throws Exception {
        startBroker();
    }

    protected PulsarClient createNewPulsarClient(ClientBuilder clientBuilder) throws PulsarClientException {
        return clientBuilder.build();
    }

    protected PulsarAdmin createNewPulsarAdmin(PulsarAdminBuilder builder) throws PulsarClientException {
        return builder.build();
    }

    protected void setUpBase(int numBroker,int numPartitionsOfTC, String topic, int numPartitions) throws Exception{
        setBrokerCount(numBroker);
        internalSetup();

        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:"
                + webServicePort).build());

        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        createTransactionCoordinatorAssign(numPartitionsOfTC);
        if (topic != null) {
            admin.tenants().createTenant(TENANT,
                    new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
            admin.namespaces().createNamespace(NAMESPACE1);
            if (numPartitions == 0) {
                admin.topics().createNonPartitionedTopic(topic);
            } else {
                admin.topics().createPartitionedTopic(topic, numPartitions);
            }
        }
        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
        pulsarClient = createNewPulsarClient(PulsarClient.builder()
                .serviceUrl(getPulsarServiceList().get(0).getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS)
                .enableTransaction(true));
    }

    protected void createTransactionCoordinatorAssign(int numPartitionsOfTC) throws MetadataStoreException {
        pulsarServiceList.get(0).getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(numPartitionsOfTC));
    }

    protected void startBroker() throws Exception {
        for (int i = 0; i < brokerCount; i++) {
            conf.setClusterName(CLUSTER_NAME);
            conf.setAdvertisedAddress("localhost");
            conf.setManagedLedgerCacheSizeMB(8);
            conf.setActiveConsumerFailoverDelayTimeMillis(0);
            conf.setDefaultNumberOfNamespaceBundles(1);
            conf.setMetadataStoreUrl("zk:localhost:2181");
            conf.setConfigurationMetadataStoreUrl("zk:localhost:3181");
            conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            conf.setBookkeeperClientExposeStatsToPrometheus(true);
            conf.setForceDeleteNamespaceAllowed(true);
            conf.setBrokerShutdownTimeoutMs(0L);
            conf.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            conf.setBrokerServicePort(Optional.of(0));
            conf.setBrokerServicePortTls(Optional.of(0));
            conf.setAdvertisedAddress("localhost");
            conf.setWebServicePort(Optional.of(0));
            conf.setWebServicePortTls(Optional.of(0));
            conf.setTransactionCoordinatorEnabled(true);
            conf.setBrokerDeduplicationEnabled(true);
            conf.setTransactionBufferSnapshotMaxTransactionCount(2);
            conf.setTransactionBufferSnapshotMinTimeInMillis(2000);
            serviceConfigurationList.add(conf);

            PulsarTestContext.Builder testContextBuilder =
                    PulsarTestContext.builder()
                            .brokerInterceptor(new CounterBrokerInterceptor())
                            .spyByDefault()
                            .config(conf);
            if (i > 0) {
                testContextBuilder.reuseMockBookkeeperAndMetadataStores(pulsarTestContexts.get(0));
            } else {
                testContextBuilder.withMockZookeeper();
            }
            PulsarTestContext pulsarTestContext = testContextBuilder
                    .build();
            PulsarService pulsar = pulsarTestContext.getPulsarService();
            pulsarServiceList.add(pulsar);
            pulsarTestContexts.add(pulsarTestContext);
        }
    }


    protected final void internalCleanup() {
        markCurrentSetupNumberCleaned();
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (pulsarClient != null) {
                pulsarClient.shutdown();
                pulsarClient = null;
            }
            if (pulsarTestContexts.size() > 0) {
                for(int i = pulsarTestContexts.size() - 1; i >= 0; i--) {
                    pulsarTestContexts.get(i).close();
                }
                pulsarTestContexts.clear();
            }
            pulsarServiceList.clear();
            if (serviceConfigurationList.size() > 0) {
                serviceConfigurationList.clear();
            }
        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
        }
    }

    /**
     * see {@link BrokerTestBase#deleteNamespaceWithRetry(String, boolean, PulsarAdmin, Collection)}
     */
    protected void deleteNamespaceWithRetry(String ns, boolean force)
            throws Exception {
        MockedPulsarServiceBaseTest.deleteNamespaceWithRetry(ns, force, admin, pulsarServiceList);
    }

    /**
     * see {@link MockedPulsarServiceBaseTest#deleteNamespaceWithRetry(String, boolean, PulsarAdmin, Collection)}
     */
    protected void deleteNamespaceWithRetry(String ns, boolean force, PulsarAdmin admin)
            throws Exception {
        MockedPulsarServiceBaseTest.deleteNamespaceWithRetry(ns, force, admin, pulsarServiceList);
    }
}
