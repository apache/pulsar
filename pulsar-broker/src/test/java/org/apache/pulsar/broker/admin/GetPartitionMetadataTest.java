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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.ClientCnx;
import org.apache.pulsar.client.impl.ConnectionPool;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
@Slf4j
public class GetPartitionMetadataTest extends TestRetrySupport {

    protected static final String DEFAULT_NS = "public/default";

    protected String clusterName = "c1";

    protected LocalBookkeeperEnsemble bkEnsemble;

    protected ServiceConfiguration conf = new ServiceConfiguration();

    protected PulsarService pulsar1;
    protected URL url1;
    protected PulsarAdmin admin1;
    protected PulsarClientImpl clientWithHttpLookup1;
    protected PulsarClientImpl clientWitBinaryLookup1;

    @Override
    @BeforeClass(alwaysRun = true)
    protected void setup() throws Exception {
        incrementSetupNumber();
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();
        // Start broker.
        setupBrokers();
        // Create default NS.
        admin1.clusters().createCluster(clusterName, new ClusterDataImpl());
        admin1.tenants().createTenant(NamespaceName.get(DEFAULT_NS).getTenant(),
                new TenantInfoImpl(Collections.emptySet(), Sets.newHashSet(clusterName)));
        admin1.namespaces().createNamespace(DEFAULT_NS);
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        cleanupBrokers();
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
    }

    protected void cleanupBrokers() throws Exception {
        // Cleanup broker2.
        if (clientWithHttpLookup1 != null) {
            clientWithHttpLookup1.close();
            clientWithHttpLookup1 = null;
        }
        if (clientWitBinaryLookup1 != null) {
            clientWitBinaryLookup1.close();
            clientWitBinaryLookup1 = null;
        }
        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }
        // Reset configs.
        conf = new ServiceConfiguration();
    }

    protected void setupBrokers() throws Exception {
        doInitConf();
        // Start broker.
        pulsar1 = new PulsarService(conf);
        pulsar1.start();
        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        clientWithHttpLookup1 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar1.getWebServiceAddress()).build();
        clientWitBinaryLookup1 =
                (PulsarClientImpl) PulsarClient.builder().serviceUrl(pulsar1.getBrokerServiceUrl()).build();
    }

    protected void doInitConf() {
        conf.setClusterName(clusterName);
        conf.setAdvertisedAddress("localhost");
        conf.setBrokerServicePort(Optional.of(0));
        conf.setWebServicePort(Optional.of(0));
        conf.setMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort());
        conf.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + bkEnsemble.getZookeeperPort() + "/foo");
        conf.setBrokerDeleteInactiveTopicsEnabled(false);
        conf.setBrokerShutdownTimeoutMs(0L);
        conf.setLoadBalancerSheddingEnabled(false);
    }

    protected PulsarClientImpl[] getClientsToTest() {
        return new PulsarClientImpl[] {clientWithHttpLookup1, clientWitBinaryLookup1};
    }

    protected PulsarClientImpl[] getClientsToTest(boolean isUsingHttpLookup) {
        if (isUsingHttpLookup) {
            return new PulsarClientImpl[] {clientWithHttpLookup1};
        } else {
            return new PulsarClientImpl[] {clientWitBinaryLookup1};
        }

    }

    protected int getLookupRequestPermits() {
        return pulsar1.getBrokerService().getLookupRequestSemaphore().availablePermits();
    }

    protected void verifyPartitionsNeverCreated(String topicNameStr) throws Exception {
        TopicName topicName = TopicName.get(topicNameStr);
        List<String> topicList = admin1.topics().getList("public/default");
        for (int i = 0; i < 3; i++) {
            assertFalse(topicList.contains(topicName.getPartition(i)));
        }
    }

    protected void verifyNonPartitionedTopicNeverCreated(String topicNameStr) throws Exception {
        TopicName topicName = TopicName.get(topicNameStr);
        List<String> topicList = admin1.topics().getList("public/default");
        assertFalse(topicList.contains(topicName.getPartitionedTopicName()));
    }

    @DataProvider(name = "topicDomains")
    public Object[][] topicDomains() {
        return new Object[][]{
            {TopicDomain.persistent},
            {TopicDomain.non_persistent}
        };
    }

    protected static void doModifyTopicAutoCreation(PulsarAdmin admin1, PulsarService pulsar1,
                                                  boolean allowAutoTopicCreation, TopicType allowAutoTopicCreationType,
                                                  int defaultNumPartitions) throws Exception {
        admin1.brokers().updateDynamicConfiguration(
                "allowAutoTopicCreation", allowAutoTopicCreation + "");
        admin1.brokers().updateDynamicConfiguration(
                "allowAutoTopicCreationType", allowAutoTopicCreationType + "");
        admin1.brokers().updateDynamicConfiguration(
                "defaultNumPartitions", defaultNumPartitions + "");
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar1.getConfiguration().isAllowAutoTopicCreation(), allowAutoTopicCreation);
            assertEquals(pulsar1.getConfiguration().getAllowAutoTopicCreationType(), allowAutoTopicCreationType);
            assertEquals(pulsar1.getConfiguration().getDefaultNumPartitions(), defaultNumPartitions);
        });
    }

    protected void modifyTopicAutoCreation(boolean allowAutoTopicCreation,
                                           TopicType allowAutoTopicCreationType,
                                           int defaultNumPartitions) throws Exception {
        doModifyTopicAutoCreation(admin1, pulsar1, allowAutoTopicCreation, allowAutoTopicCreationType,
                defaultNumPartitions);
    }

    @Test(dataProvider = "topicDomains")
    public void testAutoCreatingMetadataWhenCallingOldAPI(TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(true, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        for (PulsarClientImpl client : getClientsToTest()) {
            // Verify: the behavior of topic creation.
            final String tp = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
            client.getPartitionsForTopic(tp).join();
            Optional<PartitionedTopicMetadata> metadata1 = pulsar1.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources()
                    .getPartitionedTopicMetadataAsync(TopicName.get(tp), true).join();
            assertTrue(metadata1.isPresent());
            assertEquals(metadata1.get().partitions, 3);

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });

            // Cleanup.
            admin1.topics().deletePartitionedTopic(tp, false);
        }
    }

    @Test(dataProvider = "topicDomains", priority = Integer.MAX_VALUE)
    public void testCompatibilityForNewClientAndOldBroker(TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(true, TopicType.PARTITIONED, 3);
        // Initialize connections.
        String pulsarUrl = pulsar1.getBrokerServiceUrl();
        PulsarClientImpl[] clients = getClientsToTest(false);
        for (PulsarClientImpl client : clients) {
            client.getLookup(pulsarUrl).getBroker(TopicName.get(DEFAULT_NS + "/tp1")).join();
        }
        // Inject a not support flag into the connections initialized.
        Field field = ClientCnx.class.getDeclaredField("supportsGetPartitionedMetadataWithoutAutoCreation");
        field.setAccessible(true);
        for (PulsarClientImpl client : clients) {
            ConnectionPool pool = client.getCnxPool();
            for (CompletableFuture<ClientCnx> connectionFuture : pool.getConnections()) {
                ClientCnx clientCnx = connectionFuture.join();
                clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation();
                field.set(clientCnx, false);
            }
        }

        // Verify: the method "getPartitionsForTopic(topic, false, true)" will fallback to
        // "getPartitionsForTopic(topic)" behavior.
        int lookupPermitsBefore = getLookupRequestPermits();
        for (PulsarClientImpl client : clients) {
            // Verify: the behavior of topic creation.
            final String tp = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
            client.getPartitionedTopicMetadata(tp, false, true).join();
            Optional<PartitionedTopicMetadata> metadata1 = pulsar1.getPulsarResources().getNamespaceResources()
                    .getPartitionedTopicResources()
                    .getPartitionedTopicMetadataAsync(TopicName.get(tp), true).join();
            assertTrue(metadata1.isPresent());
            assertEquals(metadata1.get().partitions, 3);

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });

            // Cleanup.
            admin1.topics().deletePartitionedTopic(tp, false);
        }

        // reset clients.
        for (PulsarClientImpl client : clients) {
            ConnectionPool pool = client.getCnxPool();
            for (CompletableFuture<ClientCnx> connectionFuture : pool.getConnections()) {
                ClientCnx clientCnx = connectionFuture.join();
                clientCnx.isSupportsGetPartitionedMetadataWithoutAutoCreation();
                field.set(clientCnx, true);
            }
        }
    }

    @DataProvider(name = "autoCreationParamsAll")
    public Object[][] autoCreationParamsAll(){
        return new Object[][]{
            // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
            {true, true, true, TopicDomain.persistent},
            {true, true, false, TopicDomain.persistent},
            {true, false, true, TopicDomain.persistent},
            {true, false, false, TopicDomain.persistent},
            {false, true, true, TopicDomain.persistent},
            {false, true, false, TopicDomain.persistent},
            {false, false, true, TopicDomain.persistent},
            {false, false, false, TopicDomain.persistent},
            {true, true, true, TopicDomain.non_persistent},
            {true, true, false, TopicDomain.non_persistent},
            {true, false, true, TopicDomain.non_persistent},
            {true, false, false, TopicDomain.non_persistent},
            {false, true, true, TopicDomain.non_persistent},
            {false, true, false, TopicDomain.non_persistent},
            {false, false, true, TopicDomain.non_persistent},
            {false, false, false, TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfNonPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                           boolean paramMetadataAutoCreationEnabled,
                                                           boolean isUsingHttpLookup,
                                                           TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(configAllowAutoTopicCreation, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp_");
        admin1.topics().createNonPartitionedTopic(topicNameStr);

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response =
                    client.getPartitionedTopicMetadata(topicNameStr, paramMetadataAutoCreationEnabled, false).join();
            assertEquals(response.partitions, 0);
            List<String> partitionedTopics = admin1.topics().getPartitionedTopicList("public/default");
            assertFalse(partitionedTopics.contains(topicNameStr));
            verifyPartitionsNeverCreated(topicNameStr);

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });
        }

        // Cleanup.
        admin1.topics().delete(topicNameStr, false);
    }

    @Test(dataProvider = "autoCreationParamsAll")
    public void testGetMetadataIfPartitionedTopicExists(boolean configAllowAutoTopicCreation,
                                                        boolean paramMetadataAutoCreationEnabled,
                                                        boolean isUsingHttpLookup,
                                                        TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(configAllowAutoTopicCreation, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        // Create topic.
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
        admin1.topics().createPartitionedTopic(topicNameStr, 3);

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response =
                    client.getPartitionedTopicMetadata(topicNameStr, paramMetadataAutoCreationEnabled, false).join();
            assertEquals(response.partitions, 3);
            verifyNonPartitionedTopicNeverCreated(topicNameStr);

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });
        }

        // Cleanup.
        admin1.topics().deletePartitionedTopic(topicNameStr, false);
    }

    @DataProvider(name = "clients")
    public Object[][] clients(){
        return new Object[][]{
                // isUsingHttpLookup.
                {true, TopicDomain.persistent},
                {false, TopicDomain.non_persistent}
        };
    }

    @Test(dataProvider = "clients")
    public void testAutoCreatePartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(true, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Case-1: normal topic.
            final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response = client.getPartitionedTopicMetadata(topicNameStr, true, false).join();
            assertEquals(response.partitions, 3);
            // Verify: the behavior of topic creation.
            List<String> partitionedTopics = admin1.topics().getPartitionedTopicList("public/default");
            assertTrue(partitionedTopics.contains(topicNameStr));
            verifyNonPartitionedTopicNeverCreated(topicNameStr);
            // The API "getPartitionedTopicMetadata" only creates the partitioned metadata, it will not create the
            // partitions.
            verifyPartitionsNeverCreated(topicNameStr);

            // Case-2: topic with suffix "-partition-1".
            final String topicNameStrWithSuffix = BrokerTestUtil.newUniqueName(
                    topicDomain.value() + "://" + DEFAULT_NS + "/tp") + "-partition-1";
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response2 =
                    client.getPartitionedTopicMetadata(topicNameStrWithSuffix, true, false).join();
            assertEquals(response2.partitions, 0);
            // Verify: the behavior of topic creation.
            List<String> partitionedTopics2 =
                    admin1.topics().getPartitionedTopicList("public/default");
            assertFalse(partitionedTopics2.contains(topicNameStrWithSuffix));
            assertFalse(partitionedTopics2.contains(
                    TopicName.get(topicNameStrWithSuffix).getPartitionedTopicName()));

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });
            // Cleanup.
            admin1.topics().deletePartitionedTopic(topicNameStr, false);
            try {
                admin1.topics().delete(topicNameStrWithSuffix, false);
            } catch (Exception ex) {}
        }

    }

    @Test(dataProvider = "clients")
    public void testAutoCreateNonPartitionedTopic(boolean isUsingHttpLookup, TopicDomain topicDomain) throws Exception {
        modifyTopicAutoCreation(true, TopicType.NON_PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Case 1: normal topic.
            final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.value() + "://" + DEFAULT_NS + "/tp");
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response = client.getPartitionedTopicMetadata(topicNameStr, true, false).join();
            assertEquals(response.partitions, 0);
            // Verify: the behavior of topic creation.
            List<String> partitionedTopics = admin1.topics().getPartitionedTopicList("public/default");
            assertFalse(partitionedTopics.contains(topicNameStr));
            verifyPartitionsNeverCreated(topicNameStr);

            // Case-2: topic with suffix "-partition-1".
            final String topicNameStrWithSuffix = BrokerTestUtil.newUniqueName(
                    topicDomain.value() + "://" + DEFAULT_NS + "/tp") + "-partition-1";
            // Verify: the result of get partitioned topic metadata.
            PartitionedTopicMetadata response2 =
                    client.getPartitionedTopicMetadata(topicNameStrWithSuffix, true, false).join();
            assertEquals(response2.partitions, 0);
            // Verify: the behavior of topic creation.
            List<String> partitionedTopics2 =
                    admin1.topics().getPartitionedTopicList("public/default");
            assertFalse(partitionedTopics2.contains(topicNameStrWithSuffix));
            assertFalse(partitionedTopics2.contains(
                    TopicName.get(topicNameStrWithSuffix).getPartitionedTopicName()));

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });
            // Cleanup.
            try {
                admin1.topics().delete(topicNameStr, false);
            } catch (Exception ex) {}
            try {
                admin1.topics().delete(topicNameStrWithSuffix, false);
            } catch (Exception ex) {}
        }
    }

    @DataProvider(name = "autoCreationParamsNotAllow")
    public Object[][] autoCreationParamsNotAllow(){
        return new Object[][]{
                // configAllowAutoTopicCreation, paramCreateIfAutoCreationEnabled, isUsingHttpLookup.
                {true, false, true},
                {true, false, false},
                {false, false, true},
                {false, false, false},
                {false, true, true},
                {false, true, false},
        };
    }

    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreate(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        modifyTopicAutoCreation(configAllowAutoTopicCreation, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Define topic.
            final String topicNameStr = BrokerTestUtil.newUniqueName("persistent://" + DEFAULT_NS + "/tp");
            final TopicName topicName = TopicName.get(topicNameStr);
            // Verify: the result of get partitioned topic metadata.
            try {
                client.getPartitionedTopicMetadata(topicNameStr, paramMetadataAutoCreationEnabled, false)
                        .join();
                fail("Expect a not found exception");
            } catch (Exception e) {
                Throwable unwrapEx = FutureUtil.unwrapCompletionException(e);
                assertTrue(unwrapEx instanceof PulsarClientException.TopicDoesNotExistException
                        || unwrapEx instanceof PulsarClientException.NotFoundException);
            }
            // Verify: the behavior of topic creation.
            List<String> partitionedTopics = admin1.topics().getPartitionedTopicList("public/default");
            pulsar1.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                    .partitionedTopicExists(topicName);
            assertFalse(partitionedTopics.contains(topicNameStr));
            verifyNonPartitionedTopicNeverCreated(topicNameStr);
            verifyPartitionsNeverCreated(topicNameStr);

            // Verify: lookup semaphore has been releases.
            Awaitility.await().untilAsserted(() -> {
                assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
            });
        }
    }

    /**
     * Regarding the API "get partitioned metadata" about non-persistent topic.
     * The original behavior is:
     *   param-auto-create = true, broker-config-auto-create = true
     *     HTTP API: default configuration {@link ServiceConfiguration#getDefaultNumPartitions()}
     *     binary API: default configuration {@link ServiceConfiguration#getDefaultNumPartitions()}
     *   param-auto-create = true, broker-config-auto-create = false
     *     HTTP API: {partitions: 0}
     *     binary API: {partitions: 0}
     *   param-auto-create = false
     *     HTTP API: not found error
     *     binary API: not support
     * After PIP-344, the behavior will be the same as persistent topics, which was described in PIP-344.
     */
    @Test(dataProvider = "autoCreationParamsNotAllow")
    public void testGetMetadataIfNotAllowedCreateOfNonPersistentTopic(boolean configAllowAutoTopicCreation,
                                                  boolean paramMetadataAutoCreationEnabled,
                                                  boolean isUsingHttpLookup) throws Exception {
        modifyTopicAutoCreation(configAllowAutoTopicCreation, TopicType.PARTITIONED, 3);

        int lookupPermitsBefore = getLookupRequestPermits();

        PulsarClientImpl[] clientArray = getClientsToTest(isUsingHttpLookup);
        for (PulsarClientImpl client : clientArray) {
            // Define topic.
            final String topicNameStr = BrokerTestUtil.newUniqueName("non-persistent://" + DEFAULT_NS + "/tp");
            final TopicName topicName = TopicName.get(topicNameStr);
            // Verify: the result of get partitioned topic metadata.
            try {
                PartitionedTopicMetadata topicMetadata = client
                        .getPartitionedTopicMetadata(topicNameStr, paramMetadataAutoCreationEnabled, false)
                        .join();
                log.info("Get topic metadata: {}", topicMetadata.partitions);
                fail("Expected a not found ex");
            } catch (Exception ex) {
                Throwable unwrapEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(unwrapEx instanceof PulsarClientException.TopicDoesNotExistException
                        || unwrapEx instanceof PulsarClientException.NotFoundException);
            }

            // Verify: the behavior of topic creation.
            List<String> partitionedTopics = admin1.topics().getPartitionedTopicList("public/default");
            pulsar1.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                    .partitionedTopicExists(topicName);
            assertFalse(partitionedTopics.contains(topicNameStr));
            verifyNonPartitionedTopicNeverCreated(topicNameStr);
            verifyPartitionsNeverCreated(topicNameStr);
        }

        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
        });
    }

    @Test(dataProvider = "topicDomains")
    public void testNamespaceNotExist(TopicDomain topicDomain) throws Exception {
        int lookupPermitsBefore = getLookupRequestPermits();
        final String namespaceNotExist = BrokerTestUtil.newUniqueName("public/ns");
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.toString() + "://" + namespaceNotExist + "/tp");
        PulsarClientImpl[] clientArray = getClientsToTest(false);
        for (PulsarClientImpl client : clientArray) {
            try {
                PartitionedTopicMetadata topicMetadata = client
                        .getPartitionedTopicMetadata(topicNameStr, true, true)
                        .join();
                log.info("Get topic metadata: {}", topicMetadata.partitions);
                fail("Expected a not found ex");
            } catch (Exception ex) {
                Throwable unwrapEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(unwrapEx instanceof PulsarClientException.TopicDoesNotExistException);
            }
        }
        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
        });
    }

    @Test(dataProvider = "topicDomains")
    public void testTenantNotExist(TopicDomain topicDomain) throws Exception {
        int lookupPermitsBefore = getLookupRequestPermits();
        final String tenantNotExist = BrokerTestUtil.newUniqueName("tenant");
        final String namespaceNotExist = BrokerTestUtil.newUniqueName(tenantNotExist + "/default");
        final String topicNameStr = BrokerTestUtil.newUniqueName(topicDomain.toString() + "://" + namespaceNotExist + "/tp");
        PulsarClientImpl[] clientArray = getClientsToTest(false);
        for (PulsarClientImpl client : clientArray) {
            try {
                PartitionedTopicMetadata topicMetadata = client
                        .getPartitionedTopicMetadata(topicNameStr, true, true)
                        .join();
                log.info("Get topic metadata: {}", topicMetadata.partitions);
                fail("Expected a not found ex");
            } catch (Exception ex) {
                Throwable unwrapEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(unwrapEx instanceof PulsarClientException.BrokerMetadataException ||
                        unwrapEx instanceof PulsarClientException.TopicDoesNotExistException);
            }
        }
        // Verify: lookup semaphore has been releases.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(getLookupRequestPermits(), lookupPermitsBefore);
        });
    }
}
