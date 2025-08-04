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
package org.apache.pulsar.broker.service;

import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.BROKER_CERT_FILE_PATH;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.BROKER_KEY_FILE_PATH;
import static org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest.CA_CERT_FILE_PATH;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.GeoPersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentReplicator;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerImpl;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.stats.TopicStatsImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.testng.Assert;

@Slf4j
public abstract class OneWayReplicatorTestBase extends TestRetrySupport {

    protected final String defaultTenant = "public";
    protected final String replicatedNamespace = defaultTenant + "/default";
    protected final String nonReplicatedNamespace = defaultTenant + "/ns1";
    protected final String sourceClusterAlwaysSchemaCompatibleNamespace = defaultTenant + "/always-compatible";

    protected final String cluster1 = "r1";

    protected boolean usingGlobalZK = false;

    protected URL url1;
    protected URL urlTls1;
    protected ServiceConfiguration config1 = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk1;
    protected LocalBookkeeperEnsemble bkEnsemble1;
    protected PulsarService pulsar1;
    protected BrokerService broker1;
    protected PulsarAdmin admin1;
    protected PulsarClient client1;

    protected URL url2;
    protected URL urlTls2;
    protected final String cluster2 = "r2";
    protected ServiceConfiguration config2 = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk2;
    protected LocalBookkeeperEnsemble bkEnsemble2;
    protected PulsarService pulsar2;
    protected BrokerService broker2;
    protected PulsarAdmin admin2;
    protected PulsarClient client2;

    protected void startZKAndBK() throws Exception {
        // Start ZK.
        brokerConfigZk1 = new ZookeeperServerTest(0);
        brokerConfigZk1.start();
        if (usingGlobalZK) {
            brokerConfigZk2 = brokerConfigZk1;
        } else {
            brokerConfigZk2 = new ZookeeperServerTest(0);
            brokerConfigZk2.start();
        }

        // Start BK.
        bkEnsemble1 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble1.start();
        bkEnsemble2 = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble2.start();
    }

    protected void startBrokers() throws Exception {
        // Start brokers.
        setConfigDefaults(config1, cluster1, bkEnsemble1, brokerConfigZk1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();
        broker1 = pulsar1.getBrokerService();
        url1 = new URL(pulsar1.getWebServiceAddress());
        urlTls1 = new URL(pulsar1.getWebServiceAddressTls());

        // Start region 2
        setConfigDefaults(config2, cluster2, bkEnsemble2, brokerConfigZk2);
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
        broker2 = pulsar2.getBrokerService();
        url2 = new URL(pulsar2.getWebServiceAddress());
        urlTls2 = new URL(pulsar2.getWebServiceAddressTls());
    }

    protected void startAdminClient() throws Exception {
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();
    }

    protected void startPulsarClient() throws Exception{
        ClientBuilder clientBuilder1 = PulsarClient.builder().serviceUrl(url1.toString());
        client1 = initClient(clientBuilder1);
        ClientBuilder clientBuilder2 = PulsarClient.builder().serviceUrl(url2.toString());
        client2 = initClient(clientBuilder2);
    }

    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        admin1.clusters().createCluster(cluster1, ClusterData.builder()
                .serviceUrl(url1.toString())
                .serviceUrlTls(urlTls1.toString())
                .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin1.clusters().createCluster(cluster2, ClusterData.builder()
                .serviceUrl(url2.toString())
                .serviceUrlTls(urlTls2.toString())
                .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());
        admin1.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(cluster1, cluster2)));
        admin1.namespaces().createNamespace(replicatedNamespace, Sets.newHashSet(cluster1, cluster2));
        admin1.namespaces().createNamespace(
                sourceClusterAlwaysSchemaCompatibleNamespace, Sets.newHashSet(cluster1, cluster2));
        admin1.namespaces().setSchemaCompatibilityStrategy(sourceClusterAlwaysSchemaCompatibleNamespace,
                SchemaCompatibilityStrategy.ALWAYS_COMPATIBLE);
        admin1.namespaces().createNamespace(nonReplicatedNamespace);

        if (!usingGlobalZK) {
            admin2.clusters().createCluster(cluster1, ClusterData.builder()
                    .serviceUrl(url1.toString())
                    .serviceUrlTls(urlTls1.toString())
                    .brokerServiceUrl(pulsar1.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar1.getBrokerServiceUrlTls())
                    .brokerClientTlsEnabled(false)
                    .build());
            admin2.clusters().createCluster(cluster2, ClusterData.builder()
                    .serviceUrl(url2.toString())
                    .serviceUrlTls(urlTls2.toString())
                    .brokerServiceUrl(pulsar2.getBrokerServiceUrl())
                    .brokerServiceUrlTls(pulsar2.getBrokerServiceUrlTls())
                    .brokerClientTlsEnabled(false)
                    .build());
            admin2.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                    Sets.newHashSet(cluster1, cluster2)));
            admin2.namespaces().createNamespace(replicatedNamespace);
            admin2.namespaces().createNamespace(sourceClusterAlwaysSchemaCompatibleNamespace);
            admin2.namespaces().setSchemaCompatibilityStrategy(sourceClusterAlwaysSchemaCompatibleNamespace,
                    SchemaCompatibilityStrategy.FORWARD);
            admin2.namespaces().createNamespace(nonReplicatedNamespace);
        }

    }

    protected void cleanupTopics(CleanupTopicAction cleanupTopicAction) throws Exception {
        cleanupTopics(replicatedNamespace, cleanupTopicAction);
    }

    protected void cleanupTopics(String namespace, CleanupTopicAction cleanupTopicAction) throws Exception {
        if (usingGlobalZK) {
            throw new IllegalArgumentException("The method cleanupTopics does not support for global ZK");
        }
        waitChangeEventsInit(namespace);
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Collections.singleton(cluster1));
        admin1.namespaces().unload(namespace);
        cleanupTopicAction.run();
        admin1.namespaces().setNamespaceReplicationClusters(namespace, Sets.newHashSet(cluster1, cluster2));
        waitChangeEventsInit(namespace);
    }

    protected void waitChangeEventsInit(String namespace) {
        CompletableFuture<Optional<Topic>> future = pulsar1.getBrokerService()
                .getTopic(namespace + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME, false);
        if (future == null) {
            return;
        }
        Optional<Topic> optional = future.join();
        if (!optional.isPresent()) {
            return;
        }
        PersistentTopic topic = (PersistentTopic) optional.get();
        Awaitility.await().atMost(Duration.ofSeconds(180)).untilAsserted(() -> {
            TopicStatsImpl topicStats = topic.getStats(true, false, false);
            topicStats.getSubscriptions().entrySet().forEach(entry -> {
                // No wait for compaction.
                if (COMPACTION_SUBSCRIPTION.equals(entry.getKey())) {
                    return;
                }
                // No wait for durable cursor.
                if (entry.getValue().isDurable()) {
                    return;
                }
                Assert.assertTrue(entry.getValue().getMsgBacklog() == 0, entry.getKey());
            });
        });
    }

    protected interface CleanupTopicAction {
        void run() throws Exception;
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting OneWayReplicatorTestBase::setup ---");

        startZKAndBK();

        startBrokers();

        startAdminClient();

        createDefaultTenantsAndClustersAndNamespace();

        startPulsarClient();

        Thread.sleep(100);
        log.info("--- OneWayReplicatorTestBase::setup completed ---");
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                   LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bookkeeperEnsemble.getZookeeperPort());
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + brokerConfigZk.getZookeeperPort() + "/foo");
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(60);
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setBacklogQuotaCheckIntervalInSeconds(5);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
        config.setLoadBalancerSheddingEnabled(false);
        config.setForceDeleteNamespaceAllowed(true);
        config.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        config.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        config.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        config.setClusterName(clusterName);
        config.setTlsRequireTrustedClientCertOnConnect(false);
        Set<String> tlsProtocols = Sets.newConcurrentHashSet();
        tlsProtocols.add("TLSv1.3");
        tlsProtocols.add("TLSv1.2");
        config.setTlsProtocols(tlsProtocols);
    }

    protected void cleanupPulsarResources() throws Exception {
        // delete namespaces.
        waitChangeEventsInit(replicatedNamespace);
        admin1.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Sets.newHashSet(cluster1));
        if (!usingGlobalZK) {
            admin2.namespaces().setNamespaceReplicationClusters(replicatedNamespace, Sets.newHashSet(cluster2));
        }
        admin1.namespaces().deleteNamespace(replicatedNamespace, true);
        admin1.namespaces().deleteNamespace(nonReplicatedNamespace, true);
        if (!usingGlobalZK) {
            admin2.namespaces().deleteNamespace(replicatedNamespace, true);
            admin2.namespaces().deleteNamespace(nonReplicatedNamespace, true);
        }
    }

    @Override
    protected void cleanup() throws Exception {
        // cleanup pulsar resources.
        cleanupPulsarResources();

        // shutdown.
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");

        // Stop brokers.
        if (client1 != null) {
            client1.close();
            client1 = null;
        }
        if (client2 != null) {
            client2.close();
            client2 = null;
        }
        if (admin1 != null) {
            admin1.close();
            admin1 = null;
        }
        if (admin2 != null) {
            admin2.close();
            admin2 = null;
        }
        if (pulsar2 != null) {
            pulsar2.close();
            pulsar2 = null;
        }
        if (pulsar1 != null) {
            pulsar1.close();
            pulsar1 = null;
        }

        // Stop ZK and BK.
        if (bkEnsemble1 != null) {
            bkEnsemble1.stop();
            bkEnsemble1 = null;
        }
        if (bkEnsemble2 != null) {
            bkEnsemble2.stop();
            bkEnsemble2 = null;
        }
        if (brokerConfigZk1 != null) {
            brokerConfigZk1.stop();
            brokerConfigZk1 = null;
        }
        if (!usingGlobalZK && brokerConfigZk2 != null) {
            brokerConfigZk2.stop();
            brokerConfigZk2 = null;
        }

        // Reset configs.
        config1 = new ServiceConfiguration();
        config2 = new ServiceConfiguration();
    }

    protected void waitReplicatorStarted(String topicName) {
        waitReplicatorStarted(topicName, pulsar2);
    }

    protected void waitReplicatorStarted(String topicName, PulsarService remoteCluster) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> topicOptional2 = remoteCluster.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            assertFalse(persistentTopic2.getProducers().isEmpty());
        });
    }

    protected void waitReplicatorStopped(String topicName, boolean remoteTopicExpectedDeleted) {
        waitReplicatorStopped(topicName, pulsar1, pulsar2, remoteTopicExpectedDeleted);
    }

    protected void waitReplicatorStopped(String topicName, PulsarService sourceCluster, PulsarService remoteCluster,
                                         boolean remoteTopicExpectedDeleted) {
        Awaitility.await().untilAsserted(() -> {
            Optional<Topic> remoteTp = remoteCluster.getBrokerService().getTopic(topicName, false).get();
            if (remoteTopicExpectedDeleted) {
                assertTrue(remoteTp.isEmpty());
            } else {
                assertTrue(remoteTp.isPresent());
            }
            if (remoteTp.isPresent()) {
                PersistentTopic remoteTp1 = (PersistentTopic) remoteTp.get();
                for (org.apache.pulsar.broker.service.Producer p : remoteTp1.getProducers().values()) {
                    assertFalse(p.getProducerName().startsWith(remoteCluster.getConfig().getReplicatorPrefix()));
                }
            }
            Optional<Topic> sourceTp = sourceCluster.getBrokerService().getTopic(topicName, false).get();
            assertTrue(sourceTp.isPresent());
            PersistentTopic sourceTp1 = (PersistentTopic) sourceTp.get();
            assertTrue(sourceTp1.getReplicators().isEmpty()
                    || !sourceTp1.getReplicators().get(remoteCluster.getConfig().getClusterName()).isConnected());
        });
    }

    /**
     * Override "AbstractReplicator.producer" by {@param producer} and return the original value.
     */
    protected ProducerImpl overrideProducerForReplicator(AbstractReplicator replicator, ProducerImpl newProducer)
            throws Exception {
        Field producerField = AbstractReplicator.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        ProducerImpl originalValue = (ProducerImpl) producerField.get(replicator);
        synchronized (replicator) {
            producerField.set(replicator, newProducer);
        }
        return originalValue;
    }

    protected PulsarClient initClient(ClientBuilder clientBuilder) throws Exception {
        return clientBuilder.build();
    }

    protected void verifyReplicationWorks(String topic) throws Exception {
        // Wait for replicator starting.
        Awaitility.await().until(() -> {
            try {
                PersistentTopic persistentTopic = (PersistentTopic) pulsar1.getBrokerService()
                                .getTopic(topic, false).join().get();
                if (persistentTopic.getReplicators().size() > 0) {
                    return true;
                }
            } catch (Exception ex) {}

            try {
                String partition0 = TopicName.get(topic).getPartition(0).toString();
                PersistentTopic persistentTopic = (PersistentTopic) pulsar1.getBrokerService()
                        .getTopic(partition0, false).join().get();
                if (persistentTopic.getReplicators().size() > 0) {
                    return true;
                }
            } catch (Exception ex) {}

            return false;
        });
        // Verify: pub & sub.
        final String subscription = "__subscribe_1";
        final String msgValue = "__msg1";
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topic).create();
        Consumer<String> consumer2 = client2.newConsumer(Schema.STRING).topic(topic).isAckReceiptEnabled(true)
                .subscriptionName(subscription).subscribe();
        producer1.newMessage().value(msgValue).send();
        pulsar1.getBrokerService().checkReplicationPolicies();
        assertEquals(consumer2.receive(10, TimeUnit.SECONDS).getValue(), msgValue);
        consumer2.unsubscribe();
        producer1.close();
    }

    protected void setTopicLevelClusters(String topic, List<String> clusters, PulsarAdmin admin,
                                         PulsarService pulsar) throws Exception {
        Set<String> expected = new HashSet<>(clusters);
        TopicName topicName = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
        int partitions = ensurePartitionsAreSame(topic);
        admin.topics().setReplicationClusters(topic, clusters);
        Awaitility.await().untilAsserted(() -> {
            TopicPolicies policies = TopicPolicyTestUtils.getTopicPolicies(pulsar.getTopicPoliciesService(), topicName);
            assertEquals(new HashSet<>(policies.getReplicationClusters()), expected);
            if (partitions == 0) {
                checkNonPartitionedTopicLevelClusters(topicName.toString(), clusters, admin, pulsar.getBrokerService());
            } else {
                for (int i = 0; i < partitions; i++) {
                    checkNonPartitionedTopicLevelClusters(topicName.getPartition(i).toString(), clusters, admin,
                            pulsar.getBrokerService());
                }
            }
        });
    }

    protected void checkNonPartitionedTopicLevelClusters(String topic, List<String> clusters, PulsarAdmin admin,
                                           BrokerService broker) throws Exception {
        CompletableFuture<Optional<Topic>> future = broker.getTopic(topic, false);
        if (future == null) {
            return;
        }
        Optional<Topic> optional = future.join();
        if (optional == null || !optional.isPresent()) {
            return;
        }
        PersistentTopic persistentTopic = (PersistentTopic) optional.get();
        Set<String> expected = new HashSet<>(clusters);
        Set<String> act = new HashSet<>(TopicPolicyTestUtils.getTopicPolicies(persistentTopic)
                .getReplicationClusters());
        assertEquals(act, expected);
    }

    protected int ensurePartitionsAreSame(String topic) throws Exception {
        TopicName topicName = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
        boolean isPartitionedTopic1 = pulsar1.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources().partitionedTopicExists(topicName);
        boolean isPartitionedTopic2 = pulsar2.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources().partitionedTopicExists(topicName);
        if (isPartitionedTopic1 != isPartitionedTopic2) {
            throw new IllegalArgumentException(String.format("Can not delete topic."
                            + " isPartitionedTopic1: %s, isPartitionedTopic2: %s",
                    isPartitionedTopic1, isPartitionedTopic2));
        }
        if (!isPartitionedTopic1) {
            return 0;
        }
        int partitions1 = pulsar1.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources().getPartitionedTopicMetadataAsync(topicName).join().get().partitions;
        int partitions2 = pulsar2.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources().getPartitionedTopicMetadataAsync(topicName).join().get().partitions;
        if (partitions1 != partitions2) {
            throw new IllegalArgumentException(String.format("Can not delete topic."
                            + " partitions1: %s, partitions2: %s",
                    partitions1, partitions2));
        }
        return partitions1;
    }

    protected void deleteTopicAfterDisableTopicLevelReplication(String topic) throws Exception {
        setTopicLevelClusters(topic, Arrays.asList(cluster1), admin1, pulsar1);
        setTopicLevelClusters(topic, Arrays.asList(cluster1), admin2, pulsar2);
        admin2.topics().setReplicationClusters(topic, Arrays.asList(cluster2));

        int partitions = ensurePartitionsAreSame(topic);

        TopicName topicName = TopicName.get(TopicName.get(topic).getPartitionedTopicName());
        if (partitions != 0) {
            admin1.topics().deletePartitionedTopic(topicName.toString());
            admin2.topics().deletePartitionedTopic(topicName.toString());
        } else {
            admin1.topics().delete(topicName.toString());
            admin2.topics().delete(topicName.toString());
        }
    }

    protected void waitReplicatorStopped(PulsarService sourceCluster, PulsarService targetCluster, String topicName) {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            Optional<Topic> topicOptional2 = targetCluster.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional2.isPresent());
            PersistentTopic persistentTopic2 = (PersistentTopic) topicOptional2.get();
            for (org.apache.pulsar.broker.service.Producer producer : persistentTopic2.getProducers().values()) {
                assertFalse(producer.getProducerName()
                        .startsWith(targetCluster.getConfiguration().getReplicatorPrefix()));
            }
            Optional<Topic> topicOptional1 = sourceCluster.getBrokerService().getTopic(topicName, false).get();
            assertTrue(topicOptional1.isPresent());
            PersistentTopic persistentTopic1 = (PersistentTopic) topicOptional1.get();
            assertTrue(persistentTopic1.getReplicators().isEmpty()
                    || !persistentTopic1.getReplicators().get(targetCluster.getConfig().getClusterName())
                    .isConnected());
        });
    }

    protected void waitChangeEventsReplicated(String ns) {
        String topicName = "persistent://" + ns + "/" + SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
        TopicName topicNameObj = TopicName.get(topicName);
        Optional<PartitionedTopicMetadata> metadata = pulsar1.getPulsarResources().getNamespaceResources()
                .getPartitionedTopicResources()
                .getPartitionedTopicMetadataAsync(topicNameObj).join();
        Function<Replicator, Boolean> ensureNoBacklog = new Function<Replicator, Boolean>() {

            @Override
            public Boolean apply(Replicator replicator) {
                if (!replicator.getRemoteCluster().equals("c2")) {
                    return true;
                }
                PersistentReplicator persistentReplicator = (PersistentReplicator) replicator;
                Position lac = persistentReplicator.getCursor().getManagedLedger().getLastConfirmedEntry();
                Position mdPos = persistentReplicator.getCursor().getMarkDeletedPosition();
                return mdPos.compareTo(lac) >= 0;
            }
        };
        if (metadata.isPresent()) {
            for (int index = 0; index < metadata.get().partitions; index++) {
                String partitionName = topicNameObj.getPartition(index).toString();
                PersistentTopic persistentTopic =
                        (PersistentTopic) pulsar1.getBrokerService().getTopic(partitionName, false).join().get();
                persistentTopic.getReplicators().values().forEach(replicator -> {
                    assertTrue(ensureNoBacklog.apply(replicator));
                });
            }
        } else {
            PersistentTopic persistentTopic =
                    (PersistentTopic) pulsar1.getBrokerService().getTopic(topicName, false).join().get();
            persistentTopic.getReplicators().values().forEach(replicator -> {
                assertTrue(ensureNoBacklog.apply(replicator));
            });
        }
    }

    protected void waitForReplicationTaskFinish(String topicName) throws Exception {
        PersistentTopic persistentTopic1 = (PersistentTopic) pulsar1.getBrokerService()
                .getTopic(topicName, false).join().get();
        ManagedLedgerImpl ml = (ManagedLedgerImpl) persistentTopic1.getManagedLedger();
        Position lac = ml.getLastConfirmedEntry();
        ManagedCursorImpl cursor = (ManagedCursorImpl) ml.getCursors().get("pulsar.repl.r2");
        Awaitility.await().untilAsserted(() -> {
            if (cursor.getName().startsWith("pulsar.repl")) {
                assertTrue(cursor.getMarkDeletedPosition().compareTo(lac) >= 0);
            }
        });
    }

    protected GeoPersistentReplicator getReplicator(String topic) {
        waitReplicatorStarted(topic);
        return (GeoPersistentReplicator) pulsar1.getBrokerService().getTopic(topic, false).join().get()
                .getReplicators().get(cluster2);
    }
}
