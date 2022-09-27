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

import static org.apache.pulsar.common.naming.SystemTopicNames.NAMESPACE_EVENTS_LOCAL_NAME;
import static org.apache.pulsar.compaction.Compactor.COMPACTION_SUBSCRIPTION;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import java.util.Random;

public abstract class BrokerTestBase extends MockedPulsarServiceBaseTest {
    protected static final int ASYNC_EVENT_COMPLETION_WAIT = 100;

    public void baseSetup() throws Exception {
        super.internalSetup();
        baseSetupCommon();
        afterSetup();
    }

    public void baseSetup(ServiceConfiguration serviceConfiguration) throws Exception {
        super.internalSetup(serviceConfiguration);
        baseSetupCommon();
        afterSetup();
    }

    protected void afterSetup() throws Exception {
        // NOP
    }

    private void baseSetupCommon() throws Exception {
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("prop",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop/ns-abc");
        admin.namespaces().setNamespaceReplicationClusters("prop/ns-abc", Sets.newHashSet("test"));
    }

    protected void createTransactionCoordinatorAssign() throws MetadataStoreException {
        createTransactionCoordinatorAssign(1);
    }

    protected void createTransactionCoordinatorAssign(int partitions) throws MetadataStoreException {
        pulsar.getPulsarResources()
                .getNamespaceResources()
                .getPartitionedTopicResources()
                .createPartitionedTopic(SystemTopicNames.TRANSACTION_COORDINATOR_ASSIGN,
                        new PartitionedTopicMetadata(partitions));
    }

    void rolloverPerIntervalStats() {
        try {
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().updateRates()).get();
        } catch (Exception e) {
            LOG.error("Stats executor error", e);
        }
    }

    void runGC() {
        try {
            pulsar.getBrokerService().forEachTopic(topic -> {
                if (topic instanceof AbstractTopic) {
                    ((AbstractTopic) topic).getInactiveTopicPolicies().setMaxInactiveDurationSeconds(0);
                }
            });
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().checkGC()).get();
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        } catch (Exception e) {
            LOG.error("GC executor error", e);
        }
    }

    void runMessageExpiryCheck() {
        try {
            pulsar.getExecutor().submit(() -> pulsar.getBrokerService().checkMessageExpiry()).get();
            Thread.sleep(ASYNC_EVENT_COMPLETION_WAIT);
        } catch (Exception e) {
            LOG.error("Error running message expiry check", e);
        }
    }

    private static final Random random = new Random();

    protected String newTopicName() {
        return "prop/ns-abc/topic-" + Long.toHexString(random.nextLong());
    }

    /**
     * see {@link #deleteNamespaceGraceFully}
     */
    protected void deleteNamespaceGraceFully(String ns, boolean force)
            throws Exception {
        deleteNamespaceGraceFully(ns, force, pulsar, admin);
    }

    /**
     * Wait until system topic "__change_event" and subscription "__compaction" are created, and then delete the namespace.
     */
    public static void deleteNamespaceGraceFully(String ns, boolean force, PulsarService pulsar, PulsarAdmin admin)
            throws Exception {
        // namespace v1 should not wait system topic create.
        if (ns.split("/").length > 2){
            admin.namespaces().deleteNamespace(ns, force);
            return;
        }
        if (!pulsar.getConfiguration().isSystemTopicEnabled()){
            admin.namespaces().deleteNamespace(ns, force);
            return;
        }
        // If no bundle has been loaded, then the System Topic will not trigger creation.
        LockManager lockManager = pulsar.getCoordinationService().getLockManager(NamespaceEphemeralData.class);
        List<String> lockedBundles = (List<String>) lockManager.listLocks("/namespace" + "/" + ns).join();
        if (CollectionUtils.isEmpty(lockedBundles)){
            admin.namespaces().deleteNamespace(ns, force);
            return;
        }
        // Trigger change event topic create.
        NamespaceName namespace = NamespaceName.get(ns);
        NamespaceBundle namespaceBundle = mock(NamespaceBundle.class);
        when(namespaceBundle.getNamespaceObject()).thenReturn(namespace);
        pulsar.getTopicPoliciesService().addOwnedNamespaceBundleAsync(namespaceBundle);
        // Wait for change event topic and compaction create finish.
        String allowAutoTopicCreationType = pulsar.getConfiguration().getAllowAutoTopicCreationType();
        int defaultNumPartitions = pulsar.getConfiguration().getDefaultNumPartitions();
        ArrayList<String> expectChangeEventTopics = new ArrayList<>();
        if ("non-partitioned".equals(allowAutoTopicCreationType)){
            String t = String.format("persistent://%s/%s", ns, NAMESPACE_EVENTS_LOCAL_NAME);
            expectChangeEventTopics.add(t);
        } else {
            for (int i = 0; i < defaultNumPartitions; i++){
                String t = String.format("persistent://%s/%s-partition-%s", ns, NAMESPACE_EVENTS_LOCAL_NAME, i);
                expectChangeEventTopics.add(t);
            }
        }
        Awaitility.await().until(() -> {
            boolean finished = true;
            for (String changeEventTopicName : expectChangeEventTopics){
                boolean bundleExists = pulsar.getNamespaceService()
                        .checkTopicOwnership(TopicName.get(changeEventTopicName))
                        .exceptionally(ex -> false).join();
                if (!bundleExists){
                    finished = false;
                    break;
                }
                CompletableFuture<Optional<Topic>> completableFuture =
                        pulsar.getBrokerService().getTopic(changeEventTopicName, false);
                if (completableFuture == null){
                    finished = false;
                    break;
                }
                Optional<Topic> optionalTopic = completableFuture.get();
                if (!optionalTopic.isPresent()){
                    finished = false;
                    break;
                }
                PersistentTopic changeEventTopic = (PersistentTopic) optionalTopic.get();
                if (!changeEventTopic.isCompactionEnabled()){
                    break;
                }
                if (!changeEventTopic.getSubscriptions().containsKey(COMPACTION_SUBSCRIPTION)){
                    finished = false;
                    break;
                }
            }
            return finished;
        });
        int retryTimes = 3;
        while (true) {
            try {
                admin.namespaces().deleteNamespace(ns, force);
                break;
            } catch (PulsarAdminException ex) {
                // Do retry only if topic fenced.
                if (ex.getStatusCode() == 500 && ex.getMessage().contains("TopicFencedException")){
                    if (--retryTimes > 0){
                        continue;
                    } else {
                        throw ex;
                    }
                }
                throw ex;
            }
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(BrokerTestBase.class);
}
