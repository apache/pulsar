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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.TopicType;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-replication")
public class ReplicationTopicGcUsingGlobalZKTest extends ReplicationTopicGcTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.usingGlobalZK = true;
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test(dataProvider = "topicTypes")
    public void testTopicGC(TopicType topicType) throws Exception {
        super.testTopicGC(topicType);
    }

    @Test(dataProvider = "topicTypes")
    public void testRemoteClusterStillConsumeAfterCurrentClusterGc(TopicType topicType) throws Exception {
        if (topicType.equals(TopicType.PARTITIONED)) {
            // Pulsar does not support the feature "brokerDeleteInactivePartitionedTopicMetadataEnabled" when enabling
            // Geo-Replication with Global ZK.
            return;
        }
        super.testRemoteClusterStillConsumeAfterCurrentClusterGc(topicType);
    }

    /**
     * Both clusters GC their copy of an inactive partition at about the same time, and the partitioned topic metadata
     * they share in the configuration store is deleted by r1 between r2's check that it exists and r2's own
     * deletion of it. r2 must still delete its schema and topic policies.
     */
    @Test
    public void testPartitionedTopicMetadataDeletedByRemoteClusterDuringGc() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://" + replicatedNamespace + "/tp_");
        final TopicName partitionedTopic = TopicName.get(topicName);
        final TopicName partition = partitionedTopic.getPartition(0);
        final String schemaId = partitionedTopic.getSchemaName();
        admin1.topics().createPartitionedTopic(topicName, 1);
        Producer<String> producer1 = client1.newProducer(Schema.STRING).topic(topicName).create();
        waitReplicatorStarted(partition.toString());
        PublishRate publishRate = new PublishRate(1000, 1024 * 1024);
        admin1.topicPolicies().setPublishRate(topicName, publishRate);
        admin2.topicPolicies().setPublishRate(topicName, publishRate);
        admin2.schemas().createSchema(topicName, Schema.STRING.getSchemaInfo());
        PersistentTopic topic1 = (PersistentTopic) broker1.getTopic(partition.toString(), false).get().get();
        PersistentTopic topic2 = (PersistentTopic) broker2.getTopic(partition.toString(), false).get().get();
        final int executorThreads = pulsar2.getConfiguration().getNumExecutorThreadPoolSize();
        final CountDownLatch releaseTopicDeletion = new CountDownLatch(1);
        final CountDownLatch releaseMetadataDeletion = new CountDownLatch(1);
        // Stop the periodic GC so that the test decides when each cluster GCs the partition. The topics keep the
        // inactive-topic policies they loaded with.
        pulsar1.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(false);
        pulsar2.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(false);
        try {
            producer1.close();
            Awaitility.await().untilAsserted(() -> {
                broker1.checkInactiveReplication();
                broker2.checkInactiveReplication();
                assertFalse(topic1.getReplicators().values().stream().anyMatch(Replicator::isConnected));
                assertFalse(topic2.getReplicators().values().stream().anyMatch(Replicator::isConnected));
            });
            // From here on checkGC() deletes the partition.
            final long maxInactiveNanos = TimeUnit.SECONDS.toNanos(
                    pulsar2.getConfiguration().getBrokerDeleteInactiveTopicsMaxInactiveDurationSeconds());
            Awaitility.await().until(() -> System.nanoTime() - topic1.lastActive > maxInactiveNanos
                    && System.nanoTime() - topic2.lastActive > maxInactiveNanos);

            // r2 GCs the partition. Its executor runs the deletion of the partition and then holds the deletion of
            // the partitioned topic metadata, which r2 has found and marked deleted by then.
            blockExecutor(pulsar2.getExecutor(), executorThreads, releaseTopicDeletion);
            topic2.checkGC();
            blockExecutor(pulsar2.getExecutor(), executorThreads, releaseMetadataDeletion);
            releaseTopicDeletion.countDown();
            PartitionedTopicResources partitionedTopicResources2 =
                    pulsar2.getPulsarResources().getNamespaceResources().getPartitionedTopicResources();
            Awaitility.await().untilAsserted(() -> {
                assertFalse(pulsar2.getPulsarResources().getTopicResources().persistentTopicExists(partition).get());
                assertTrue(partitionedTopicResources2.isPartitionedTopicBeingDeletedAsync(partitionedTopic).get());
            });

            // Meanwhile r1 GCs its partition and deletes the partitioned topic metadata.
            topic1.checkGC();
            Awaitility.await().untilAsserted(() -> {
                assertFalse(partitionedTopicResources2.partitionedTopicExists(partitionedTopic));
                assertEquals(pulsar2.getBrokerService().fetchPartitionedTopicMetadataAsync(partitionedTopic).get()
                        .partitions, 0);
            });
        } finally {
            releaseTopicDeletion.countDown();
            releaseMetadataDeletion.countDown();
            pulsar1.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(true);
            pulsar2.getConfiguration().setBrokerDeleteInactiveTopicsEnabled(true);
        }

        Awaitility.await().untilAsserted(() -> {
            assertFalse(pulsar1.getPulsarResources().getTopicResources().persistentTopicExists(partition).get());
            assertFalse(pulsar2.getPulsarResources().getTopicResources().persistentTopicExists(partition).get());
            assertTrue(pulsar1.getTopicPoliciesService().getTopicPoliciesAsync(partitionedTopic,
                    TopicPoliciesService.GetType.LOCAL_ONLY).get().isEmpty());
            assertTrue(pulsar2.getTopicPoliciesService().getTopicPoliciesAsync(partitionedTopic,
                    TopicPoliciesService.GetType.LOCAL_ONLY).get().isEmpty());
            assertTrue(CollectionUtils.isEmpty(pulsar1.getSchemaStorage().getAll(schemaId).get()));
            assertTrue(CollectionUtils.isEmpty(pulsar2.getSchemaStorage().getAll(schemaId).get()));
        });
    }

    /**
     * Occupies every thread of the executor until {@code release} counts down. The executor runs its tasks in
     * submission order, so the tasks submitted before still run and the ones submitted after wait.
     */
    private static void blockExecutor(Executor executor, int threads, CountDownLatch release) {
        for (int i = 0; i < threads; i++) {
            executor.execute(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}
