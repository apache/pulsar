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
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.AutoTopicCreationOverride;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.common.policies.data.impl.AutoTopicCreationOverrideImpl;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class BrokerServiceChaosTest extends CanReconnectZKClientPulsarServiceBaseTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Test
    public void testFetchPartitionedTopicMetadataWithCacheRefresh() throws Exception {
        final String configMetadataStoreConnectString =
                WhiteboxImpl.getInternalState(pulsar.getConfigurationMetadataStore(), "zkConnectString");
        final ZooKeeper anotherZKCli = new ZooKeeper(configMetadataStoreConnectString, 5000,
                watchedEvent -> { });
        // Set policy of auto create topic to PARTITIONED.
        final String ns = defaultTenant + "/ns_" + UUID.randomUUID().toString().replaceAll("-", "");
        final TopicName topicName1 = TopicName.get("persistent://" + ns + "/tp1");
        final TopicName topicName2 = TopicName.get("persistent://" + ns + "/tp2");
        admin.namespaces().createNamespace(ns);
        AutoTopicCreationOverride autoTopicCreationOverride =
                new AutoTopicCreationOverrideImpl.AutoTopicCreationOverrideImplBuilder().allowAutoTopicCreation(true)
                                .topicType(TopicType.PARTITIONED.toString())
                                .defaultNumPartitions(3).build();
        admin.namespaces().setAutoTopicCreationAsync(ns, autoTopicCreationOverride);
        // Make the cache of namespace policy is valid.
        admin.namespaces().getAutoSubscriptionCreation(ns);
        // Trigger the zk node "/admin/partitioned-topics/{namespace}/persistent" created.
        admin.topics().createPartitionedTopic(topicName1.toString(), 2);
        admin.topics().deletePartitionedTopic(topicName1.toString());

        // Since there is no partitioned metadata created, the partitions count of metadata will be 0.
        PartitionedTopicMetadata partitionedTopicMetadata1 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2).get();
        assertEquals(partitionedTopicMetadata1.partitions, 0);

        // Create the partitioned metadata by another zk client.
        // Make a error to make the cache could not update.
        makeLocalMetadataStoreKeepReconnect();
        anotherZKCli.create("/admin/partitioned-topics/" + ns + "/persistent/" + topicName2.getLocalName(),
                "{\"partitions\":3}".getBytes(StandardCharsets.UTF_8),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        stopLocalMetadataStoreAlwaysReconnect();

        // Get the partitioned metadata from cache, there is 90% chance that partitions count of metadata is 0.
        PartitionedTopicMetadata partitionedTopicMetadata2 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2).get();
        // Note: If you want to reproduce the issue, you can perform validation on the next line.
        // assertEquals(partitionedTopicMetadata2.partitions, 0);

        // Verify the new method will return a correct result.
        PartitionedTopicMetadata partitionedTopicMetadata3 =
                pulsar.getBrokerService().fetchPartitionedTopicMetadataAsync(topicName2, true).get();
        assertEquals(partitionedTopicMetadata3.partitions, 3);

        // cleanup.
        admin.topics().deletePartitionedTopic(topicName2.toString());
        anotherZKCli.close();
    }
}
