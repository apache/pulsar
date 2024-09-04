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
package org.apache.pulsar.broker.zookeeper;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.MetadataSessionExpiredPolicy;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;


@Test
public class ZKReconnectTest extends MockedPulsarServiceBaseTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        this.conf.setZookeeperSessionExpiredPolicy(MetadataSessionExpiredPolicy.reconnect);
        this.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("public",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("public/default");
        admin.namespaces().setNamespaceReplicationClusters("public/default", Sets.newHashSet("test"));
    }

    @Test
    public void testGetPartitionMetadataFailAlsoCanProduceMessage() throws Exception {

        pulsarClient = PulsarClient.builder().
                serviceUrl(pulsar.getBrokerServiceUrl())
                .build();

        String topic = "testGetPartitionMetadataFailAlsoCanProduceMessage";
        admin.topics().createPartitionedTopic(topic, 5);
        Producer<byte[]> producer = pulsarClient.newProducer()
                .autoUpdatePartitionsInterval(1, TimeUnit.SECONDS).topic(topic).create();

        this.mockZooKeeper.setAlwaysFail(KeeperException.Code.SESSIONEXPIRED);

        // clear cache
        pulsar.getPulsarResources().getNamespaceResources().getPartitionedTopicResources()
                .getCache().delete("/admin/partitioned-topics/public/default/persistent"
                        + "/testGetPartitionMetadataFailAlsoCanProduceMessage");
        pulsar.getNamespaceService().getOwnershipCache().invalidateLocalOwnerCache();

        // autoUpdatePartitions 1 second
        TimeUnit.SECONDS.sleep(3);

        // also can send message
        producer.send("test".getBytes());
        this.mockZooKeeper.unsetAlwaysFail();
        producer.send("test".getBytes());
        producer.close();
    }


    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        this.internalCleanup();
    }
}
