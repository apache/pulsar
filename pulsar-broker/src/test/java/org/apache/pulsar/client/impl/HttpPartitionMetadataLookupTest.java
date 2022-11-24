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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.ExecutionException;
import lombok.Cleanup;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.collections.Sets;

@Test(groups = "broker-impl")
public class HttpPartitionMetadataLookupTest extends MockedPulsarServiceBaseTest {

    private final EventLoopGroup eventExecutors = new NioEventLoopGroup();

    @DataProvider
    public Object[][] legacyLookup() {
        return new Object[][] { {true}, {false} };
    }

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // No ops
    }

    private void internalSetup(boolean legacy) throws Exception {
        if (legacy) {
            conf.setCheckTopicExistsWhenQueryPartitions(false);
        }
        super.internalSetup();
        admin.clusters().createCluster("test", ClusterData.builder().serviceUrl(brokerUrl.toString()).build());
        admin.tenants().createTenant("prop",
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace("prop/ns-abc");
        admin.namespaces().setNamespaceReplicationClusters("prop/ns-abc", Sets.newHashSet("test"));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test(timeOut = 45000, dataProvider = "legacyLookup")
    public void testLegacyLookup(boolean legacy) throws Exception {
        internalSetup(legacy);
        BinaryProtoLookupService binaryLookup = (BinaryProtoLookupService)
                ((PulsarClientImpl) pulsar.getClient()).getLookup();
        @Cleanup HttpLookupService lookup = new HttpLookupService(newConf(pulsar), eventExecutors);
        @Cleanup LegacyHttpLookupService legacyLookup = new LegacyHttpLookupService(pulsar, eventExecutors);
        String topic = "persistent://prop/ns-abc/nonexistent-topic";
        try {
            assertEquals(legacyLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);
        } catch (ExecutionException e) {
            assertFalse(legacy);
            assertTrue(e.getCause() instanceof PulsarClientException.NotFoundException);
        }
        try {
            assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, 0);
        } catch (PulsarAdminException e) {
            assertFalse(legacy);
            assertTrue(e instanceof PulsarAdminException.NotFoundException);
        }
        assertEquals(lookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);
        assertEquals(binaryLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);

        topic = "persistent://prop/ns-abc/non-partitioned-topic";
        admin.topics().createNonPartitionedTopic(topic);
        assertEquals(legacyLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);
        assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, 0);
        assertEquals(lookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);
        assertEquals(binaryLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 0);

        topic = "persistent://prop/ns-abc/partitioned-topic";
        admin.topics().createPartitionedTopic(topic, 1);
        assertEquals(legacyLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 1);
        assertEquals(admin.topics().getPartitionedTopicMetadata(topic).partitions, 1);
        assertEquals(lookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 1);
        assertEquals(binaryLookup.getPartitionedTopicMetadata(TopicName.get(topic)).get().partitions, 1);
    }

    private static ClientConfigurationData newConf(PulsarService pulsar) {
        ClientConfigurationData conf = new ClientConfigurationData();
        conf.setServiceUrl(pulsar.getWebServiceAddress());
        return conf;
    }

    private static class LegacyHttpLookupService extends HttpLookupService {

        public LegacyHttpLookupService(PulsarService pulsar, EventLoopGroup eventLoopGroup)
                throws PulsarClientException {
            super(newConf(pulsar), eventLoopGroup);
        }

        @Override
        public boolean checkAllowTopicCreation() {
            return false;
        }
    }
}
