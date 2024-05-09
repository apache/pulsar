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

import static org.apache.pulsar.common.naming.NamespaceName.SYSTEM_NAMESPACE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import java.util.HashSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.api.MetadataEvent;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SyncConfigStoreTest extends GeoReplicationWithConfigurationSyncTestBase {

    private static final String CONF_NAME_SYNC_EVENT_TOPIC = "configurationMetadataSyncEventTopic";
    private static final String SYNC_EVENT_TOPIC = TopicDomain.persistent.value() + "://" + SYSTEM_NAMESPACE
            + "/__sync_config_meta";

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
        TenantInfoImpl tenantInfo = new TenantInfoImpl();
        tenantInfo.setAllowedClusters(new HashSet<>(Arrays.asList(cluster1, cluster2)));
        admin1.tenants().createTenant(TopicName.get(SYNC_EVENT_TOPIC).getTenant(), tenantInfo);
        admin1.namespaces().createNamespace(TopicName.get(SYNC_EVENT_TOPIC).getNamespace());
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
    }

    @Test
    public void testDynamicEnableConfigurationMetadataSyncEventTopic() throws Exception {
        // Verify the condition that supports synchronizer: the metadata store is a different one.
        Awaitility.await().untilAsserted(() -> {
            boolean shouldShutdownConfigurationMetadataStore =
                    WhiteboxImpl.getInternalState(pulsar1, "shouldShutdownConfigurationMetadataStore");
            assertTrue(shouldShutdownConfigurationMetadataStore);
        });

        // Verify the synchronizer will be created dynamically.
        admin1.brokers().updateDynamicConfiguration(CONF_NAME_SYNC_EVENT_TOPIC, SYNC_EVENT_TOPIC);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(pulsar1.getConfig().getConfigurationMetadataSyncEventTopic(), SYNC_EVENT_TOPIC);
            PulsarMetadataEventSynchronizer synchronizer =
                    WhiteboxImpl.getInternalState(pulsar1, "configMetadataSynchronizer");
            assertNotNull(synchronizer);
            assertEquals(synchronizer.getState(), PulsarMetadataEventSynchronizer.State.Started);
            assertTrue(synchronizer.isStarted());
        });

        PulsarMetadataEventSynchronizer synchronizerStarted =
                WhiteboxImpl.getInternalState(pulsar1, "configMetadataSynchronizer");
        Producer<MetadataEvent> producerStarted =
                WhiteboxImpl.getInternalState(synchronizerStarted, "producer");
        Consumer<MetadataEvent> consumerStarted =
                WhiteboxImpl.getInternalState(synchronizerStarted, "consumer");

        // Verify the synchronizer will be closed dynamically.
        admin1.brokers().deleteDynamicConfiguration(CONF_NAME_SYNC_EVENT_TOPIC);
        Awaitility.await().untilAsserted(() -> {
            // The synchronizer that was started will be closed.
            assertEquals(synchronizerStarted.getState(), PulsarMetadataEventSynchronizer.State.Closed);
            assertTrue(synchronizerStarted.isClosingOrClosed());
            assertFalse(producerStarted.isConnected());
            assertFalse(consumerStarted.isConnected());
            // The synchronizer in memory will be null.
            assertNull(pulsar1.getConfig().getConfigurationMetadataSyncEventTopic());
            PulsarMetadataEventSynchronizer synchronizer =
                    WhiteboxImpl.getInternalState(pulsar1, "configMetadataSynchronizer");
            assertNull(synchronizer);
        });
    }
}
