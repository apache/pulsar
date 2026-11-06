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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Covers brokers whose configuration metadata store URL is the same as the local one, so both use one store instance.
 */
@Test(groups = "broker")
public class SyncConfigStoreSharedWithLocalTest extends GeoReplicationWithConfigurationSyncTestBase {

    private static final String CONF_NAME_SYNC_EVENT_TOPIC = "configurationMetadataSyncEventTopic";
    private static final String SYNC_EVENT_TOPIC = TopicDomain.persistent.value() + "://" + SYSTEM_NAMESPACE
            + "/__sync_config_meta";

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

    @Override
    protected void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                     LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        super.setConfigDefaults(config, clusterName, bookkeeperEnsemble, brokerConfigZk);
        config.setConfigurationMetadataStoreUrl(config.getMetadataStoreUrl());
    }

    @Test
    public void testRejectDynamicUpdateOfConfigurationMetadataSyncEventTopic() throws Exception {
        assertTrue(pulsar1.isConfigurationMetadataStoreSharedWithLocal());

        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin1.brokers().updateDynamicConfiguration(CONF_NAME_SYNC_EVENT_TOPIC, SYNC_EVENT_TOPIC));
        assertFalse(admin1.brokers().getAllDynamicConfigurations().containsKey(CONF_NAME_SYNC_EVENT_TOPIC));
    }

    @Test
    public void testSkipSynchronizerSwapKeepsLocalWritesWorking() throws Exception {
        // The value can still reach the broker without the admin endpoint, e.g. persisted before an upgrade.
        pulsar1.getConfig().setConfigurationMetadataSyncEventTopic(SYNC_EVENT_TOPIC);
        try {
            pulsar1.initConfigMetadataSynchronizerIfNeeded();
            assertNull(pulsar1.getConfigMetadataSynchronizer());

            // Without the guard, these writes fail because the shared store waits for the sync topic producer.
            admin1.namespaces().createNamespace(defaultTenant + "/ns-after-sync-skip");
            admin1.topics().createNonPartitionedTopic(
                    "persistent://" + defaultTenant + "/ns-after-sync-skip/tp-after-sync-skip");
        } finally {
            pulsar1.getConfig().setConfigurationMetadataSyncEventTopic(null);
        }
    }
}
