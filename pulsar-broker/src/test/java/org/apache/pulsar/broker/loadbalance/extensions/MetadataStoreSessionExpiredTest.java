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
package org.apache.pulsar.broker.loadbalance.extensions;

import java.util.Collections;
import java.util.Optional;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.schema.SchemaStorageFactory;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.schema.SchemaStorage;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MetadataStoreSessionExpiredTest {

    private static final String clusterName = "test";
    private PulsarService pulsar;

    @BeforeClass
    protected void setup() throws Exception {
        pulsar = new PulsarService(brokerConfig());
        pulsar.start();
        final var admin = pulsar.getAdminClient();
        admin.clusters().createCluster(clusterName, ClusterData.builder().build());
        admin.tenants().createTenant("public", TenantInfo.builder()
                .allowedClusters(Collections.singleton(clusterName)).build());
        admin.namespaces().createNamespace("public/default");
    }

    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        pulsar.close();
    }

    @Test
    public void testLookupAfterSessionTimeout() throws Exception {
        final var topic = "test-lookup-after-session-timeout";
        pulsar.getAdminClient().topics().createPartitionedTopic(topic, 1);
    }

    private ServiceConfiguration brokerConfig() {
        final var config = new ServiceConfiguration();
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setMetadataStoreUrl("memory:local");
        config.setSchemaRegistryStorageClassName(MockSchemaStorageFactory.class.getName());
        config.setManagedLedgerStorageClassName(MockManagedLedgerStorage.class.getName());

        config.setSystemTopicEnabled(false);
        config.setTopicLevelPoliciesEnabled(false);

        config.setManagedLedgerDefaultWriteQuorum(1);
        config.setManagedLedgerDefaultAckQuorum(1);
        config.setManagedLedgerDefaultEnsembleSize(1);
        config.setDefaultNumberOfNamespaceBundles(16);
        config.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config.setLoadBalancerDebugModeEnabled(true);
        config.setBrokerShutdownTimeoutMs(100);
        return config;
    }

    public static class MockSchemaStorageFactory implements SchemaStorageFactory {

        @Override
        public SchemaStorage create(PulsarService pulsar) throws Exception {
            return new MockSchemaStorage();
        }
    }
}
