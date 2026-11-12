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
package org.apache.pulsar;

import static org.assertj.core.api.Assertions.assertThat;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.bookkeeper.bookie.storage.ldb.DbLedgerStorage;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.pulsar.metadata.bookkeeper.BKCluster;
import org.apache.pulsar.metrics.prometheus.bookkeeper.PrometheusMetricsProvider;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.testng.annotations.Test;

/**
 * Standalone runs its bookie in the broker's JVM. The broker's /metrics reads the Prometheus Java client 1.x
 * default registry, so the bookie's stats provider has to register into a registry of its own, or the broker
 * scrape shows every JVM metric family twice and leaks the bookie's metrics.
 */
@Test(groups = "broker")
public class StandaloneBookieStatsProviderTest {

    private static final Predicate<String> BOOKIE_OR_JVM =
            name -> name.startsWith("jvm_") || name.startsWith("process_") || name.startsWith("bookie_");

    private static ServerConfiguration bookieConfiguration() {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setStatsProviderClass(PrometheusMetricsProvider.class);
        conf.setProperty(PrometheusMetricsProvider.PROMETHEUS_STATS_HTTP_ENABLE, false);
        conf.setLedgerStorageClass(DbLedgerStorage.class.getName());
        conf.setProperty("dbStorage_writeCacheMaxSizeMb", 2);
        conf.setProperty("dbStorage_readAheadCacheMaxSizeMb", 1);
        conf.setProperty("dbStorage_rocksDB_writeBufferSizeMB", 1);
        conf.setProperty("dbStorage_rocksDB_blockCacheSize", 1024 * 1024);
        conf.setJournalSyncData(false);
        conf.setAdvertisedAddress("127.0.0.1");
        conf.setAllowLoopback(true);
        conf.setBookiePort(0);
        conf.setAllowEphemeralPorts(true);
        conf.setAllocatorPoolingPolicy(PoolingPolicy.UnpooledHeap);
        return conf;
    }

    private static void assertBrokerRegistryUntouched(String when) {
        MetricSnapshots leaked = PrometheusRegistry.defaultRegistry.scrape(BOOKIE_OR_JVM);
        assertThat(leaked.stream().map(snapshot -> snapshot.getMetadata().getPrometheusName()))
                .as("metric families the bookie leaked into the broker registry %s", when)
                .isEmpty();
    }

    @Test
    public void testBookieOnMetadataStoreStaysOutOfTheBrokerRegistry() throws Exception {
        BKCluster cluster = BKCluster.builder()
                .baseServerConfiguration(bookieConfiguration())
                .metadataServiceUri("memory:" + UUID.randomUUID())
                .numBookies(1)
                .build();
        try {
            assertBrokerRegistryUntouched("while the bookie is running");
        } finally {
            cluster.close();
        }
        assertBrokerRegistryUntouched("after the cluster was closed");
    }

    @Test
    public void testBookieOnZooKeeperStaysOutOfTheBrokerRegistry() throws Exception {
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(1, 0);
        try {
            ensemble.startStandalone(bookieConfiguration(), false);
            assertBrokerRegistryUntouched("while the bookie is running");
        } finally {
            ensemble.stop();
        }
        assertBrokerRegistryUntouched("after the ensemble was stopped");
    }
}
