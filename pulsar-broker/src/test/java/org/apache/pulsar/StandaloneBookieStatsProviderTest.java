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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
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

    private static final Predicate<String> JVM_FAMILIES =
            name -> name.startsWith("jvm_") || name.startsWith("process_");

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

    private static Set<String> familyNames(Predicate<String> filter) {
        return PrometheusRegistry.defaultRegistry.scrape(filter).stream()
                .map(snapshot -> snapshot.getMetadata().getPrometheusName())
                .collect(Collectors.toCollection(TreeSet::new));
    }

    /**
     * A broker started earlier in this JVM registers the JVM metrics on the default registry itself, so the bookie
     * must leave the JVM families as they were and add no bookie families.
     */
    private static void assertBrokerRegistryUntouched(Set<String> jvmFamiliesBefore, String when) {
        assertThat(familyNames(name -> name.startsWith("bookie_")))
                .as("bookie families leaked into the broker registry %s", when)
                .isEmpty();
        assertThat(familyNames(JVM_FAMILIES))
                .as("JVM families in the broker registry %s", when)
                .isEqualTo(jvmFamiliesBefore);
    }

    @Test
    public void testBookieOnMetadataStoreStaysOutOfTheBrokerRegistry() throws Exception {
        Set<String> jvmFamiliesBefore = familyNames(JVM_FAMILIES);
        BKCluster cluster = BKCluster.builder()
                .baseServerConfiguration(bookieConfiguration())
                .metadataServiceUri("memory:" + UUID.randomUUID())
                .numBookies(1)
                .build();
        try {
            assertBrokerRegistryUntouched(jvmFamiliesBefore, "while the bookie is running");
        } finally {
            cluster.close();
        }
        assertBrokerRegistryUntouched(jvmFamiliesBefore, "after the cluster was closed");
    }

    @Test
    public void testBookieOnZooKeeperStaysOutOfTheBrokerRegistry() throws Exception {
        Set<String> jvmFamiliesBefore = familyNames(JVM_FAMILIES);
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(1, 0);
        try {
            ensemble.startStandalone(bookieConfiguration(), false);
            assertBrokerRegistryUntouched(jvmFamiliesBefore, "while the bookie is running");
        } finally {
            ensemble.stop();
        }
        assertBrokerRegistryUntouched(jvmFamiliesBefore, "after the ensemble was stopped");
    }
}
