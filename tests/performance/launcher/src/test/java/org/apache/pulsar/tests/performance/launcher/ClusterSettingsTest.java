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
package org.apache.pulsar.tests.performance.launcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.testng.annotations.Test;

public class ClusterSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsTheReplicasAndTheEnvironmentOfEachComponent() throws Exception {
        ClusterSettings settings = ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 2
                  env:
                    brokerDeduplicationEnabled: "true"
                    managedLedgerMaxEntriesPerLedger: 100000000
                bookies:
                  replicas: 3
                """));

        assertThat(settings.brokers().replicas()).isEqualTo(2);
        assertThat(settings.brokers().env()).containsEntry("brokerDeduplicationEnabled", "true")
                .containsEntry("managedLedgerMaxEntriesPerLedger", "100000000");
        assertThat(settings.bookies().replicas()).isEqualTo(3);
        assertThat(settings.bookies().env()).isEmpty();
        assertThat(settings.bookies().journalTmpfs()).isNull();
        assertThat(settings.bookies().containerEnv()).isEmpty();
        assertThat(settings.bookies().journalTmpfsMount()).isEmpty();
    }

    @Test
    public void putsTheBookiesJournalsOnATmpfs() throws Exception {
        ClusterSettings settings = ClusterSettings.read(mapper, mapper.readTree("""
                brokers:
                  replicas: 1
                bookies:
                  replicas: 3
                  env:
                    journalMaxSizeMB: "8192"
                    gcWaitTime: "86400000"
                  journalTmpfs: 2g
                """));

        assertThat(settings.bookies().journalTmpfs()).isEqualTo("2g");
        assertThat(settings.bookies().journalTmpfsMount()).containsExactly(
                entry(ClusterSettings.TMPFS_JOURNAL_DIRECTORY, "rw,size=2g,mode=1777"));
        // The tmpfs journal's settings replace the scenario's
        assertThat(settings.bookies().containerEnv()).containsEntry("journalMaxSizeMB", "256")
                .containsEntry("journalMaxBackups", "0")
                .containsEntry("journalDirectory", ClusterSettings.TMPFS_JOURNAL_DIRECTORY)
                .containsEntry("gcWaitTime", "86400000");
        assertThat(settings.bookies().env()).containsEntry("journalMaxSizeMB", "8192");
    }

    @Test
    public void rejectsAnInvalidJournalTmpfs() throws Exception {
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\n"
                + "bookies: {replicas: 3, journalTmpfs: 2 GB}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.bookies.journalTmpfs must be the size of the tmpfs");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\n"
                + "bookies: {replicas: 3, journalTmpfs: [2g]}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.bookies.journalTmpfs must be the size of the tmpfs");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\n"
                + "bookies: {replicas: 3, journalTmpfs: 2g, env: {journalDirectories: /journal}}")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.bookies.env can't set journalDirectories");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1, "
                + "journalTmpfs: 2g}\nbookies: {replicas: 3}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.brokers.journalTmpfs isn't a setting");
    }

    @Test
    public void rejectsAMissingReplicaCountAndTheFormerLayout() throws Exception {
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {env: {}}\nbookies: "
                + "{replicas: 3}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.brokers.replicas must be a positive number, not missing");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: 1\nbookies: 3")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.brokers must be a mapping with replicas and env");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\n"
                + "bookies: {replicas: 3}\nbrokerEnvs: {}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.brokerEnvs isn't a setting");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("brokers: {replicas: 1}\n"
                + "bookies: {replicas: 3}\nproducerEnvs: {}"))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster.producerEnvs isn't a setting");
        assertThatThrownBy(() -> ClusterSettings.read(mapper, mapper.readTree("workloads: {}").path("cluster")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs a cluster section");
    }
}
