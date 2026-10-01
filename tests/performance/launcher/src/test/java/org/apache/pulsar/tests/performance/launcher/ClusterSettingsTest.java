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
