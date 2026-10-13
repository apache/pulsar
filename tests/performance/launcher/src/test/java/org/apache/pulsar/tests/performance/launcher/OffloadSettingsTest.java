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
import java.util.Map;
import org.testng.annotations.Test;

public class OffloadSettingsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsTheSectionWithItsDefaults() throws Exception {
        assertThat(OffloadSettings.read(mapper.readTree("workloads: {}").path("offload"))).isNull();
        assertThat(OffloadSettings.read(mapper.readTree("{}"))).isEqualTo(new OffloadSettings(
                OffloadSettings.DEFAULT_IMAGE, "public/default", 0, null, 0));
        assertThat(OffloadSettings.read(mapper.readTree("""
                image: chrislusf/seaweedfs:4.40
                namespace: tenant/ns
                thresholdBytes: 1048576
                thresholdSeconds: 0
                deletionLagMillis: 60000
                """))).isEqualTo(new OffloadSettings("chrislusf/seaweedfs:4.40", "tenant/ns", 1048576, 0L, 60000));
    }

    @Test
    public void rejectsInvalidSettings() {
        assertThatThrownBy(() -> OffloadSettings.read(mapper.readTree("driver: aws-s3")))
                .hasMessageContaining("offload.driver isn't a setting");
        assertThatThrownBy(() -> OffloadSettings.read(mapper.readTree("thresholdBytes: -1")))
                .hasMessageContaining("offload.thresholdBytes must be a whole number, 0 or more");
        assertThatThrownBy(() -> OffloadSettings.read(mapper.readTree("namespace: default")))
                .hasMessageContaining("offload.namespace must be <tenant>/<namespace>");
        assertThatThrownBy(() -> OffloadSettings.read(mapper.readTree("true")))
                .hasMessageContaining("offload must be a mapping");
    }

    @Test
    public void givesTheBrokersTheOffloadersSettingsUnlessTheScenarioSetsThem() {
        Map<String, String> env = new OffloadSettings(OffloadSettings.DEFAULT_IMAGE, "public/default", 0, null, 0)
                .withBrokerOffloadSettings(Map.of("s3ManagedLedgerOffloadBucket", "other", "PULSAR_MEM", "-Xmx1g"));

        assertThat(env)
                .containsEntry("managedLedgerOffloadDriver", "aws-s3")
                .containsEntry("s3ManagedLedgerOffloadServiceEndpoint", "http://offload-storage:8333")
                .containsEntry("PULSAR_PREFIX_s3ManagedLedgerOffloadCredentialId", OffloadSettings.CREDENTIAL)
                .containsEntry("s3ManagedLedgerOffloadBucket", "other")
                .containsEntry("PULSAR_MEM", "-Xmx1g");
    }
}
