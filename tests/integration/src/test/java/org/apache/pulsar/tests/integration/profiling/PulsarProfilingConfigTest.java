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
package org.apache.pulsar.tests.integration.profiling;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.Test;

public class PulsarProfilingConfigTest {
    @Test
    public void loadsYamlAndEnvironmentOverrides() throws Exception {
        Path file = Files.createTempFile("pulsar-profiling", ".yaml");
        try {
            Files.writeString(file, "load:\n  messageSize: 256\noutput:\n  directory: custom-output\n");
            PulsarProfilingConfig.Config config = PulsarProfilingConfig.Config.read(file, Map.of(
                    "PULSAR_PROFILING_LOAD_NUMBER_OF_MESSAGES", "1234",
                    "PULSAR_PROFILING_CLUSTER_NUM_BOOKIES", "5"));

            assertThat(config.load().messageSize()).isEqualTo(256);
            assertThat(config.load().numberOfMessages()).isEqualTo(1234);
            assertThat(config.cluster().numBookies()).isEqualTo(5);
            assertThat(config.output().directory()).isEqualTo("custom-output");
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
