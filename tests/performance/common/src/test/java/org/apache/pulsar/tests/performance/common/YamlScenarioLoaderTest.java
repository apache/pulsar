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
package org.apache.pulsar.tests.performance.common;

import static org.assertj.core.api.Assertions.assertThat;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.testng.annotations.Test;

public class YamlScenarioLoaderTest {
    @Test
    public void resolvesRelativeParentsRemovalSelectionAndEnvironment() throws Exception {
        Path directory = Files.createTempDirectory("scenario-loader");
        Files.createDirectories(directory.resolve("parents"));
        Files.writeString(directory.resolve("base.yaml"), "workload:\n  rate: 100\n  removed: value\n");
        Files.writeString(directory.resolve("parents/child.yaml"), """
                extends: ../base.yaml
                workload:
                  removed: ~
                  clients: 10
                """);
        var loader = new YamlScenarioLoader();
        var resolved = loader.resolve(directory.resolve("parents/child.yaml"), null,
                Map.of("PERF_WORKLOAD_RATE", "200"), "PERF_", "PERF_CONFIG");

        assertThat(loader.select(resolved, "workload").get("rate").intValue()).isEqualTo(200);
        assertThat(loader.select(resolved, "workload").get("clients").intValue()).isEqualTo(10);
        assertThat(loader.select(resolved, "workload").has("removed")).isFalse();
    }
}
