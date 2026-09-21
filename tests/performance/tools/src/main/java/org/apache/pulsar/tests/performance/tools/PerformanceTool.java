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
package org.apache.pulsar.tests.performance.tools;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "pulsar-performance-tools", mixinStandardHelpOptions = true,
        subcommands = {TelemetryProducer.class, TelemetryConsumer.class})
public class PerformanceTool implements Callable<Integer> {
    public static void main(String[] args) {
        System.exit(new CommandLine(new PerformanceTool()).execute(args));
    }

    static IotScenario readScenario(Path config, String path) throws Exception {
        YamlScenarioLoader loader = new YamlScenarioLoader();
        JsonNode raw = loader.mapper().readTree(config.toFile());
        JsonNode selected = loader.select(raw, path);
        return loader.mapper().treeToValue(selected, IotScenario.class);
    }

    @Override
    public Integer call() {
        return CommandLine.ExitCode.USAGE;
    }

    abstract static class ScenarioCommand implements Callable<Integer> {
        @Option(names = "--config", required = true)
        Path config;

        @Option(names = "--config-path", defaultValue = "workloads.iotTelemetry")
        String configPath;

        @Option(names = "--output", required = true)
        Path output;

        @Option(names = "--coordination-directory",
                description = "Shared directory for workload phase barriers (default: <output>/coordination)")
        Path coordinationDirectory;

        @Option(names = "--run-id",
                description = "Shared correlation ID, required when warmup is enabled; use a new ID per run")
        String runId;

        IotScenario scenario() throws Exception {
            IotScenario scenario = readScenario(config, configPath);
            if (scenario.warmupMessageCount() > 0 && (runId == null || runId.isBlank())) {
                throw new IllegalArgumentException(
                        "Warmup requires the same --run-id for the producer and all consumers");
            }
            return scenario;
        }

        Path coordinationDirectory() {
            return coordinationDirectory != null ? coordinationDirectory : output.resolve("coordination");
        }
    }
}
