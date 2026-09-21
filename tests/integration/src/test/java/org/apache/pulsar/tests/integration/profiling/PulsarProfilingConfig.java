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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;

/** Configuration for the profiling scenario harness. */
final class PulsarProfilingConfig {
    static final String CONFIG_ENV = "PULSAR_PROFILING_CONFIG";
    static final String ENV_PREFIX = "PULSAR_PROFILING_";

    record Config(Cluster cluster, Load load, Profiling profiling, Output output) {
        static Config read() {
            String configFile = System.getenv(CONFIG_ENV);
            return read(configFile == null || configFile.isBlank() ? null : Path.of(configFile),
                    System.getenv());
        }

        static Config read(Path configFile, Map<String, String> environment) {
            YamlScenarioLoader loader = new YamlScenarioLoader();
            ObjectMapper mapper = loader.mapper();
            var root = loader.resolve(configFile, mapper.valueToTree(defaults()), environment,
                    ENV_PREFIX, CONFIG_ENV);
            try {
                return mapper.treeToValue(root, Config.class);
            } catch (IOException e) {
                throw new IllegalArgumentException("Cannot parse profiling configuration", e);
            }
        }

        static Config defaults() {
            return new Config(
                    new Cluster(1, 3, 0,
                            "-Xms2g -Xmx2g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch",
                            "-Xmx1g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch",
                            Map.ofEntries(
                                    Map.entry("managedLedgerMinLedgerRolloverTimeMinutes", "1"),
                                    Map.entry("managedLedgerMaxLedgerRolloverTimeMinutes", "5"),
                                    Map.entry("managedLedgerMaxSizePerLedgerMbytes", "512"),
                                    Map.entry("managedLedgerDefaultEnsembleSize", "1"),
                                    Map.entry("managedLedgerDefaultWriteQuorum", "1"),
                                    Map.entry("managedLedgerDefaultAckQuorum", "1"),
                                    Map.entry("dispatcherRetryBackoffInitialTimeInMs", "0"),
                                    Map.entry("dispatcherRetryBackoffMaxTimeInMs", "0"),
                                    Map.entry("preciseDispatcherFlowControl", "true"),
                                    Map.entry("dispatcherMaxReadBatchSize", "1000")),
                            Map.ofEntries(
                                    Map.entry("dbStorage_writeCacheMaxSizeMb", "64"),
                                    Map.entry("dbStorage_readAheadCacheMaxSizeMb", "96"),
                                    Map.entry("journalMaxSizeMB", "256"),
                                    Map.entry("journalSyncData", "false"),
                                    Map.entry("majorCompactionInterval", "300"),
                                    Map.entry("minorCompactionInterval", "30"),
                                    Map.entry("compactionRateByEntries", "20000"),
                                    Map.entry("gcWaitTime", "30000"),
                                    Map.entry("isForceGCAllowWhenNoSpace", "true"),
                                    Map.entry("diskUsageLwmThreshold", "0.75"),
                                    Map.entry("diskCheckInterval", "60"))),
                    new Load(20_000_000, "200M", "200M", Integer.MAX_VALUE, 128, 20_000, 10, 0, 0,
                            1, 1, 1, 1, SubscriptionType.Shared, 50_000, 180, false, ""),
                    new Profiling("", ""),
                    new Output("build/pulsar-profiling"));
        }
    }

    record Cluster(int numBrokers, int numBookies, int numProxies, String brokerMemory,
                   String bookkeeperMemory, Map<String, String> brokerEnvs,
                   Map<String, String> bookkeeperEnvs) {
    }

    record Load(long numberOfMessages, String produceMemoryLimit, String consumeMemoryLimit,
                int produceRate, int messageSize, int maxOutstanding, int statsIntervalSeconds,
                int isolatedProducers, int isolatedConsumers, int producerCount, int consumerCount,
                int producerIoThreads, int consumerIoThreads, SubscriptionType subscriptionType,
                int receiverQueueSize, int timeoutSeconds, boolean batchingEnabled, String messageKeyGenerationMode) {
        Load {
            if (messageKeyGenerationMode != null && !messageKeyGenerationMode.isEmpty()
                    && !messageKeyGenerationMode.equals("random")
                    && !messageKeyGenerationMode.equals("autoIncrement")) {
                throw new IllegalArgumentException("Message key generation mode must be random or autoIncrement");
            }
            if (producerCount < 1 || consumerCount < 1 || producerIoThreads < 1 || consumerIoThreads < 1
                    || isolatedProducers < 0 || isolatedConsumers < 0 || receiverQueueSize < 1 || timeoutSeconds < 1
                    || numberOfMessages < 1 || produceRate < 1 || subscriptionType == null) {
                throw new IllegalArgumentException("Profiling counts, rate, queue size and timeout must be positive");
            }
            // pulsar-perf divides the message count and rate between workers using integer division.
            int workers = Math.max(1, isolatedProducers);
            if (isolatedProducers > producerCount || isolatedConsumers > consumerCount
                    || numberOfMessages % workers != 0 || produceRate < workers) {
                throw new IllegalArgumentException("Isolated clients require at least one producer/consumer "
                        + "per client, a message count divisible by producer clients, and a rate >= producer clients");
            }
            if (subscriptionType == SubscriptionType.Exclusive && consumerCount != 1) {
                throw new IllegalArgumentException("Exclusive subscriptions require exactly one consumer");
            }
        }
    }

    record Output(String directory) {
    }

    /** Empty options disable profiling for that client process. */
    record Profiling(String producerOptions, String consumerOptions) {
    }

    private PulsarProfilingConfig() {
    }
}
