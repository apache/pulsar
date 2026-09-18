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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.util.ObjectMapperFactory;

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
            ObjectMapper mapper = ObjectMapperFactory.getYamlMapper().getObjectMapper();
            ObjectNode root = mapper.valueToTree(defaults());
            if (configFile != null) {
                mergeFile(root, configFile, mapper, new LinkedHashSet<>());
            }
            applyEnvironmentOverrides(root, environment);
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
                            1, 1, 1, 1, SubscriptionType.Shared, 50_000, 180, false),
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
                int receiverQueueSize, int timeoutSeconds, boolean batchingEnabled) {
        Load {
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

    private static void mergeFile(ObjectNode target, Path file, ObjectMapper mapper, Set<Path> activeFiles) {
        try {
            Path path = file.toRealPath();
            if (!activeFiles.add(path)) {
                throw new IllegalArgumentException("Profiling config inheritance cycle: "
                        + activeFiles + " -> " + path);
            }
            try {
                JsonNode source = mapper.readTree(path.toFile());
                if (!(source instanceof ObjectNode object)) {
                    throw new IllegalArgumentException("Profiling config must be a YAML mapping: " + path);
                }
                JsonNode parents = object.remove("extends");
                if (parents != null) {
                    if (parents.isTextual()) {
                        mergeParent(target, parents, path, mapper, activeFiles);
                    } else if (parents.isArray()) {
                        for (JsonNode parent : parents) {
                            mergeParent(target, parent, path, mapper, activeFiles);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "Profiling config 'extends' must be a path or list of paths: " + path);
                    }
                }
                // Apply directly to the accumulated config so inherited nulls also remove earlier values/defaults.
                merge(target, object);
            } finally {
                activeFiles.remove(path);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot read profiling config " + file, e);
        }
    }

    private static void mergeParent(ObjectNode target, JsonNode parent, Path file,
                                    ObjectMapper mapper, Set<Path> activeFiles) {
        if (!parent.isTextual() || parent.textValue().isBlank()) {
            throw new IllegalArgumentException("Profiling config 'extends' entries must be non-empty paths: " + file);
        }
        mergeFile(target, file.getParent().resolve(parent.textValue()), mapper, activeFiles);
    }

    private static void merge(ObjectNode target, ObjectNode source) {
        source.properties().forEach(entry -> {
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            if (value.isNull()) {
                target.remove(key);
            } else if (value instanceof ObjectNode object) {
                JsonNode current = target.get(key);
                ObjectNode child = current instanceof ObjectNode ? (ObjectNode) current : target.putObject(key);
                merge(child, object);
            } else {
                target.set(key, value);
            }
        });
    }

    private static void applyEnvironmentOverrides(ObjectNode root, Map<String, String> environment) {
        ObjectMapper mapper = ObjectMapperFactory.getYamlMapper().getObjectMapper();
        environment.forEach((name, value) -> {
            if (!name.startsWith(ENV_PREFIX) || name.equals(CONFIG_ENV)) {
                return;
            }
            String[] path = name.substring(ENV_PREFIX.length()).toLowerCase().split("_");
            ObjectNode node = root;
            int pathIndex = 0;
            while (pathIndex < path.length) {
                String field = findField(node, path, pathIndex);
                if (field == null) {
                    return;
                }
                int consumed = field.split("(?=[A-Z])").length;
                JsonNode existing = node.get(field);
                if (pathIndex + consumed == path.length) {
                    node.set(field, parseValue(mapper, value, existing));
                    return;
                }
                if (!(existing instanceof ObjectNode)) {
                    return;
                }
                node = (ObjectNode) existing;
                pathIndex += consumed;
            }
        });
    }

    private static String findField(ObjectNode node, String[] path, int start) {
        StringBuilder candidate = new StringBuilder();
        String result = null;
        int resultLength = 0;
        int tokenCount = 0;
        for (int i = start; i < path.length; i++) {
            candidate.append(path[i]);
            tokenCount++;
            String candidateName = candidate.toString();
            var fields = node.fieldNames();
            while (fields.hasNext()) {
                String field = fields.next();
                if (field.replace("_", "").equalsIgnoreCase(candidateName)) {
                    result = field;
                    resultLength = tokenCount;
                }
            }
        }
        return result;
    }

    private static JsonNode parseValue(ObjectMapper mapper, String value, JsonNode existing) {
        if (existing.isBoolean()) {
            return mapper.getNodeFactory().booleanNode(Boolean.parseBoolean(value));
        }
        if (existing.isIntegralNumber()) {
            return mapper.getNodeFactory().numberNode(Long.parseLong(value));
        }
        if (existing.isFloatingPointNumber()) {
            return mapper.getNodeFactory().numberNode(Double.parseDouble(value));
        }
        return mapper.getNodeFactory().textNode(value);
    }

    private PulsarProfilingConfig() {
    }
}
