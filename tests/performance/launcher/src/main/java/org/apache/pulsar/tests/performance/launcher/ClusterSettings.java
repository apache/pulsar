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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A scenario's {@code cluster} section: the replicas and the environment of the brokers and of the bookies, and
 * whether the bookies' journals are on a tmpfs. The workload's containers have their environment in the workload's
 * section.
 *
 * <pre>
 * cluster:
 *   brokers:
 *     replicas: 1
 *     bootstrap: first
 *     env:
 *       brokerDeduplicationEnabled: "true"
 *   bookies:
 *     replicas: 3
 *     env:
 *       journalMaxSizeMB: "8192"
 *     journalTmpfs: 2g
 * </pre>
 *
 * <p>The brokers' {@code bootstrap} is the brokers in the workloads' service URL: {@code first}, the default, gives the
 * first broker, and {@code all} every broker, so that the clients can look topics up while a broker is unreachable,
 * for example cut off by a network partition.
 *
 * <p>A {@code tls} section makes the workloads connect to the brokers with TLS, as {@link TlsSettings} describes,
 * and a {@code jwt} section makes the brokers authenticate the clients with tokens, as {@link JwtSettings} describes;
 * the remote cluster's too.
 *
 * <pre>
 * cluster:
 *   tls:
 *     enabled: true
 *   jwt:
 *     enabled: true
 * </pre>
 *
 * <p>A {@code remote} section adds a second cluster, which shares the configuration store with the first, so that
 * the namespace replicates the workload's topics to it, and runs applications that consume the replicated messages
 * there, as {@link Remote} describes. The gateways publish to the first cluster, and its applications consume there as
 * without a remote cluster.
 *
 * <pre>
 * cluster:
 *   remote:
 *     brokers:
 *       env:
 *         PULSAR_MEM: "-Xmx1g"
 *     applications:
 *       count: 4
 * </pre>
 */
record ClusterSettings(Component brokers, Component bookies, boolean allBrokersBootstrap, Remote remote,
                       TlsSettings tls, JwtSettings jwt) {
    static final String BROKERS = "brokers";
    static final String BOOKIES = "bookies";
    static final String REMOTE = "remote";
    static final String TLS = "tls";
    static final String JWT = "jwt";
    static final String APPLICATIONS = "applications";
    private static final List<String> KEYS = List.of(BROKERS, BOOKIES, REMOTE, TLS, JWT);
    private static final String REPLICAS = "replicas";
    private static final String ENV = "env";
    static final String BOOTSTRAP = "bootstrap";
    static final String JOURNAL_TMPFS = "journalTmpfs";
    static final String COUNT = "count";
    static final String PODS_PER_APPLICATION = "podsPerApplication";
    private static final List<String> BROKER_KEYS = List.of(REPLICAS, ENV, BOOTSTRAP);
    private static final List<String> BOOKIE_KEYS = List.of(REPLICAS, ENV, JOURNAL_TMPFS);
    private static final List<String> REMOTE_KEYS = List.of(BROKERS, BOOKIES, APPLICATIONS);
    private static final List<String> REMOTE_APPLICATION_KEYS = List.of(COUNT, PODS_PER_APPLICATION, ENV);
    /** The remote cluster's name: the first cluster's with this suffix. */
    static final String REMOTE_CLUSTER_SUFFIX = "-remote";
    /** A tmpfs size as Docker takes it: bytes, or a number with the unit k, m or g. */
    private static final Pattern TMPFS_SIZE = Pattern.compile("[1-9][0-9]*[kmgKMG]?");
    /**
     * The bookies' journal directory on the tmpfs, in place of the cluster's directory on the disk. Docker creates the
     * missing parent directories of a mount as root, so it is in /pulsar/data, which the image has and the bookie's
     * user owns: in data/bookkeeper, the bookie couldn't create its ledger directories.
     */
    static final String TMPFS_JOURNAL_DIRECTORY = "/pulsar/data/journal-tmpfs";
    private static final String JOURNAL_DIRECTORY = "journalDirectory";
    private static final String JOURNAL_DIRECTORIES = "journalDirectories";
    /**
     * The journal settings of a journal on a tmpfs, in place of the scenario's. A bookie deletes its old journal files
     * when it has flushed its write cache, but it keeps the current file until the file is full, and the backups.
     * Files of 256 MB without backups keep each bookie's journal to about 512 MB of memory.
     */
    static final Map<String, String> TMPFS_JOURNAL_SETTINGS = Map.of(
            "journalMaxSizeMB", "256",
            "journalMaxBackups", "0");

    /**
     * A component of the cluster.
     *
     * @param replicas how many containers run it
     * @param env the environment variables of its containers, such as Pulsar settings and {@code PULSAR_MEM}
     * @param journalTmpfs the size of the tmpfs that holds each bookie's journal, such as {@code 2g}, or null when
     *     the journal is on the disk; bookies only
     */
    record Component(int replicas, Map<String, String> env, String journalTmpfs) {
        /**
         * The environment of the containers: {@link #env()}, and with a journal on a tmpfs the journal's directory
         * and {@link #TMPFS_JOURNAL_SETTINGS}, which replace those of {@link #env()}.
         */
        Map<String, String> containerEnv() {
            if (journalTmpfs == null) {
                return env;
            }
            Map<String, String> containerEnv = new LinkedHashMap<>(env);
            containerEnv.putAll(TMPFS_JOURNAL_SETTINGS);
            containerEnv.put(JOURNAL_DIRECTORY, TMPFS_JOURNAL_DIRECTORY);
            return containerEnv;
        }

        /** The tmpfs mount of a bookie's journal, for Testcontainers' {@code withTmpFs}; empty without one. */
        Map<String, String> journalTmpfsMount() {
            // The bookie doesn't run as root, so the tmpfs is writable by every user, as /tmp is
            return journalTmpfs == null ? Map.of()
                    : Map.of(TMPFS_JOURNAL_DIRECTORY, "rw,size=" + journalTmpfs + ",mode=1777");
        }
    }

    /**
     * The second cluster, which the namespace replicates the workload's topics to.
     *
     * @param brokers its brokers: the first cluster's, with the settings and the variables of {@code remote.brokers}
     *     in place of theirs
     * @param bookies its bookies, from the first cluster's and {@code remote.bookies} in the same way
     * @param allBrokersBootstrap whether the remote applications' service URL has every remote broker
     * @param applications the applications that consume the replicated messages in it
     */
    record Remote(Component brokers, Component bookies, boolean allBrokersBootstrap, RemoteApplications applications) {
        /** The remote applications' service URL, with TLS when {@code tls} is enabled. */
        String serviceUrl(String clusterName, TlsSettings tls) {
            return ClusterSettings.serviceUrl(remoteClusterName(clusterName), brokers, allBrokersBootstrap, tls);
        }
    }

    /**
     * The remote applications, which run as the workload's applications do, with their own subscriptions in the
     * remote cluster: each setting that is null is the workload's.
     *
     * @param count how many applications
     * @param podsPerApplication the pods of each application
     * @param env the variables of their container, in place of those of the workload's applications
     */
    record RemoteApplications(Integer count, Integer podsPerApplication, Map<String, String> env) {
    }

    /** Reads the {@code cluster} section, which needs the brokers' and the bookies' replicas. */
    static ClusterSettings read(ObjectMapper mapper, JsonNode cluster) {
        if (!cluster.isObject()) {
            throw new IllegalArgumentException("The scenario needs a cluster section with " + BROKERS + " and "
                    + BOOKIES);
        }
        cluster.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException("cluster." + field + " isn't a setting; cluster has " + KEYS
                        + ", brokers have " + BROKER_KEYS + " and bookies " + BOOKIE_KEYS);
            }
        });
        return new ClusterSettings(component(mapper, cluster.path(BROKERS), "cluster." + BROKERS, BROKER_KEYS),
                component(mapper, cluster.path(BOOKIES), "cluster." + BOOKIES, BOOKIE_KEYS),
                allBrokersBootstrap(cluster.path(BROKERS), "cluster." + BROKERS), remote(mapper, cluster),
                TlsSettings.read(cluster.path(TLS)), JwtSettings.read(cluster.path(JWT)));
    }

    /** The {@code remote} section, null when it is missing or null. */
    private static Remote remote(ObjectMapper mapper, JsonNode cluster) {
        JsonNode remote = cluster.path(REMOTE);
        if (remote.isMissingNode() || remote.isNull()) {
            return null;
        }
        String path = "cluster." + REMOTE;
        if (!remote.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping, empty for a copy of the cluster");
        }
        remote.fieldNames().forEachRemaining(field -> {
            if (!REMOTE_KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; " + path + " has "
                        + REMOTE_KEYS);
            }
        });
        JsonNode brokers = merged(cluster.path(BROKERS), remote.path(BROKERS), path + "." + BROKERS);
        return new Remote(component(mapper, brokers, path + "." + BROKERS, BROKER_KEYS),
                component(mapper, merged(cluster.path(BOOKIES), remote.path(BOOKIES), path + "." + BOOKIES),
                        path + "." + BOOKIES, BOOKIE_KEYS),
                allBrokersBootstrap(brokers, path + "." + BROKERS),
                remoteApplications(mapper, remote.path(APPLICATIONS), path + "." + APPLICATIONS));
    }

    /**
     * A component of the remote cluster: a copy of the first cluster's, with the settings of {@code overrides} in place
     * of its own, and the variables of the overrides' {@code env} in place of its variables of the same names.
     */
    private static JsonNode merged(JsonNode component, JsonNode overrides, String path) {
        if (overrides.isMissingNode() || overrides.isNull()) {
            return component;
        }
        if (!overrides.isObject() || !component.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping");
        }
        ObjectNode merged = ((ObjectNode) component).deepCopy();
        overrides.properties().forEach(entry -> {
            if (entry.getKey().equals(ENV) && entry.getValue().isObject() && merged.path(ENV).isObject()) {
                ObjectNode env = (ObjectNode) merged.get(ENV);
                entry.getValue().properties().forEach(variable -> env.set(variable.getKey(), variable.getValue()));
            } else {
                merged.set(entry.getKey(), entry.getValue());
            }
        });
        return merged;
    }

    private static RemoteApplications remoteApplications(ObjectMapper mapper, JsonNode section, String path) {
        if (section.isMissingNode() || section.isNull()) {
            return new RemoteApplications(null, null, null);
        }
        if (!section.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + REMOTE_APPLICATION_KEYS);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!REMOTE_APPLICATION_KEYS.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; " + path + " has "
                        + REMOTE_APPLICATION_KEYS);
            }
        });
        return new RemoteApplications(positive(section.path(COUNT), path + "." + COUNT),
                positive(section.path(PODS_PER_APPLICATION), path + "." + PODS_PER_APPLICATION),
                section.has(ENV) && !section.get(ENV).isNull() ? env(mapper, section.get(ENV), path + "." + ENV)
                        : null);
    }

    /** A positive number, null when the setting is missing or null. */
    private static Integer positive(JsonNode setting, String path) {
        if (setting.isMissingNode() || setting.isNull()) {
            return null;
        }
        if (!setting.canConvertToInt() || setting.intValue() < 1) {
            throw new IllegalArgumentException(path + " must be a positive number, not " + setting);
        }
        return setting.intValue();
    }

    private static Component component(ObjectMapper mapper, JsonNode section, String path, List<String> keys) {
        if (!section.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping with " + REPLICAS + " and " + ENV);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!keys.contains(field)) {
                throw new IllegalArgumentException(path + "." + field + " isn't a setting; " + path + " has " + keys);
            }
        });
        JsonNode replicas = section.path(REPLICAS);
        if (!replicas.canConvertToInt() || replicas.intValue() < 1) {
            throw new IllegalArgumentException(path + "." + REPLICAS + " must be a positive number, not "
                    + (replicas.isMissingNode() ? "missing" : replicas.toString()));
        }
        Map<String, String> env = env(mapper, section.path(ENV), path + "." + ENV);
        return new Component(replicas.intValue(), env, journalTmpfs(section.path(JOURNAL_TMPFS), env, path));
    }

    /** Whether the brokers' {@code bootstrap} is {@code all}; {@code first} when the setting is missing or null. */
    private static boolean allBrokersBootstrap(JsonNode brokers, String path) {
        JsonNode setting = brokers.path(BOOTSTRAP);
        if (setting.isMissingNode() || setting.isNull()) {
            return false;
        }
        return switch (setting.asText()) {
            case "first" -> false;
            case "all" -> true;
            default -> throw new IllegalArgumentException(path + "." + BOOTSTRAP + " must be first or all, not "
                    + setting.asText());
        };
    }

    /** The containers that the network impairments' endpoints can name. */
    NetworkSettings.Topology topology() {
        return remote == null ? new NetworkSettings.Topology(brokers.replicas(), bookies.replicas())
                : new NetworkSettings.Topology(brokers.replicas(), bookies.replicas(), remote.brokers().replicas(),
                        remote.bookies().replicas());
    }

    /** The name of the remote cluster of the cluster {@code clusterName}. */
    static String remoteClusterName(String clusterName) {
        return clusterName + REMOTE_CLUSTER_SUFFIX;
    }

    /**
     * The workloads' service URL: the first broker, or every broker with {@code bootstrap: all}, at the TLS port with
     * TLS.
     */
    String serviceUrl(String clusterName) {
        return serviceUrl(clusterName, brokers, allBrokersBootstrap, tls);
    }

    private static String serviceUrl(String clusterName, Component brokers, boolean allBrokersBootstrap,
                                     TlsSettings tls) {
        int count = allBrokersBootstrap ? brokers.replicas() : 1;
        StringBuilder url = new StringBuilder(tls.scheme());
        for (int broker = 0; broker < count; broker++) {
            url.append(broker > 0 ? "," : "").append(clusterName).append("-pulsar-broker-").append(broker)
                    .append(':').append(tls.brokerPort());
        }
        return url.toString();
    }

    /** The size of the bookies' journal tmpfs, null when the setting is missing or null. */
    private static String journalTmpfs(JsonNode setting, Map<String, String> env, String componentPath) {
        if (setting.isMissingNode() || setting.isNull()) {
            return null;
        }
        String path = componentPath + "." + JOURNAL_TMPFS;
        String size = setting.asText();
        if (!setting.isValueNode() || !TMPFS_SIZE.matcher(size).matches()) {
            throw new IllegalArgumentException(path + " must be the size of the tmpfs, such as 2g, 512m or a number"
                    + " of bytes, not " + setting);
        }
        for (String directory : List.of(JOURNAL_DIRECTORY, JOURNAL_DIRECTORIES)) {
            if (env.containsKey(directory)) {
                throw new IllegalArgumentException(path + " puts the journal in " + TMPFS_JOURNAL_DIRECTORY
                        + ", so " + componentPath + ".env can't set " + directory);
            }
        }
        return size;
    }

    /** An {@code env} mapping of environment variables, empty when missing; {@code path} names it in errors. */
    static Map<String, String> env(ObjectMapper mapper, JsonNode env, String path) {
        if (env.isMissingNode() || env.isNull()) {
            return Map.of();
        }
        if (!env.isObject()) {
            throw new IllegalArgumentException(path + " must be a mapping of environment variables");
        }
        Map<String, String> variables = new LinkedHashMap<>();
        mapper.convertValue(env, new TypeReference<LinkedHashMap<String, Object>>() { })
                .forEach((name, value) -> variables.put(name, value != null ? value.toString() : ""));
        return variables;
    }
}
