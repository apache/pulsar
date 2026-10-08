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
 *     env:
 *       brokerDeduplicationEnabled: "true"
 *   bookies:
 *     replicas: 3
 *     env:
 *       journalMaxSizeMB: "8192"
 *     journalTmpfs: 2g
 * </pre>
 */
record ClusterSettings(Component brokers, Component bookies) {
    static final String BROKERS = "brokers";
    static final String BOOKIES = "bookies";
    private static final List<String> KEYS = List.of(BROKERS, BOOKIES);
    private static final String REPLICAS = "replicas";
    private static final String ENV = "env";
    static final String JOURNAL_TMPFS = "journalTmpfs";
    private static final List<String> BROKER_KEYS = List.of(REPLICAS, ENV);
    private static final List<String> BOOKIE_KEYS = List.of(REPLICAS, ENV, JOURNAL_TMPFS);
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
        return new ClusterSettings(component(mapper, cluster, BROKERS, BROKER_KEYS),
                component(mapper, cluster, BOOKIES, BOOKIE_KEYS));
    }

    private static Component component(ObjectMapper mapper, JsonNode cluster, String name, List<String> keys) {
        JsonNode section = cluster.path(name);
        if (!section.isObject()) {
            throw new IllegalArgumentException("cluster." + name + " must be a mapping with " + REPLICAS + " and "
                    + ENV);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!keys.contains(field)) {
                throw new IllegalArgumentException("cluster." + name + "." + field + " isn't a setting; cluster."
                        + name + " has " + keys);
            }
        });
        JsonNode replicas = section.path(REPLICAS);
        if (!replicas.canConvertToInt() || replicas.intValue() < 1) {
            throw new IllegalArgumentException("cluster." + name + "." + REPLICAS + " must be a positive number, not "
                    + (replicas.isMissingNode() ? "missing" : replicas.toString()));
        }
        Map<String, String> env = env(mapper, section.path(ENV), "cluster." + name + "." + ENV);
        return new Component(replicas.intValue(), env, journalTmpfs(section.path(JOURNAL_TMPFS), env, name));
    }

    /** The size of the bookies' journal tmpfs, null when the setting is missing or null. */
    private static String journalTmpfs(JsonNode setting, Map<String, String> env, String name) {
        if (setting.isMissingNode() || setting.isNull()) {
            return null;
        }
        String path = "cluster." + name + "." + JOURNAL_TMPFS;
        String size = setting.asText();
        if (!setting.isValueNode() || !TMPFS_SIZE.matcher(size).matches()) {
            throw new IllegalArgumentException(path + " must be the size of the tmpfs, such as 2g, 512m or a number"
                    + " of bytes, not " + setting);
        }
        for (String directory : List.of(JOURNAL_DIRECTORY, JOURNAL_DIRECTORIES)) {
            if (env.containsKey(directory)) {
                throw new IllegalArgumentException(path + " puts the journal in " + TMPFS_JOURNAL_DIRECTORY
                        + ", so cluster." + name + ".env can't set " + directory);
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
