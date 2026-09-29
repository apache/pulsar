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
import java.util.Set;

/**
 * A scenario's {@code cluster} section: the replicas and the environment of the brokers and of the bookies. The
 * workload's containers have their environment in the workload's section.
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
 * </pre>
 */
record ClusterSettings(Component brokers, Component bookies) {
    static final String BROKERS = "brokers";
    static final String BOOKIES = "bookies";
    private static final List<String> KEYS = List.of(BROKERS, BOOKIES);
    private static final String REPLICAS = "replicas";
    private static final String ENV = "env";
    private static final Set<String> COMPONENT_KEYS = Set.of(REPLICAS, ENV);

    /**
     * A component of the cluster.
     *
     * @param replicas how many containers run it
     * @param env the environment variables of its containers, such as Pulsar settings and {@code PULSAR_MEM}
     */
    record Component(int replicas, Map<String, String> env) {
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
                        + ", and brokers and bookies each have " + REPLICAS + " and " + ENV);
            }
        });
        return new ClusterSettings(component(mapper, cluster, BROKERS), component(mapper, cluster, BOOKIES));
    }

    private static Component component(ObjectMapper mapper, JsonNode cluster, String name) {
        JsonNode section = cluster.path(name);
        if (!section.isObject()) {
            throw new IllegalArgumentException("cluster." + name + " must be a mapping with " + REPLICAS + " and "
                    + ENV);
        }
        section.fieldNames().forEachRemaining(field -> {
            if (!COMPONENT_KEYS.contains(field)) {
                throw new IllegalArgumentException("cluster." + name + "." + field + " isn't a setting; cluster."
                        + name + " has " + REPLICAS + " and " + ENV);
            }
        });
        JsonNode replicas = section.path(REPLICAS);
        if (!replicas.canConvertToInt() || replicas.intValue() < 1) {
            throw new IllegalArgumentException("cluster." + name + "." + REPLICAS + " must be a positive number, not "
                    + (replicas.isMissingNode() ? "missing" : replicas.toString()));
        }
        return new Component(replicas.intValue(), env(mapper, section.path(ENV), "cluster." + name + "." + ENV));
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
