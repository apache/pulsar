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

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.images.builder.Transferable;

/**
 * A scenario's {@code offload} section: the brokers offload the managed ledgers to tiered storage, an S3-compatible
 * <a href="https://github.com/seaweedfs/seaweedfs">SeaweedFS</a> in a container on the cluster's network, and the
 * namespace's policies make them offload each ledger as soon as it rolls over. The brokers' ledger rollover settings,
 * such as {@code managedLedgerMaxEntriesPerLedger} and {@code managedLedgerMinLedgerRolloverTimeMinutes}, go in
 * {@code cluster.brokers.env}. The network impairments name the storage {@code offload-storage}.
 *
 * <pre>
 * offload:
 *   image: chrislusf/seaweedfs:4.47
 *   namespace: public/default
 *   thresholdBytes: 0
 *   thresholdSeconds: 0
 *   deletionLagMillis: 0
 * </pre>
 *
 * @param image the SeaweedFS image
 * @param namespace the namespace whose offload policies the launcher sets, after the cluster starts
 * @param thresholdBytes the namespace's offload threshold: how many bytes of a topic stay in BookKeeper, 0 offloading
 *                       every ledger once it rolls over
 * @param thresholdSeconds the namespace's offload threshold in seconds, or null to leave it unset, which older
 *                         brokers don't have
 * @param deletionLagMillis how long a ledger stays in BookKeeper after it has been offloaded, 0 deleting it at once,
 *                          so that reading it again reads the tiered storage
 */
record OffloadSettings(String image, String namespace, long thresholdBytes, Long thresholdSeconds,
                       long deletionLagMillis) {
    static final String DEFAULT_IMAGE = "chrislusf/seaweedfs:4.47";
    static final String DEFAULT_NAMESPACE = "public/default";
    /** The storage's name on the cluster's network, as the network impairments' endpoint names it too. */
    static final String STORAGE_HOST = NetworkSettings.OFFLOAD_STORAGE;
    static final int S3_PORT = 8333;
    static final String BUCKET = "pulsar";
    /**
     * The access key and the secret of the storage's only identity, which the brokers use. SeaweedFS verifies the
     * signature of each chunk of an upload, which the S3 offloader signs, with the identity's secret, so it needs one.
     */
    static final String CREDENTIAL = "performance";
    private static final String S3_CONFIG = "/etc/seaweedfs/s3.json";
    private static final List<String> KEYS = List.of("image", "namespace", "thresholdBytes", "thresholdSeconds",
            "deletionLagMillis");

    /** Reads the {@code offload} section, which may be missing: null then, without offloading. */
    static OffloadSettings read(JsonNode offload) {
        if (offload.isMissingNode() || offload.isNull()) {
            return null;
        }
        if (!offload.isObject()) {
            throw new IllegalArgumentException("offload must be a mapping with " + KEYS);
        }
        offload.fieldNames().forEachRemaining(field -> {
            if (!KEYS.contains(field)) {
                throw new IllegalArgumentException("offload." + field + " isn't a setting; offload has " + KEYS);
            }
        });
        String image = text(offload, "image", DEFAULT_IMAGE);
        String namespace = text(offload, "namespace", DEFAULT_NAMESPACE);
        if (!namespace.matches("[^/\\s]+/[^/\\s]+")) {
            throw new IllegalArgumentException("offload.namespace must be <tenant>/<namespace>, not " + namespace);
        }
        long thresholdBytes = number(offload, "thresholdBytes", 0L);
        Long thresholdSeconds = number(offload, "thresholdSeconds", null);
        long deletionLagMillis = number(offload, "deletionLagMillis", 0L);
        return new OffloadSettings(image, namespace, thresholdBytes, thresholdSeconds, deletionLagMillis);
    }

    private static String text(JsonNode section, String key, String defaultValue) {
        JsonNode value = section.path(key);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException("offload." + key + " must be text");
        }
        return value.asText();
    }

    private static Long number(JsonNode section, String key, Long defaultValue) {
        JsonNode value = section.path(key);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw new IllegalArgumentException("offload." + key + " must be a whole number, 0 or more");
        }
        return value.longValue();
    }

    /**
     * The brokers' environment with the S3 offloader's settings for the storage, which the scenario's broker
     * environment can override.
     */
    Map<String, String> withBrokerOffloadSettings(Map<String, String> brokerEnv) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put("managedLedgerOffloadDriver", "aws-s3");
        env.put("s3ManagedLedgerOffloadRegion", "us-east-1");
        env.put("s3ManagedLedgerOffloadBucket", BUCKET);
        env.put("s3ManagedLedgerOffloadServiceEndpoint", "http://" + STORAGE_HOST + ":" + S3_PORT);
        // broker.conf doesn't have the credentials, which the prefix adds
        env.put("PULSAR_PREFIX_s3ManagedLedgerOffloadCredentialId", CREDENTIAL);
        env.put("PULSAR_PREFIX_s3ManagedLedgerOffloadCredentialSecret", CREDENTIAL);
        if (brokerEnv != null) {
            env.putAll(brokerEnv);
        }
        return env;
    }

    /**
     * The storage's container on the network, not started: SeaweedFS's single-process server with its S3 API and the
     * offloads' bucket.
     */
    GenericContainer<?> storageContainer(Network network) {
        String s3Config = "{\"identities\": [{\"name\": \"" + CREDENTIAL + "\", \"credentials\": [{\"accessKey\": \""
                + CREDENTIAL + "\", \"secretKey\": \"" + CREDENTIAL + "\"}],"
                + " \"actions\": [\"Admin\", \"Read\", \"Write\", \"List\", \"Tagging\"]}]}";
        return new GenericContainer<>(image)
                .withNetwork(network)
                .withNetworkAliases(STORAGE_HOST)
                .withCopyToContainer(Transferable.of(s3Config), S3_CONFIG)
                .withCommand("mini", "-dir=/data", "-bucket=" + BUCKET, "-s3.config=" + S3_CONFIG,
                        "-master.telemetry=false")
                .withExposedPorts(S3_PORT)
                // The S3 API listens, and the filer, which holds the buckets, has the bucket
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forListeningPort())
                        .withStrategy(Wait.forSuccessfulCommand("wget -q -O /dev/null http://localhost:8888/buckets/"
                                + BUCKET + "/"))
                        .withStartupTimeout(Duration.ofMinutes(2)));
    }
}
