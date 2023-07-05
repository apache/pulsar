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
package org.apache.pulsar.sql.presto;

import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.type.TypeManager;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPulsarConnectorConfig {

    @Test
    public void testDefaultNamespaceDelimiterRewrite() {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        Assert.assertFalse(connectorConfig.getNamespaceDelimiterRewriteEnable());
        Assert.assertEquals("/", connectorConfig.getRewriteNamespaceDelimiter());
    }

    @Test
    public void testNamespaceRewriteDelimiterRestriction() {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        try {
            connectorConfig.setRewriteNamespaceDelimiter("-=:.Az09_");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        connectorConfig.setRewriteNamespaceDelimiter("|");
        Assert.assertEquals("|", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("||");
        Assert.assertEquals("||", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("$");
        Assert.assertEquals("$", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("&");
        Assert.assertEquals("&", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("--&");
        Assert.assertEquals("--&", (connectorConfig.getRewriteNamespaceDelimiter()));
    }

    @Test
    public void testDefaultBookkeeperConfig() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        Assert.assertEquals(0, connectorConfig.getBookkeeperThrottleValue());
        Assert.assertEquals(2 * availableProcessors, connectorConfig.getBookkeeperNumIOThreads());
        Assert.assertEquals(availableProcessors, connectorConfig.getBookkeeperNumWorkerThreads());
    }

    @Test
    public void testDefaultManagedLedgerConfig() {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        Assert.assertEquals(0L, connectorConfig.getManagedLedgerCacheSizeMB());
        Assert.assertEquals(availableProcessors, connectorConfig.getManagedLedgerNumSchedulerThreads());
        Assert.assertEquals(connectorConfig.getMaxSplitQueueSizeBytes(), -1);
    }

    @Test
    public void testGetOffloadPolices() throws Exception {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();

        final String managedLedgerOffloadDriver = "s3";
        final String offloaderDirectory = "/pulsar/offloaders";
        final Integer managedLedgerOffloadMaxThreads = 5;
        final String bucket = "offload-bucket";
        final String region = "us-west-2";
        final String endpoint = "http://s3.amazonaws.com";
        final String offloadProperties = "{"
                + "\"s3ManagedLedgerOffloadBucket\":\"" + bucket + "\","
                + "\"s3ManagedLedgerOffloadRegion\":\"" + region + "\","
                + "\"s3ManagedLedgerOffloadServiceEndpoint\":\"" + endpoint + "\""
                + "}";

        connectorConfig.setManagedLedgerOffloadDriver(managedLedgerOffloadDriver);
        connectorConfig.setOffloadersDirectory(offloaderDirectory);
        connectorConfig.setManagedLedgerOffloadMaxThreads(managedLedgerOffloadMaxThreads);
        connectorConfig.setOffloaderProperties(offloadProperties);

        OffloadPoliciesImpl offloadPolicies = connectorConfig.getOffloadPolices();
        Assert.assertNotNull(offloadPolicies);
        Assert.assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), managedLedgerOffloadDriver);
        Assert.assertEquals(offloadPolicies.getOffloadersDirectory(), offloaderDirectory);
        Assert.assertEquals((int) offloadPolicies.getManagedLedgerOffloadMaxThreads(), (int) managedLedgerOffloadMaxThreads);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadBucket(), bucket);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRegion(), region);
        Assert.assertEquals(offloadPolicies.getS3ManagedLedgerOffloadServiceEndpoint(), endpoint);
    }

    @Test
    public void testAnnotatedConfigurations() {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new PulsarConnectorModule("connectorId", Mockito.mock(TypeManager.class)));

        Map<String, String> config = new HashMap<>();

        config.put("pulsar.managed-ledger-offload-driver", "aws-s3");
        config.put("pulsar.offloaders-directory", "/pulsar/offloaders");
        config.put("pulsar.managed-ledger-offload-max-threads", "2");
        config.put("pulsar.offloader-properties", "{\"s3ManagedLedgerOffloadBucket\":\"offload-bucket\","
                + "\"s3ManagedLedgerOffloadRegion\":\"us-west-2\","
                + "\"s3ManagedLedgerOffloadServiceEndpoint\":\"http://s3.amazonaws.com\"}");
        config.put("pulsar.auth-plugin", "org.apache.pulsar.client.impl.auth.AuthenticationToken");
        config.put("pulsar.auth-params", "params");
        config.put("pulsar.tls-allow-insecure-connection", "true");
        config.put("pulsar.tls-hostname-verification-enable", "true");
        config.put("pulsar.tls-trust-cert-file-path", "/path");
        config.put("pulsar.bookkeeper-num-io-threads", "10");
        config.put("pulsar.bookkeeper-num-worker-threads", "10");
        config.put("pulsar.managed-ledger-num-scheduler-threads", "10");
        config.put("pulsar.stats-provider", "org.apache.bookkeeper.stats.prometheus.PrometheusMetricsProvider");
        config.put("pulsar.stats-provider-configs", "{\"httpServerEnabled\":\"false\","
                + "\"prometheusStatsHttpPort\":\"9092\","
                + "\"prometheusStatsHttpEnable\":\"true\"}");

        app.doNotInitializeLogging().setRequiredConfigurationProperties(config).initialize();
    }

}
