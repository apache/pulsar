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
package org.apache.pulsar.common.configuration;

import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.isComplete;
import static org.apache.pulsar.common.configuration.PulsarConfigurationLoader.runtimeConfigurationOverrides;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class PulsarConfigurationLoaderTest {
    public static class MockConfiguration implements PulsarConfiguration {
        private Properties properties = new Properties();

        private String metadataStoreUrl = "zk:localhost:2181";
        private String configurationMetadataStoreUrl = "zk:localhost:2184";
        private Optional<Integer> brokerServicePort = Optional.of(7650);
        private Optional<Integer> brokerServicePortTls = Optional.of(7651);
        private Optional<Integer> webServicePort = Optional.of(9080);
        private Optional<Integer> webServicePortTls = Optional.of(9443);

        // This unused field is intentionally included to test the ignoreNonExistentMember feature of
        // PulsarConfigurationLoader.convertFrom()
        private static final String A_FIELD_THAT_IS_NOT_DECLARED_IN_ServiceConfiguration = "x";

        @Override
        public Properties getProperties() {
            return properties;
        }

        @Override
        public void setProperties(Properties properties) {
            this.properties = properties;
        }
    }

    @Test
    public void testConfigurationConverting() {
        MockConfiguration mockConfiguration = new MockConfiguration();
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(mockConfiguration);

        // check whether converting correctly
        assertEquals(serviceConfiguration.getMetadataStoreUrl(), "zk:localhost:2181");
        assertEquals(serviceConfiguration.getConfigurationMetadataStoreUrl(), "zk:localhost:2184");
        assertEquals(serviceConfiguration.getBrokerServicePort().get(), Integer.valueOf(7650));
        assertEquals(serviceConfiguration.getBrokerServicePortTls().get(), Integer.valueOf((7651)));
        assertEquals(serviceConfiguration.getWebServicePort().get(), Integer.valueOf((9080)));
        assertEquals(serviceConfiguration.getWebServicePortTls().get(), Integer.valueOf((9443)));
    }

    @Test
    public void testConfigurationConverting_checkNonExistMember() {
        assertThrows(IllegalArgumentException.class,
                () -> PulsarConfigurationLoader.convertFrom(new MockConfiguration(), false));
    }

    // PIP-478: a stale, removed PIP-337 sslFactoryPlugin key set to a non-default value is rejected loudly at
    // config-file load with an actionable migration message, replacing the removed @Deprecated fail-loud field.
    @Test
    public void testRemovedPip337SslFactoryPluginKeysRejectedLoudly() {
        for (String key : new String[] {"sslFactoryPlugin", "sslFactoryPluginParams",
                "brokerClientSslFactoryPlugin", "brokerClientSslFactoryPluginParams",
                // PIP-478 (FIX): broker.conf passes brokerClient_<clientKey> through to the internal client, so
                // the prefixed form of the client removed keys must be rejected too (it would otherwise be
                // silently dropped by ClientConfigurationData ignoreUnknown).
                "brokerClient_sslFactoryPlugin", "brokerClient_sslFactoryPluginParams"}) {
            Properties props = new Properties();
            props.setProperty("clusterName", "test");
            props.setProperty(key, "com.example.CustomSslFactory");
            IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                    () -> PulsarConfigurationLoader.create(props, ServiceConfiguration.class));
            assertTrue(ex.getMessage().contains(key), "message should name the removed key: " + ex.getMessage());
            assertTrue(ex.getMessage().contains("tlsFactoryClassName"),
                    "message should point to the successor: " + ex.getMessage());
        }
        // A blank value (the default) is tolerated -> loads cleanly.
        Properties blank = new Properties();
        blank.setProperty("clusterName", "test");
        blank.setProperty("sslFactoryPlugin", "");
        blank.setProperty("brokerClientSslFactoryPlugin", "");
        assertNotNull(expectNoThrow(() -> PulsarConfigurationLoader.create(blank, ServiceConfiguration.class)));
    }

    // PIP-478 (FIX): a *Plugin key still naming the OLD DEFAULT factory FQCN is equivalent to "unset" (no
    // custom factory), so it is tolerated — only a non-default (custom) value needs migration. The *PluginParams
    // keys carry no default value, so any non-blank value is still rejected.
    @Test
    public void testRemovedPip337DefaultSslFactoryFqcnTolerated() {
        String defaultFqcn = "org.apache.pulsar.common.util.DefaultPulsarSslFactory";
        for (String key : new String[] {"sslFactoryPlugin", "brokerClientSslFactoryPlugin",
                "brokerClient_sslFactoryPlugin"}) {
            Properties props = new Properties();
            props.setProperty("clusterName", "test");
            props.setProperty(key, defaultFqcn);
            assertNotNull(expectNoThrow(() -> PulsarConfigurationLoader.create(props, ServiceConfiguration.class)),
                    "old default factory FQCN on '" + key + "' should be tolerated");
        }
        // A custom (non-default) value on the same key is still rejected.
        Properties custom = new Properties();
        custom.setProperty("clusterName", "test");
        custom.setProperty("sslFactoryPlugin", "com.acme.CustomFactory");
        expectThrows(IllegalArgumentException.class,
                () -> PulsarConfigurationLoader.create(custom, ServiceConfiguration.class));
        // The default FQCN is NOT a *PluginParams default -> a non-blank params value is still rejected.
        Properties params = new Properties();
        params.setProperty("clusterName", "test");
        params.setProperty("sslFactoryPluginParams", defaultFqcn);
        expectThrows(IllegalArgumentException.class,
                () -> PulsarConfigurationLoader.create(params, ServiceConfiguration.class));
    }

    private static <T> T expectNoThrow(java.util.concurrent.Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            throw new AssertionError("expected no exception but got: " + e, e);
        }
    }

    // Deprecation warning suppressed as this test targets deprecated methods
    @SuppressWarnings("deprecation")
    @Test
    public void testPulsarConfigurationLoadingStream() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String metadataStoreUrl = "zk:z1.example.com,z2.example.com,z3.example.com";
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("metadataStoreUrl=" + metadataStoreUrl);
        printWriter.println("configurationMetadataStoreUrl=gz1.example.com,gz2.example.com,gz3.example.com/foo");
        printWriter.println("brokerDeleteInactiveTopicsEnabled=true");
        printWriter.println("statusFilePath=/tmp/status.html");
        printWriter.println("managedLedgerDefaultEnsembleSize=1");
        printWriter.println("backlogQuotaDefaultLimitGB=18");
        printWriter.println("clusterName=usc");
        printWriter.println("brokerClientAuthenticationPlugin=test.xyz.client.auth.plugin");
        printWriter.println("brokerClientAuthenticationParameters=role:my-role");
        printWriter.println("superUserRoles=appid1,appid2");
        printWriter.println("brokerServicePort=7777");
        printWriter.println("brokerServicePortTls=8777");
        printWriter.println("webServicePort=");
        printWriter.println("webServicePortTls=");
        printWriter.println("managedLedgerDefaultMarkDeleteRateLimit=5.0");
        printWriter.println("managedLedgerDigestType=CRC32C");
        printWriter.println("managedLedgerCacheSizeMB=");
        printWriter.println("bookkeeperDiskWeightBasedPlacementEnabled=true");
        printWriter.println("metadataStoreSessionTimeoutMillis=60");
        printWriter.println("metadataStoreOperationTimeoutSeconds=600");
        printWriter.println("metadataStoreCacheExpirySeconds=500");
        printWriter.close();
        testConfigFile.deleteOnExit();
        InputStream stream = new FileInputStream(testConfigFile);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getMetadataStoreUrl(), metadataStoreUrl);
        assertTrue(serviceConfig.isBrokerDeleteInactiveTopicsEnabled());

        assertEquals(serviceConfig.getBacklogQuotaDefaultLimitGB(), 18);
        assertEquals(serviceConfig.getClusterName(), "usc");
        assertEquals(serviceConfig.getBrokerClientAuthenticationParameters(), "role:my-role");
        assertEquals(serviceConfig.getBrokerServicePort().get(), Integer.valueOf((7777)));
        assertEquals(serviceConfig.getBrokerServicePortTls().get(), Integer.valueOf((8777)));
        assertFalse(serviceConfig.getWebServicePort().isPresent());
        assertFalse(serviceConfig.getWebServicePortTls().isPresent());
        assertEquals(serviceConfig.getManagedLedgerDigestType(), DigestType.CRC32C);
        assertTrue(serviceConfig.getManagedLedgerCacheSizeMB() > 0);
        assertTrue(serviceConfig.isBookkeeperDiskWeightBasedPlacementEnabled());
        assertEquals(serviceConfig.getMetadataStoreSessionTimeoutMillis(), 60);
        assertEquals(serviceConfig.getMetadataStoreOperationTimeoutSeconds(), 600);
        assertEquals(serviceConfig.getMetadataStoreCacheExpirySeconds(), 500);
    }

    @Test
    public void testPulsarConfigurationLoadingProp() throws Exception {
        final String zk = "zk:localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("metadataStoreUrl", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getMetadataStoreUrl(), zk);
    }

    @Test
    public void testPulsarConfigurationComplete() throws Exception {
        final String zk = "zk:localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("metadataStoreUrl", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        assertThrows(IllegalArgumentException.class, () -> isComplete(serviceConfig));
    }

    @Test
    public void testBackwardCompatibility() throws IOException {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)))) {
            printWriter.println("zooKeeperSessionTimeoutMillis=60");
            printWriter.println("zooKeeperOperationTimeoutSeconds=600");
            printWriter.println("zooKeeperCacheExpirySeconds=500");
        }
        testConfigFile.deleteOnExit();
        InputStream stream = new FileInputStream(testConfigFile);
        ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        stream.close();
        assertEquals(serviceConfig.getMetadataStoreSessionTimeoutMillis(), 60);
        assertEquals(serviceConfig.getMetadataStoreOperationTimeoutSeconds(), 600);
        assertEquals(serviceConfig.getMetadataStoreCacheExpirySeconds(), 500);

        testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)))) {
            printWriter.println("metadataStoreSessionTimeoutMillis=60");
            printWriter.println("metadataStoreOperationTimeoutSeconds=600");
            printWriter.println("metadataStoreCacheExpirySeconds=500");
            printWriter.println("zooKeeperSessionTimeoutMillis=-1");
            printWriter.println("zooKeeperOperationTimeoutSeconds=-1");
            printWriter.println("zooKeeperCacheExpirySeconds=-1");
        }
        testConfigFile.deleteOnExit();
        stream = new FileInputStream(testConfigFile);
        serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        stream.close();
        assertEquals(serviceConfig.getMetadataStoreSessionTimeoutMillis(), 60);
        assertEquals(serviceConfig.getMetadataStoreOperationTimeoutSeconds(), 600);
        assertEquals(serviceConfig.getMetadataStoreCacheExpirySeconds(), 500);

        testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)))) {
            printWriter.println("metadataStoreSessionTimeoutMillis=10");
            printWriter.println("metadataStoreOperationTimeoutSeconds=20");
            printWriter.println("metadataStoreCacheExpirySeconds=30");
            printWriter.println("zooKeeperSessionTimeoutMillis=100");
            printWriter.println("zooKeeperOperationTimeoutSeconds=200");
            printWriter.println("zooKeeperCacheExpirySeconds=300");
        }
        testConfigFile.deleteOnExit();
        stream = new FileInputStream(testConfigFile);
        serviceConfig = PulsarConfigurationLoader.create(stream, ServiceConfiguration.class);
        stream.close();
        assertEquals(serviceConfig.getMetadataStoreSessionTimeoutMillis(), 100);
        assertEquals(serviceConfig.getMetadataStoreOperationTimeoutSeconds(), 200);
        assertEquals(serviceConfig.getMetadataStoreCacheExpirySeconds(), 300);
    }

    @Test
    public void testComplete() throws Exception {
        TestCompleteObject complete = new TestCompleteObject();
        assertTrue(isComplete(complete));
    }

    @Test
    public void testIncomplete() throws IllegalAccessException {
        assertThrows(IllegalArgumentException.class, () -> isComplete(new TestInCompleteObjectRequired()));
        assertThrows(IllegalArgumentException.class, () -> isComplete(new TestInCompleteObjectMin()));
        assertThrows(IllegalArgumentException.class, () -> isComplete(new TestInCompleteObjectMax()));
        assertThrows(IllegalArgumentException.class, () -> isComplete(new TestInCompleteObjectMix()));
    }

    @Test
    public void testRuntimeConfigurationOverrides() {
        // A fresh configuration has no overrides.
        assertTrue(runtimeConfigurationOverrides(new ServiceConfiguration()).isEmpty());

        ServiceConfiguration config = new ServiceConfiguration();

        // String override.
        config.setClusterName("my-cluster");
        // int override.
        config.setManagedLedgerDefaultEnsembleSize(5);
        // long override.
        config.setMetadataStoreSessionTimeoutMillis(60_000L);
        // boolean override.
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        // double override.
        config.setManagedLedgerDefaultMarkDeleteRateLimit(5.0);
        // enum override.
        config.setManagedLedgerDigestType(DigestType.MAC);
        // Optional<Integer> override.
        config.setBrokerServicePort(Optional.of(7777));
        // Set<String> override.
        config.setSuperUserRoles(Sets.newHashSet("admin", "ops"));
        // Setting a field to its default value: should NOT appear in overrides.
        config.setNumIOThreads(config.getNumIOThreads());
        // Extra property not backed by a declared FieldContext field.
        config.getProperties().setProperty("custom.plugin.option", "enabled");

        Map<String, Object> overrides = runtimeConfigurationOverrides(config);

        assertEquals(overrides.get("clusterName"), "my-cluster");
        assertEquals(overrides.get("managedLedgerDefaultEnsembleSize"), 5);
        assertEquals(overrides.get("metadataStoreSessionTimeoutMillis"), 60_000L);
        assertEquals(overrides.get("brokerDeleteInactiveTopicsEnabled"), false);
        assertEquals(overrides.get("managedLedgerDefaultMarkDeleteRateLimit"), 5.0);
        assertEquals(overrides.get("managedLedgerDigestType"), DigestType.MAC);
        assertEquals(overrides.get("brokerServicePort"), Optional.of(7777));
        assertEquals(overrides.get("superUserRoles"), Sets.newHashSet("admin", "ops"));
        assertEquals(overrides.get("custom.plugin.option"), "enabled");

        // Unchanged fields must not appear.
        assertFalse(overrides.containsKey("numIOThreads"));
        assertFalse(overrides.containsKey("metadataStoreUrl"));
    }

    static class TestCompleteObject {
        @FieldContext(required = true)
        String required = "I am not null";
        @FieldContext(required = false)
        String optional;
        @FieldContext
        String optional2;
        @FieldContext(minValue = 1)
        int minValue = 2;
        @FieldContext(minValue = 1, maxValue = 3)
        int minMaxValue = 2;
    }

    static class TestInCompleteObjectRequired {
        @FieldContext(required = true)
        String inValidRequired;
    }

    static class TestInCompleteObjectMin {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
    }

    static class TestInCompleteObjectMax {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }

    static class TestInCompleteObjectMix {
        @FieldContext(required = true)
        String inValidRequired;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }
}
