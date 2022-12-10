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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
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
        private String A_FIELD_THAT_IS_NOT_DECLARED_IN_ServiceConfiguration = "x";

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
