/**
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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

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
    public class MockConfiguration implements PulsarConfiguration {
        private Properties properties = new Properties();

        private String zookeeperServers = "localhost:2181";
        private String configurationStoreServers = "localhost:2184";
        private Optional<Integer> brokerServicePort = Optional.of(7650);
        private Optional<Integer> brokerServicePortTls = Optional.of(7651);
        private Optional<Integer> webServicePort = Optional.of(9080);
        private Optional<Integer> webServicePortTls = Optional.of(9443);
        private int notExistFieldInServiceConfig = 0;

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
    public void testConfigurationConverting() throws Exception {
        MockConfiguration mockConfiguration = new MockConfiguration();
        ServiceConfiguration serviceConfiguration = PulsarConfigurationLoader.convertFrom(mockConfiguration);

        // check whether converting correctly
        assertEquals(serviceConfiguration.getMetadataStoreUrl(), "localhost:2181");
        assertEquals(serviceConfiguration.getConfigurationMetadataStoreUrl(), "localhost:2184");
        assertEquals(serviceConfiguration.getBrokerServicePort().get(), Integer.valueOf(7650));
        assertEquals(serviceConfiguration.getBrokerServicePortTls().get(), Integer.valueOf((7651)));
        assertEquals(serviceConfiguration.getWebServicePort().get(), Integer.valueOf((9080)));
        assertEquals(serviceConfiguration.getWebServicePortTls().get(), Integer.valueOf((9443)));

        // check whether exception causes
        try {
            PulsarConfigurationLoader.convertFrom(mockConfiguration, false);
            fail();
        } catch (Exception e) {
            assertEquals(e.getClass(), IllegalArgumentException.class);
        }
    }

    @Test
    public void testPulsarConfiguraitonLoadingStream() throws Exception {
        File testConfigFile = new File("tmp." + System.currentTimeMillis() + ".properties");
        if (testConfigFile.exists()) {
            testConfigFile.delete();
        }
        final String zkServer = "z1.example.com,z2.example.com,z3.example.com";
        PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(testConfigFile)));
        printWriter.println("zookeeperServers=" + zkServer);
        printWriter.println("configurationStoreServers=gz1.example.com,gz2.example.com,gz3.example.com/foo");
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
        assertEquals(serviceConfig.getMetadataStoreUrl(), zkServer);
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
    public void testPulsarConfiguraitonLoadingProp() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        assertNotNull(serviceConfig);
        assertEquals(serviceConfig.getMetadataStoreUrl(), zk);
    }

    @Test
    public void testPulsarConfiguraitonComplete() throws Exception {
        final String zk = "localhost:2184";
        final Properties prop = new Properties();
        prop.setProperty("zookeeperServers", zk);
        final ServiceConfiguration serviceConfig = PulsarConfigurationLoader.create(prop, ServiceConfiguration.class);
        try {
            isComplete(serviceConfig);
            fail("it should fail as config is not complete");
        } catch (IllegalArgumentException e) {
            // Ok
        }
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
        TestCompleteObject complete = this.new TestCompleteObject();
        assertTrue(isComplete(complete));
    }

    @Test
    public void testInComplete() throws IllegalAccessException {

        try {
            isComplete(this.new TestInCompleteObjectRequired());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMin());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMax());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }

        try {
            isComplete(this.new TestInCompleteObjectMix());
            fail("Should fail w/ illegal argument exception");
        } catch (IllegalArgumentException iae) {
            // OK, expected
        }
    }

    class TestCompleteObject {
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

    class TestInCompleteObjectRequired {
        @FieldContext(required = true)
        String inValidRequired;
    }

    class TestInCompleteObjectMin {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
    }

    class TestInCompleteObjectMax {
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }

    class TestInCompleteObjectMix {
        @FieldContext(required = true)
        String inValidRequired;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMin = 0;
        @FieldContext(minValue = 1, maxValue = 3)
        long inValidMax = 4;
    }
}
