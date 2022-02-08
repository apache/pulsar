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
package org.apache.pulsar.functions.config;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

public class TestWorkerConfig {
    @Test
    public void validateAuthenticationCompatibleWorkerConfig() {
        WorkerConfig workerConfig = new WorkerConfig();

        workerConfig.setAuthenticationEnabled(false);
        assertFalse(workerConfig.isAuthenticationEnabled());
        workerConfig.setBrokerClientAuthenticationEnabled(null);
        assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setAuthenticationEnabled(true);
        assertTrue(workerConfig.isAuthenticationEnabled());
        workerConfig.setBrokerClientAuthenticationEnabled(null);
        assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setBrokerClientAuthenticationEnabled(true);
        workerConfig.setAuthenticationEnabled(false);
        assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());
        workerConfig.setAuthenticationEnabled(true);
        assertTrue(workerConfig.isBrokerClientAuthenticationEnabled());

        workerConfig.setBrokerClientAuthenticationEnabled(false);
        workerConfig.setAuthenticationEnabled(false);
        assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());
        workerConfig.setAuthenticationEnabled(true);
        assertFalse(workerConfig.isBrokerClientAuthenticationEnabled());
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
        WorkerConfig serviceConfig = PulsarConfigurationLoader.create(stream, WorkerConfig.class);
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
        serviceConfig = PulsarConfigurationLoader.create(stream, WorkerConfig.class);
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
        serviceConfig = PulsarConfigurationLoader.create(stream, WorkerConfig.class);
        stream.close();
        assertEquals(serviceConfig.getMetadataStoreSessionTimeoutMillis(), 100);
        assertEquals(serviceConfig.getMetadataStoreOperationTimeoutSeconds(), 200);
        assertEquals(serviceConfig.getMetadataStoreCacheExpirySeconds(), 300);
    }
}
