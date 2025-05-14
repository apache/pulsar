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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import io.etcd.jetcd.launcher.EtcdCluster;
import io.etcd.jetcd.test.EtcdClusterExtension;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.apache.pulsar.tests.TestRetrySupport;
import org.assertj.core.util.Files;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

public abstract class BaseMetadataStoreTest extends TestRetrySupport {
    // to debug specific implementations, set the TEST_METADATA_PROVIDERS environment variable
    // or temporarily hard code this value in the test class before running tests in the IDE
    // supported values are ZooKeeper,Memory,RocksDB,Etcd,Oxia,MockZooKeeper
    private static final String TEST_METADATA_PROVIDERS = System.getenv("TEST_METADATA_PROVIDERS");
    private static String originalMetadatastoreProvidersPropertyValue;
    protected TestZKServer zks;
    protected EtcdCluster etcdCluster;
    protected OxiaContainer oxiaServer;
    private String mockZkUrl;
    // reference to keep the MockZooKeeper instance alive in MockZookeeperMetadataStoreProvider
    private MetadataStore mockZkStoreRef;
    private String zksConnectionString;
    private String memoryConnectionString;
    private String rocksdbConnectionString;
    private File rocksDbDirectory;
    private boolean running;

    @BeforeClass(alwaysRun = true)
    @Override
    public void setup() throws Exception {
        running = true;
        incrementSetupNumber();
        zks = new TestZKServer();
        zksConnectionString = zks.getConnectionString();
        memoryConnectionString = "memory:" + UUID.randomUUID();
        rocksDbDirectory = Files.newTemporaryFolder().getAbsoluteFile();
        rocksdbConnectionString = "rocksdb:" + rocksDbDirectory;
        originalMetadatastoreProvidersPropertyValue =
                System.getProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        // register MockZooKeeperMetadataStoreProvider
        System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                MockZooKeeperMetadataStoreProvider.class.getName());
        mockZkUrl = "mock-zk:" + UUID.randomUUID();
        // create a reference in MockZooKeeperMetadataStoreProvider to keep the MockZooKeeper instance alive
        mockZkStoreRef = MetadataStoreFactory.create(mockZkUrl, MetadataStoreConfig.builder().build());
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        running = false;
        markCurrentSetupNumberCleaned();
        if (zks != null) {
            zks.close();
            zks = null;
        }

        if (etcdCluster != null) {
            etcdCluster.close();
            etcdCluster = null;
        }

        if (oxiaServer != null) {
            oxiaServer.close();
            oxiaServer = null;
        }

        if (mockZkStoreRef != null) {
            mockZkStoreRef.close();
            mockZkStoreRef = null;
            mockZkUrl = null;
        }

        if (rocksDbDirectory != null) {
            Files.delete(rocksDbDirectory);
            rocksDbDirectory = null;
        }

        if (originalMetadatastoreProvidersPropertyValue != null) {
            System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                    originalMetadatastoreProvidersPropertyValue);
        } else {
            System.clearProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        }
    }

    @DataProvider(name = "impl")
    public Object[][] implementations() {
        // If the environment variable TEST_METADATA_PROVIDERS is set, only run the specified implementations
        if (StringUtils.isNotBlank(TEST_METADATA_PROVIDERS)) {
            return filterImplementations(TEST_METADATA_PROVIDERS.split(","));
        }
        return allImplementations();
    }

    private Object[][] allImplementations() {
        // A Supplier<String> must be used for the Zookeeper connection string parameter. The retried test run will
        // use the same arguments as the failed attempt.
        // The Zookeeper test server gets restarted by TestRetrySupport before the retry.
        // The new connection string won't be available to the test method unless a
        // Supplier<String> lambda is used for providing the value.
        return new Object[][]{
                {"ZooKeeper", providerUrlSupplier(() -> zksConnectionString)},
                {"Memory", providerUrlSupplier(() -> memoryConnectionString)},
                {"RocksDB", providerUrlSupplier(() -> rocksdbConnectionString)},
                {"Etcd", providerUrlSupplier(() -> "etcd:" + getEtcdClusterConnectString(), "etcd:...")},
                {"Oxia", providerUrlSupplier(() -> "oxia://" + getOxiaServerConnectString(), "oxia://...")},
                {"MockZooKeeper", providerUrlSupplier(() -> mockZkUrl)},
        };
    }

    @DataProvider(name = "distributedImpl")
    public Object[][] distributedImplementations() {
        return filterImplementations("ZooKeeper", "Etcd", "Oxia");
    }

    @DataProvider(name = "zkImpls")
    public Object[][] zkImplementations() {
        return filterImplementations("ZooKeeper", "MockZooKeeper");
    }

    protected Object[][] filterImplementations(String... providers) {
        Set<String> providersSet = Set.of(providers);
        return Arrays.stream(allImplementations())
                .filter(impl -> providersSet.contains(impl[0]))
                .toArray(Object[][]::new);
    }

    protected synchronized String getOxiaServerConnectString() {
        if (!running) {
            return null;
        }
        if (oxiaServer == null) {
            oxiaServer = new OxiaContainer(OxiaContainer.DEFAULT_IMAGE_NAME);
            oxiaServer.start();
        }
        return oxiaServer.getServiceAddress();
    }

    private synchronized String getEtcdClusterConnectString() {
        if (!running) {
            return null;
        }
        if (etcdCluster == null) {
            etcdCluster = EtcdClusterExtension.builder().withClusterName("test").withNodes(1).withSsl(false).build()
                    .cluster();
            etcdCluster.start();
        }
        return etcdCluster.clientEndpoints().stream().map(URI::toString).collect(Collectors.joining(","));
    }

    private static Supplier<String> providerUrlSupplier(Supplier<String> supplier) {
        return new ProviderUrlSupplier(supplier);
    }

    // Use this method to provide a custom toString value for the Supplier<String>. Use this when Testcontainers is used
    // so that a toString call doesn't eagerly trigger container initialization which could cause a deadlock
    // with Gradle Develocity Maven Extension.
    private static Supplier<String> providerUrlSupplier(Supplier<String> supplier, String toStringValue) {
        return new ProviderUrlSupplier(supplier, Optional.ofNullable(toStringValue));
    }

    // Implements toString() so that the test name is more descriptive
    private static class ProviderUrlSupplier implements Supplier<String> {
        private final Supplier<String> supplier;
        private final Optional<String> toStringValue;

        ProviderUrlSupplier(Supplier<String> supplier) {
            this(supplier, Optional.empty());
        }

        ProviderUrlSupplier(Supplier<String> supplier, Optional<String> toStringValue) {
            this.supplier = supplier;
            this.toStringValue = toStringValue;
        }

        @Override
        public String get() {
            return supplier.get();
        }

        @Override
        public String toString() {
            // toStringValue is used to prevent deadlocks which could occur if toString method call eagerly triggers
            // Testcontainers initialization. This is the case when Gradle Develocity Maven Extension is used.
            return toStringValue.orElseGet(this::get);
        }
    }

    protected String newKey() {
        return "/key-" + System.nanoTime();
    }

    static void assertException(CompletionException e, Class<?> clazz) {
        assertException(e.getCause(), clazz);
    }

    static void assertException(Throwable t, Class<?> clazz) {
        assertTrue(clazz.isInstance(t), String.format("Exception %s is not of type %s", t.getClass(), clazz));
    }

    public static void assertEqualsAndRetry(Supplier<Object> actual,
                                            Object expected,
                                            Object expectedAndRetry) throws Exception {
        assertEqualsAndRetry(actual, expected, expectedAndRetry, 5, 100);
    }

    public static void assertEqualsAndRetry(Supplier<Object> actual,
                                            Object expected,
                                            Object expectedAndRetry,
                                            int retryCount,
                                            long intSleepTimeInMillis) throws Exception {
        assertTrue(retryStrategically((__) -> {
            Object actualObject = actual.get();
            if (actualObject.equals(expectedAndRetry)) {
                return false;
            }
            assertEquals(actualObject, expected);
            return true;
        }, retryCount, intSleepTimeInMillis));
    }

    public static boolean retryStrategically(Predicate<Void> predicate, int retryCount, long intSleepTimeInMillis)
            throws Exception {
        for (int i = 0; i < retryCount; i++) {
            if (predicate.test(null)) {
                return true;
            }
            Thread.sleep(intSleepTimeInMillis + (intSleepTimeInMillis * i));
        }
        return false;
    }

    /**
     * Delete all the empty container nodes
     * @param provider the metadata store provider
     * @throws Exception
     */
    protected void maybeTriggerDeletingEmptyContainers(String provider) throws Exception {
        if ("ZooKeeper".equals(provider) && zks != null) {
            zks.checkContainers();
        }
    }
}
