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
package org.apache.pulsar.metadata.bookkeeper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import io.opentelemetry.api.OpenTelemetry;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.AbstractMetadataStore;
import org.apache.pulsar.metadata.impl.MetadataStoreFactoryImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AbstractMetadataDriverTest {
    private static final String CAPTURING_METADATA_STORE_URL = "config-capture://host:4181/bookkeeper";

    private String originalMetadataStoreProviders;

    @BeforeClass
    public void addCapturingMetadataStoreProvider() {
        originalMetadataStoreProviders = System.getProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        String providers = CapturingMetadataStoreProvider.class.getName();
        if (originalMetadataStoreProviders != null && !originalMetadataStoreProviders.isBlank()) {
            providers = originalMetadataStoreProviders + "," + providers;
        }
        System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY, providers);
    }

    @AfterClass(alwaysRun = true)
    public void restoreMetadataStoreProviders() {
        if (originalMetadataStoreProviders != null) {
            System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                    originalMetadataStoreProviders);
        } else {
            System.clearProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        }
    }

    @Test
    public void testMetadataStoreConfigCanBeConfiguredWithMetadataServiceUriQuery() throws Exception {
        CapturingMetadataStoreProvider.reset();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkTimeout(12_345);
        conf.setMetadataServiceUri("metadata-store:" + CAPTURING_METADATA_STORE_URL
                + "?batchingEnabled=false"
                + "&batchingMaxDelayMillis=10"
                + "&batchingMaxOperations=11"
                + "&batchingMaxSizeKb=12"
                + "&numSerDesThreads=3"
                + "&sessionTimeoutMillis=40000"
                + "&allowReadOnlyOperations=true"
                + "&configFilePath=%2Ftmp%2Foxia-client.conf"
                + "&fsyncEnable=false");

        createMetadataStore(conf);

        assertThat(CapturingMetadataStoreProvider.metadataURL).isEqualTo(CAPTURING_METADATA_STORE_URL);
        MetadataStoreConfig metadataStoreConfig = CapturingMetadataStoreProvider.metadataStoreConfig;
        assertThat(metadataStoreConfig.getMetadataStoreName()).isEqualTo(MetadataStoreConfig.METADATA_STORE);
        assertThat(metadataStoreConfig.isBatchingEnabled()).isFalse();
        assertThat(metadataStoreConfig.getBatchingMaxDelayMillis()).isEqualTo(10);
        assertThat(metadataStoreConfig.getBatchingMaxOperations()).isEqualTo(11);
        assertThat(metadataStoreConfig.getBatchingMaxSizeKb()).isEqualTo(12);
        assertThat(metadataStoreConfig.getNumSerDesThreads()).isEqualTo(3);
        assertThat(metadataStoreConfig.getSessionTimeoutMillis()).isEqualTo(40_000);
        assertThat(metadataStoreConfig.isAllowReadOnlyOperations()).isTrue();
        assertThat(metadataStoreConfig.getConfigFilePath()).isEqualTo("/tmp/oxia-client.conf");
        assertThat(metadataStoreConfig.isFsyncEnable()).isFalse();
    }

    @Test
    public void testUnknownMetadataServiceUriQueryParamsArePassedToProvider() throws Exception {
        CapturingMetadataStoreProvider.reset();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setMetadataServiceUri("metadata-store:" + CAPTURING_METADATA_STORE_URL
                + "?providerParam=1"
                + "&batchingMaxDelayMillis=10"
                + "&providerFlag"
                + "&batchingMaxDelayMillis=20"
                + "&numSerDesThreads=3");

        createMetadataStore(conf);

        assertThat(CapturingMetadataStoreProvider.metadataURL)
                .isEqualTo(CAPTURING_METADATA_STORE_URL + "?providerParam=1&providerFlag");
        MetadataStoreConfig metadataStoreConfig = CapturingMetadataStoreProvider.metadataStoreConfig;
        assertThat(metadataStoreConfig.getBatchingMaxDelayMillis()).isEqualTo(20);
        assertThat(metadataStoreConfig.getNumSerDesThreads()).isEqualTo(3);
    }

    @Test
    public void testMetadataServiceUriWithoutQueryUsesExistingDefaults() throws Exception {
        CapturingMetadataStoreProvider.reset();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setZkTimeout(12_345);
        conf.setMetadataServiceUri("metadata-store:config-capture://host1:4181;host2:4181/bookkeeper");

        createMetadataStore(conf);

        assertThat(CapturingMetadataStoreProvider.metadataURL)
                .isEqualTo("config-capture://host1:4181,host2:4181/bookkeeper");
        MetadataStoreConfig metadataStoreConfig = CapturingMetadataStoreProvider.metadataStoreConfig;
        assertThat(metadataStoreConfig.getMetadataStoreName()).isEqualTo(MetadataStoreConfig.METADATA_STORE);
        assertThat(metadataStoreConfig.getSessionTimeoutMillis()).isEqualTo(12_345);
        assertThat(metadataStoreConfig.isBatchingEnabled()).isTrue();
        assertThat(metadataStoreConfig.getBatchingMaxDelayMillis()).isEqualTo(5);
        assertThat(metadataStoreConfig.getBatchingMaxOperations()).isEqualTo(1_000);
        assertThat(metadataStoreConfig.getBatchingMaxSizeKb()).isEqualTo(128);
        assertThat(metadataStoreConfig.getNumSerDesThreads()).isEqualTo(1);
    }

    @Test(dataProvider = "invalidMetadataStoreConfigQueryParams")
    public void testInvalidMetadataStoreConfigQueryParamFails(String query, String invalidParameter,
                                                             boolean expectBackendUrlInErrorMessage) {
        assertInvalidConfigQuery(query, invalidParameter, expectBackendUrlInErrorMessage);
    }

    private static void assertInvalidConfigQuery(String query, String invalidParameter,
                                                 boolean expectBackendUrlInErrorMessage) {
        ClientConfiguration conf = new ClientConfiguration();
        String metadataServiceUri = "metadata-store:" + CAPTURING_METADATA_STORE_URL + query;
        conf.setMetadataServiceUri(metadataServiceUri);

        Throwable error = catchThrowable(() -> createMetadataStore(conf));

        assertThat(error)
                .isInstanceOf(MetadataException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("BookKeeper metadata store")
                .hasMessageContaining("metadataServiceUri")
                .hasMessageContaining(metadataServiceUri);
        if (expectBackendUrlInErrorMessage) {
            assertThat(error).hasMessageContaining(CAPTURING_METADATA_STORE_URL);
        }
        assertThat(error.getCause()).hasMessageContaining(invalidParameter);
    }

    @DataProvider(name = "invalidMetadataStoreConfigQueryParams")
    public Object[][] invalidMetadataStoreConfigQueryParams() {
        return new Object[][] {
                {"?batchingEnabled=maybe", "batchingEnabled", true},
                {"?batchingMaxDelayMillis=-1", "batchingMaxDelayMillis", true},
                {"?batchingMaxOperations=0", "batchingMaxOperations", true},
                {"?numSerDesThreads=abc", "numSerDesThreads", true},
                {"?batchingEnabled", "batchingEnabled", false}
        };
    }

    @Test
    public void testMetadataStoreInstancePathIgnoresMetadataServiceUriQuery() throws Exception {
        CapturingMetadataStoreProvider.reset();
        ClientConfiguration conf = new ClientConfiguration();
        MetadataStore sharedStore = new CapturingMetadataStore();
        conf.setProperty(AbstractMetadataDriver.METADATA_STORE_INSTANCE, sharedStore);
        conf.setMetadataServiceUri("metadata-store:" + CAPTURING_METADATA_STORE_URL
                + "?unknown=1&batchingMaxDelayMillis=bad");

        TestMetadataDriver driver = new TestMetadataDriver();
        try {
            driver.createMetadataStore(conf);

            assertThat(driver.store()).isSameAs(sharedStore);
            assertThat(CapturingMetadataStoreProvider.metadataURL).isNull();
            assertThat(CapturingMetadataStoreProvider.metadataStoreConfig).isNull();
        } finally {
            driver.close();
        }
    }

    @SuppressWarnings("rawtypes")
    private static void createMetadataStore(AbstractConfiguration conf) throws MetadataException {
        TestMetadataDriver driver = new TestMetadataDriver();
        try {
            driver.createMetadataStore(conf);
        } finally {
            driver.close();
        }
    }

    @SuppressWarnings("rawtypes")
    private static class TestMetadataDriver extends AbstractMetadataDriver {
        private void createMetadataStore(AbstractConfiguration conf) throws MetadataException {
            this.conf = conf;
            super.createMetadataStore();
        }

        private MetadataStore store() {
            return store;
        }
    }

    public static class CapturingMetadataStoreProvider implements MetadataStoreProvider {
        private static volatile String metadataURL;
        private static volatile MetadataStoreConfig metadataStoreConfig;

        static void reset() {
            metadataURL = null;
            metadataStoreConfig = null;
        }

        @Override
        public String urlScheme() {
            return "config-capture";
        }

        @Override
        public MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                    boolean enableSessionWatcher) throws MetadataStoreException {
            CapturingMetadataStoreProvider.metadataURL = metadataURL;
            CapturingMetadataStoreProvider.metadataStoreConfig = metadataStoreConfig;
            return new CapturingMetadataStore();
        }
    }

    private static class CapturingMetadataStore extends AbstractMetadataStore {
        private CapturingMetadataStore() {
            super("config-capture", OpenTelemetry.noop(), null, 1);
        }

        @Override
        protected CompletableFuture<Boolean> existsFromStore(String path, Set<Option> opts) {
            return CompletableFuture.completedFuture(false);
        }

        @Override
        public CompletableFuture<List<String>> getChildrenFromStore(String path, Set<Option> opts) {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        protected CompletableFuture<Optional<GetResult>> storeGet(String path, Set<Option> opts) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        @Override
        protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion, Set<Option> opts) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                   Set<Option> opts) {
            return CompletableFuture.completedFuture(new Stat(path, 0, 0, 0, false, false, false));
        }
    }
}
