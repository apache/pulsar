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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.discover.RegistrationClient;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LayoutManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LegacyHierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.exceptions.Code;
import org.apache.bookkeeper.meta.exceptions.MetadataException;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

public abstract class AbstractMetadataDriver implements Closeable {

    public static final String METADATA_STORE_SCHEME = "metadata-store";

    public static final String METADATA_STORE_INSTANCE = "metadata-store-instance";
    public static final long BLOCKING_CALL_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    protected MetadataStoreExtended store;
    private boolean storeInstanceIsOwned;

    protected RegistrationClient registrationClient;
    protected RegistrationManager registrationManager;
    protected LedgerManagerFactory ledgerManagerFactory;
    protected LayoutManager layoutManager;
    protected AbstractConfiguration conf;
    protected String ledgersRootPath;

    protected void initialize(AbstractConfiguration conf) throws MetadataException {
        this.conf = conf;
        this.ledgersRootPath = resolveLedgersRootPath();
        createMetadataStore();
        this.registrationClient = new PulsarRegistrationClient(store, ledgersRootPath);
        createRegistrationManager();
        this.layoutManager = new PulsarLayoutManager(store, ledgersRootPath);
        this.ledgerManagerFactory = new PulsarLedgerManagerFactory();

        try {
            ledgerManagerFactory.initialize(conf, layoutManager, LegacyHierarchicalLedgerManagerFactory.CUR_VERSION);
        } catch (IOException e) {
            throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
        }
    }

    public RegistrationManager createRegistrationManager() {
        if (registrationManager == null) {
            registrationManager = new PulsarRegistrationManager(store, ledgersRootPath, conf);
        }
        return registrationManager;
    }

    @SneakyThrows
    @Override
    public void close() {
        if (registrationClient != null) {
            registrationClient.close();
        }

        if (registrationManager != null) {
            registrationManager.close();
        }

        if (ledgerManagerFactory != null) {
            ledgerManagerFactory.close();
        }

        if (store != null && storeInstanceIsOwned) {
            store.close();
        }
    }

    void createMetadataStore() throws MetadataException {
        Object instance = conf.getProperty(METADATA_STORE_INSTANCE);
        if (instance != null) {
            // We have been passed a metadata store instance, so we're going to use that instead of creating a new
            // instance
            this.store = (MetadataStoreExtended) instance;
            this.storeInstanceIsOwned = false;
        } else {

            String url;
            try {
                url = conf.getMetadataServiceUri()
                        .replaceFirst(METADATA_STORE_SCHEME + ":", "")
                        .replace(";", ",");
            } catch (Exception e) {
                throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
            }
            try {
                this.store = MetadataStoreExtended.create(url,
                        MetadataStoreConfig.builder()
                                .sessionTimeoutMillis(conf.getZkTimeout())
                                .metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                                .build());
                this.storeInstanceIsOwned = true;
            } catch (MetadataStoreException e) {
                throw new MetadataException(Code.METADATA_SERVICE_ERROR, e);
            }
        }
    }

    public String getScheme() {
        return METADATA_STORE_SCHEME;
    }

    @SuppressWarnings("deprecation")
    private String resolveLedgersRootPath() {
        String metadataServiceUriStr = conf.getMetadataServiceUriUnchecked();
        if (metadataServiceUriStr == null) {
            return conf.getZkLedgersRootPath();
        }
        URI metadataServiceUri = URI.create(metadataServiceUriStr);
        String path = metadataServiceUri.getPath();
        return path == null ? BookKeeperConstants.DEFAULT_ZK_LEDGERS_ROOT_PATH : path;
    }
}
