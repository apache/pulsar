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
package org.apache.pulsar.metadata.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreProvider;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class MetadataStoreFactoryImplTest {
    private static String originalMetadatastoreProvidersPropertyValue;

    @BeforeClass
    public void setMetadataStoreProperty() {
        originalMetadatastoreProvidersPropertyValue =
                System.getProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                MyMetadataStoreProvider.class.getName());
    }

    @AfterClass
    public void resetMetadataStoreProperty() {
        if (originalMetadatastoreProvidersPropertyValue != null) {
            System.setProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY,
                    originalMetadatastoreProvidersPropertyValue);
        } else {
            System.clearProperty(MetadataStoreFactoryImpl.METADATASTORE_PROVIDERS_PROPERTY);
        }
    }


    @Test
    public void testCreate() throws Exception{
        @Cleanup
        MetadataStore instance = MetadataStoreFactoryImpl.create(
                "custom://localhost",
                MetadataStoreConfig.builder().build());
        assertTrue(instance instanceof MyMetadataStore);
    }


    @Test
    public void testRemoveIdentifierFromMetadataURL() {
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("zk:host:port"), "host:port");
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("rocksdb:/data/dir"), "/data/dir");
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("etcd:host:port"), "host:port");
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("memory:name"), "name");
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("http://unknown/url/scheme"), "http://unknown/url/scheme");
        assertEquals(MetadataStoreFactoryImpl.removeIdentifierFromMetadataURL("custom:suffix"), "suffix");
    }

    public static class MyMetadataStoreProvider implements MetadataStoreProvider {

        @Override
        public String urlScheme() {
            return "custom";
        }

        @Override
        public MetadataStore create(String metadataURL, MetadataStoreConfig metadataStoreConfig,
                                    boolean enableSessionWatcher) throws MetadataStoreException {
            return new MyMetadataStore();
        }
    }

    public static class MyMetadataStore extends AbstractMetadataStore {
        protected MyMetadataStore() {
            super("custom");
        }

        @Override
        public CompletableFuture<List<String>> getChildrenFromStore(String path) {
            return null;
        }

        @Override
        protected CompletableFuture<Boolean> existsFromStore(String path) {
            return null;
        }

        @Override
        protected CompletableFuture<Optional<GetResult>> storeGet(String path) {
            return null;
        }

        @Override
        protected CompletableFuture<Void> storeDelete(String path, Optional<Long> expectedVersion) {
            return null;
        }

        @Override
        protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                                   EnumSet<CreateOption> options) {
            return null;
        }
    }


}
