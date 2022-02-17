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
package org.apache.pulsar.functions.instance.state;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.functions.api.StateStoreContext;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;

public class PulsarMetadataStateStoreImpl implements DefaultStateStore {

    private final MetadataStore store;
    private final String prefixPath;
    private final MetadataCache<Long> countersCache;

    private final String namespace;
    private final String tenant;
    private final String name;
    private final String fqsn;

    PulsarMetadataStateStoreImpl(MetadataStore store, String prefix, String tenant, String namespace, String name) {
        this.store = store;
        this.tenant = tenant;
        this.namespace = namespace;
        this.name = name;
        this.fqsn = tenant + '/' + namespace + '/' + name;

        this.prefixPath = prefix + '/' + fqsn + '/';
        this.countersCache = store.getMetadataCache(Long.class);
    }

    @Override
    public String tenant() {
        return tenant;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String fqsn() {
        return fqsn;
    }

    @Override
    public void init(StateStoreContext ctx) {
    }

    @Override
    public void close() {
    }

    @Override
    public void put(String key, ByteBuffer value) {
        putAsync(key, value).join();
    }

    @Override
    public CompletableFuture<Void> putAsync(String key, ByteBuffer value) {
        byte[] bytes = new byte[value.remaining()];
        value.get(bytes);
        return store.put(getPath(key), bytes, Optional.empty())
                .thenApply(__ -> null);
    }

    @Override
    public void delete(String key) {
        deleteAsync(key).join();
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String key) {
        return store.delete(getPath(key), Optional.empty());
    }

    @Override
    public ByteBuffer get(String key) {
        return getAsync(key).join();
    }

    @Override
    public CompletableFuture<ByteBuffer> getAsync(String key) {
        return store.get(getPath(key))
                .thenApply(optRes ->
                        optRes.map(x -> ByteBuffer.wrap(x.getValue()))
                                .orElse(null));
    }

    @Override
    public void incrCounter(String key, long amount) {
        incrCounterAsync(key, amount).join();
    }

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
        return countersCache.readModifyUpdateOrCreate(getPath(key), optValue ->
           optValue.orElse(0L) + amount
        ).thenApply(__ -> null);
    }

    @Override
    public long getCounter(String key) {
        return getCounterAsync(key).join();
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
        return countersCache.get(getPath(key))
                .thenApply(optValue -> optValue.orElse(0L));
    }

    private String getPath(String key) {
        return prefixPath + key;
    }
}
