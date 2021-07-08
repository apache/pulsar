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
package org.apache.pulsar.broker.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.Getter;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Base class for all configuration resources to access configurations from metadata-store.
 *
 * @param <T>
 *            type of configuration-resources.
 */
public class BaseResources<T> {

    @Getter
    private final MetadataStore store;
    @Getter
    private final MetadataCache<T> cache;
    private int operationTimeoutSec;

    public BaseResources(MetadataStore store, Class<T> clazz, int operationTimeoutSec) {
        this.store = store;
        this.cache = store.getMetadataCache(clazz);
        this.operationTimeoutSec = operationTimeoutSec;
    }

    public BaseResources(MetadataStore store, TypeReference<T> typeRef, int operationTimeoutSec) {
        this.store = store;
        this.cache = store.getMetadataCache(typeRef);
        this.operationTimeoutSec = operationTimeoutSec;
    }

    public List<String> getChildren(String path) throws MetadataStoreException {
        try {
            return getChildrenAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to get childeren of " + path, e);
        }
    }

    public CompletableFuture<List<String>> getChildrenAsync(String path) {
        return cache.getChildren(path);
    }

    public Optional<T> get(String path) throws MetadataStoreException {
        try {
            return getAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to get data from " + path, e);
        }
    }

    public CompletableFuture<Optional<T>> getAsync(String path) {
        return cache.get(path);
    }

    public void set(String path, Function<T, T> modifyFunction) throws MetadataStoreException {
        try {
            setAsync(path, modifyFunction).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to set data for " + path, e);
        }
    }

    public CompletableFuture<Void> setAsync(String path, Function<T, T> modifyFunction) {
        return cache.readModifyUpdate(path, modifyFunction).thenApply(__ -> null);
    }

    public void setWithCreate(String path, Function<Optional<T>, T> createFunction) throws MetadataStoreException {
        try {
            setWithCreateAsync(path, createFunction).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to set/create " + path, e);
        }
    }

    public CompletableFuture<Void> setWithCreateAsync(String path, Function<Optional<T>, T> createFunction) {
        return cache.readModifyUpdateOrCreate(path, createFunction).thenApply(__ -> null);
    }

    public void create(String path, T data) throws MetadataStoreException {
        try {
            createAsync(path, data).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to create " + path, e);
        }
    }

    public CompletableFuture<Void> createAsync(String path, T data) {
        return cache.create(path, data);
    }

    public void delete(String path) throws MetadataStoreException {
        try {
            deleteAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to delete " + path, e);
        }
    }

    public CompletableFuture<Void> deleteAsync(String path) {
        return cache.delete(path);
    }

    public boolean exists(String path) throws MetadataStoreException {
        try {
            return existsAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to check exist " + path, e);
        }
    }

    public int getOperationTimeoutSec() {
        return operationTimeoutSec;
    }

    public CompletableFuture<Boolean> existsAsync(String path) {
        return cache.exists(path);
    }
}