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
package org.apache.pulsar.broker.resources;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException;

/**
 * Base class for all configuration resources to access configurations from metadata-store.
 *
 * @param <T>
 *            type of configuration-resources.
 */
@Slf4j
public class BaseResources<T> {

    protected static final String BASE_POLICIES_PATH = "/admin/policies";
    protected static final String BASE_CLUSTERS_PATH = "/admin/clusters";
    protected static final String LOCAL_POLICIES_ROOT = "/admin/local-policies";

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

    protected List<String> getChildren(String path) throws MetadataStoreException {
        try {
            return getChildrenAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to get children of " + path, e);
        }
    }

    protected CompletableFuture<List<String>> getChildrenAsync(String path) {
        return cache.getChildren(path);
    }

    protected CompletableFuture<List<String>> getChildrenRecursiveAsync(String path) {
        Set<String> children = ConcurrentHashMap.newKeySet();
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        getChildrenRecursiveAsync(path, children, result, new AtomicInteger(1), path);
        return result;
    }

    private void getChildrenRecursiveAsync(String path, Set<String> children, CompletableFuture<List<String>> result,
            AtomicInteger totalResults, String parent) {
        cache.getChildren(path).thenAccept(childList -> {
            childList = childList != null ? childList : Collections.emptyList();
            if (totalResults.decrementAndGet() == 0 && childList.isEmpty()) {
                result.complete(new ArrayList<>(children));
                return;
            }
            if (childList.isEmpty()) {
                return;
            }
            // remove current node from children if current node is not leaf
            children.remove(parent);
            // childPrefix creates a path hierarchy if children has multi level path
            String childPrefix = path.equals(parent) ? "" : parent + "/";
            totalResults.addAndGet(childList.size());
            for (String child : childList) {
                children.add(childPrefix + child);
                String childPath = path + "/" + child;
                getChildrenRecursiveAsync(childPath, children, result, totalResults, child);
            }
        });
    }

    protected Optional<T> get(String path) throws MetadataStoreException {
        try {
            return getAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to get data from " + path, e);
        }
    }

    protected CompletableFuture<Optional<T>> getAsync(String path) {
        return cache.get(path);
    }

    protected CompletableFuture<Optional<T>> refreshAndGetAsync(String path) {
        return store.sync(path).thenCompose(___ -> {
            cache.invalidate(path);
            return cache.get(path);
        });
    }

    protected void set(String path, Function<T, T> modifyFunction) throws MetadataStoreException {
        try {
            setAsync(path, modifyFunction).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to set data for " + path, e);
        }
    }

    protected CompletableFuture<Void> setAsync(String path, Function<T, T> modifyFunction) {
        return cache.readModifyUpdate(path, modifyFunction).thenApply(__ -> null);
    }

    protected void setWithCreate(String path, Function<Optional<T>, T> createFunction) throws MetadataStoreException {
        try {
            setWithCreateAsync(path, createFunction).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to set/create " + path, e);
        }
    }

    protected CompletableFuture<Void> setWithCreateAsync(String path, Function<Optional<T>, T> createFunction) {
        return cache.readModifyUpdateOrCreate(path, createFunction).thenApply(__ -> null);
    }

    protected void create(String path, T data) throws MetadataStoreException {
        try {
            createAsync(path, data).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to create " + path, e);
        }
    }

    protected CompletableFuture<Void> createAsync(String path, T data) {
        return cache.create(path, data);
    }

    protected void delete(String path) throws MetadataStoreException {
        try {
            deleteAsync(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to delete " + path, e);
        }
    }

    protected CompletableFuture<Void> deleteAsync(String path) {
        return cache.delete(path);
    }

    protected CompletableFuture<Void> deleteIfExistsAsync(String path) {
        return cache.exists(path).thenCompose(exists -> {
            if (!exists) {
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            cache.delete(path).whenComplete((ignore, ex) -> {
                if (ex != null && ex.getCause() instanceof MetadataStoreException.NotFoundException) {
                    future.complete(null);
                } else if (ex != null) {
                    future.completeExceptionally(ex);
                } else {
                    future.complete(null);
                }
            });
            return future;
        });
    }

    protected boolean exists(String path) throws MetadataStoreException {
        try {
            return cache.exists(path).get(operationTimeoutSec, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw (e.getCause() instanceof MetadataStoreException) ? (MetadataStoreException) e.getCause()
                    : new MetadataStoreException(e.getCause());
        } catch (Exception e) {
            throw new MetadataStoreException("Failed to check exist " + path, e);
        }
    }

    protected CompletableFuture<Boolean> existsAsync(String path) {
        return cache.exists(path);
    }

    public int getOperationTimeoutSec() {
        return operationTimeoutSec;
    }

    protected static String joinPath(String... parts) {
        StringBuilder sb = new StringBuilder();
        Joiner.on('/').appendTo(sb, parts);
        return sb.toString();
    }
}