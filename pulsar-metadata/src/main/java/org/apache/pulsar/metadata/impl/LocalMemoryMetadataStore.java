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
package org.apache.pulsar.metadata.impl;

import com.google.common.collect.MapMaker;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.CreateOption;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

@Slf4j
public class LocalMemoryMetadataStore extends AbstractMetadataStore implements MetadataStoreExtended {

    @Data
    private static class Value {
        final long version;
        final byte[] data;
        final long createdTimestamp;
        final long modifiedTimestamp;
        final boolean ephemeral;
    }

    private final NavigableMap<String, Value> map;
    private final AtomicLong sequentialIdGenerator;

    private static final Map<String, NavigableMap<String, Value>> STATIC_MAPS = new MapMaker()
            .weakValues().makeMap();
    private static final Map<String, AtomicLong> STATIC_ID_GEN_MAP = new MapMaker()
            .weakValues().makeMap();

    public LocalMemoryMetadataStore(String metadataURL, MetadataStoreConfig metadataStoreConfig)
            throws MetadataStoreException {
        URI uri;
        try {
            uri = new URI(metadataURL);
        } catch (URISyntaxException e) {
            throw new MetadataStoreException(e);
        }

        // Local means a private data set
        if ("local".equals(uri.getHost())) {
            map = new TreeMap<>();
            sequentialIdGenerator = new AtomicLong();
        } else {
            // Use a reference from a shared data set
            String name = uri.getHost();
            map = STATIC_MAPS.computeIfAbsent(name, __ -> new TreeMap<>());
            sequentialIdGenerator = STATIC_ID_GEN_MAP.computeIfAbsent(name, __ -> new AtomicLong());
            log.info("Created LocalMemoryDataStore for '{}'", name);
        }
    }

    @Override
    public CompletableFuture<Optional<GetResult>> get(String path) {
        synchronized (map) {
            if (!isValidPath(path)) {
                return FutureUtils.exception(new MetadataStoreException(""));
            }

            Value v = map.get(path);
            if (v != null) {
                return FutureUtils.value(
                        Optional.of(
                                new GetResult(v.data, new Stat(path, v.version, v.createdTimestamp, v.modifiedTimestamp,
                                        v.isEphemeral(), true))));
            } else {
                return FutureUtils.value(Optional.empty());
            }
        }
    }

    @Override
    public CompletableFuture<List<String>> getChildrenFromStore(String path) {
        synchronized (map) {
            if (!isValidPath(path)) {
                return FutureUtils.exception(new MetadataStoreException(""));
            }

            String firstKey = path.equals("/") ? path : path + "/";
            String lastKey = path.equals("/") ? "0" : path + "0"; // '0' is lexicographically just after '/'

            Set<String> children = new TreeSet<>();
            map.subMap(firstKey, false, lastKey, false).forEach((key, value) -> {
                String relativePath = key.replace(firstKey, "");

                // Only return first-level children
                String child = relativePath.split("/", 2)[0];
                children.add(child);
            });

            return FutureUtils.value(new ArrayList<>(children));
        }
    }

    @Override
    public CompletableFuture<Boolean> existsFromStore(String path) {
        synchronized (map) {
            if (!isValidPath(path)) {
                return FutureUtils.exception(new MetadataStoreException(""));
            }

            Value v = map.get(path);
            return FutureUtils.value(v != null ? true : false);
        }
    }

    @Override
    public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion) {
        return put(path, value, expectedVersion, EnumSet.noneOf(CreateOption.class));
    }

    @Override
    public CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> optExpectedVersion,
                                            EnumSet<CreateOption> options) {
        synchronized (map) {
            if (!isValidPath(path)) {
                return FutureUtils.exception(new MetadataStoreException(""));
            }

            boolean hasVersion = optExpectedVersion.isPresent();
            int expectedVersion = optExpectedVersion.orElse(-1L).intValue();

            if (options.contains(CreateOption.Sequential)) {
                path += Long.toString(sequentialIdGenerator.getAndIncrement());
            }

            long now = System.currentTimeMillis();

            if (hasVersion && expectedVersion == -1) {
                Value newValue = new Value(0, data, now, now, options.contains(CreateOption.Ephemeral));
                Value existingValue = map.putIfAbsent(path, newValue);
                if (existingValue != null) {
                    return FutureUtils.exception(new BadVersionException(""));
                } else {
                    receivedNotification(new Notification(NotificationType.Created, path));
                    notifyParentChildrenChanged(path);
                    return FutureUtils.value(new Stat(path, 0, now, now, newValue.isEphemeral(), true));
                }
            } else {
                Value existingValue = map.get(path);
                long existingVersion = existingValue != null ? existingValue.version : -1;
                if (hasVersion && expectedVersion != existingVersion) {
                    return FutureUtils.exception(new BadVersionException(""));
                } else {
                    long newVersion = existingValue != null ? existingValue.version + 1 : 0;
                    long createdTimestamp = existingValue != null ? existingValue.createdTimestamp : now;
                    Value newValue = new Value(newVersion, data, createdTimestamp, now,
                            options.contains(CreateOption.Ephemeral));
                    map.put(path, newValue);

                    NotificationType type =
                            existingValue == null ? NotificationType.Created : NotificationType.Modified;
                    receivedNotification(new Notification(type, path));
                    if (type == NotificationType.Created) {
                        notifyParentChildrenChanged(path);
                    }
                    return FutureUtils
                            .value(new Stat(path, newValue.version, newValue.createdTimestamp,
                                    newValue.modifiedTimestamp,
                                    false, true));
                }
            }
        }
    }

    @Override
    public CompletableFuture<Void> storeDelete(String path, Optional<Long> optExpectedVersion) {
        synchronized (map) {
            if (!isValidPath(path)) {
                return FutureUtils.exception(new MetadataStoreException(""));
            }

            Value value = map.get(path);
            if (value == null) {
                return FutureUtils.exception(new NotFoundException(""));
            } else if (optExpectedVersion.isPresent() && optExpectedVersion.get() != value.version) {
                return FutureUtils.exception(new BadVersionException(""));
            } else {
                map.remove(path);
                receivedNotification(new Notification(NotificationType.Deleted, path));

                notifyParentChildrenChanged(path);
                return FutureUtils.value(null);
            }
        }
    }

    private void notifyParentChildrenChanged(String path) {
        String parent = parent(path);
        while (parent != null) {
            receivedNotification(new Notification(NotificationType.ChildrenChanged, parent));
            parent = parent(parent);
        }
    }

    private static boolean isValidPath(String path) {
        if (path == null || !path.startsWith("/")) {
            return false;
        }

        return !path.equals("/") || !path.endsWith("/");
    }
}
