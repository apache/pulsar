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
package org.apache.pulsar.packages.manager.storage.memory;

import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.PackageStorageConfig;
import org.apache.pulsar.packages.manager.exceptions.PackageManagerException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MemoryPackageStorage implements PackageStorage {

    private final ConcurrentHashMap<String, ByteBuffer> storage;
    private final MemoryPackageStorageConfig config;

    MemoryPackageStorage(PackageStorageConfig config) {
        this.config = (MemoryPackageStorageConfig) config;
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            storage.computeIfAbsent(path, store -> {
                ByteBuffer buffer = ByteBuffer.allocate(config.dataMaxSize);
                try (ReadableByteChannel channel = Channels.newChannel(inputStream)) {
                    channel.read(buffer);
                } catch (IOException e) {
                    future.completeExceptionally(new PackageManagerException.StorageException("Failed write package to buffer.", e));
                }
                return buffer;
            });
            future.complete(null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            ByteBuffer buffer = storage.get(path);
            if (buffer == null) {
                future.completeExceptionally(new PackageManagerException.PackageNotFoundException("Package is missing"));
                return;
            }
            try {
                outputStream.write(buffer.array());
            } catch (IOException e) {
                future.completeExceptionally(new PackageManagerException.StorageException("Read package failed.", e));
            }
            future.complete(null);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return CompletableFuture.runAsync(() -> {
            storage.remove(path);
        });
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        return CompletableFuture.supplyAsync(() -> {
            return storage.keySet().stream()
                .filter(key -> key.startsWith(path))
                .collect(Collectors.toList());
        });
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        return CompletableFuture.supplyAsync(() -> storage.get(path) == null);
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.runAsync(storage::clear);
    }
}
