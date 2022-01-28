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
package org.apache.pulsar.packages.management.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class MockedPackagesStorage implements PackagesStorage {
    private PackagesStorageConfiguration configuration;

    private ConcurrentHashMap<String, byte[]> storage = new ConcurrentHashMap<>();

    MockedPackagesStorage(PackagesStorageConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void initialize() {

    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                byte[] bytes = new byte[inputStream.available()];
                inputStream.read(bytes);
                storage.put(path, bytes);
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            byte[] bytes = storage.get(path);
            if (bytes == null) {
                future.completeExceptionally(
                    new Exception(String.format("Path '%s' does not exist", path)));
                return;
            }
            try {
                outputStream.write(bytes);
                outputStream.flush();
                future.complete(null);
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        storage.remove(path);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        return CompletableFuture.completedFuture(storage.keySet().stream()
            .filter(s -> s.startsWith(path))
            .map(s -> s.substring(path.length()))
            .map(s -> s.split("/")[1])
            .distinct()
            .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        return CompletableFuture.completedFuture(storage.keySet().stream()
            .filter(s -> s.startsWith(path))
            .collect(Collectors.toList()))
            .thenApply(paths -> !paths.isEmpty());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        storage.clear();
        return CompletableFuture.completedFuture(null);
    }
}
