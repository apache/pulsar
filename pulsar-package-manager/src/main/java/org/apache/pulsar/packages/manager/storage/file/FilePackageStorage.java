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
package org.apache.pulsar.packages.manager.storage.file;

import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.storage.exception.StorageException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * File package storage will save all the packages to a configured path.
 */
public class FilePackageStorage implements PackageStorage {
    private final FilePackageStorageConfig config;

    public FilePackageStorage(FilePackageStorageConfig config) {
        this.config = config;
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Files.copy(inputStream, Paths.get(config.directory, path), StandardCopyOption.ATOMIC_MOVE);
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(new StorageException("Write package failed.", e));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Files.copy(Paths.get(config.getDirectory(), path), outputStream);
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(new StorageException("Read package failed.", e));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Files.delete(Paths.get(config.getDirectory(), path));
            future.complete(null);
        } catch (IOException e) {
            future.completeExceptionally(new StorageException("Delete package failed.", e));
        }
        return future;
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        try {
            List<String> files = Files.list(Paths.get(config.getDirectory(), path))
                .map(Path::toString).collect(Collectors.toList());
            future.complete(files);
        } catch (IOException e) {
            future.completeExceptionally(new StorageException("List packages failed.", e));
        }
        return future;
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        return CompletableFuture.completedFuture(
            Files.exists(Paths.get(config.getDirectory(), path), LinkOption.NOFOLLOW_LINKS));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }
}
