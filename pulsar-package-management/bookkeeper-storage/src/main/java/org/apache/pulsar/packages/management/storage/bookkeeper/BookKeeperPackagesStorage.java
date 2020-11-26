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
package org.apache.pulsar.packages.management.storage.bookkeeper;

import org.apache.pulsar.packages.management.core.PackagesStorage;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BookKeeperPackagesStorage implements PackagesStorage {
    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        return null;
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        return null;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return null;
    }
}
