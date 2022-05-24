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

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface PackagesStorage {
    /**
     * Initialize the packages management service with the given storage.
     */
    void initialize();
    /**
     * Write a input stream to a path.
     *
     * @param path
     *          file path
     * @param inputStream
     *          the input file stream
     * @return
     */
    CompletableFuture<Void> writeAsync(String path, InputStream inputStream);

    /**
     * Read a file to a output stream.
     *
     * @param path
     *          file path
     * @param outputStream
     *          the output file stream
     * @return
     */
    CompletableFuture<Void> readAsync(String path, OutputStream outputStream);

    /**
     * Delete a file.
     *
     * @param path
     *          file path
     * @return
     */
    CompletableFuture<Void> deleteAsync(String path);

    /**
     * List all the file under a path.
     *
     * @param path
     *          file path
     * @return
     */
    CompletableFuture<List<String>> listAsync(String path);

    /**
     * Check the file is or not exists.
     *
     * @param path
     *          file path
     * @return
     */
    CompletableFuture<Boolean> existAsync(String path);

    /**
     * Close storage asynchronously.
     *
     * @return
     */
    CompletableFuture<Void> closeAsync();

    /**
     * The extra path for saving package data.
     *
     * For example, we have a package function://public/default/package@v0.1,
     * it will save the meta to the path function/public/default/package/v0.1/meta,
     * and save the data to the path function/public/default/package/v0.1.
     * By default, we are using distributed log as the package storage, and it supports
     * saving data at a directory.
     * But some storage like filesystem don't have the similar ability, it needs another path
     * for saving the data.
     * This api provides the ability to support saving the data in another place.
     * If you specify the data path as `/data`, the package will saved into
     * function/public/default/package/v0.1/data.
     *
     * @return
     *      the data path
     */
    default String dataPath() {
        return "";
    }
}
