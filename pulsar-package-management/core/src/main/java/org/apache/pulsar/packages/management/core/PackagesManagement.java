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
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.packages.management.core.common.PackageName;
import org.apache.pulsar.packages.management.core.common.PackageType;

/**
 * PackagesManagement provides a way to manage the packages of function, sink, and source.
 */
public interface PackagesManagement {
    /**
     * Initialize the packages management service with the given storage.
     *
     * @param storage
     *          the storage used to saving packages
     */
    void initialize(PackagesStorage storage);

    /**
     * Get the metadata of a package.
     *
     * @param packageName package name
     * @return
     */
    CompletableFuture<PackageMetadata> getMeta(PackageName packageName);

    /**
     * Update the metadata of a package.
     *
     * @param packageName package name
     * @param metadata
     * @return
     */
    CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata);

    /**
     * Download a package of a given version to a given path.
     *
     * @param packageName package name
     * @param outputStream
     * @return
     */
    CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream);

    /**
     * Upload a package of a given version from a given path.
     *
     * @param packageName package name
     * @param metadata metadata of a package
     * @param inputStream
     * @return
     */
    CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream);

    /**
     * Delete a package.
     * It will delete all versions of a package if the version is not specified.
     * Otherwise it will delete the specified version package.
     *
     * @param packageName package name
     *            type://tenant/namespace/name@version is delete a given version of the package
     *            type://tenant/namespace/name is delete all versions of the package
     * @return
     */
    CompletableFuture<Void> delete(PackageName packageName);

    /**
     * List all the versions of a package.
     *
     * @param packageName package name without version
     * @return
     *      all the versions of the specified package
     */
    CompletableFuture<List<String>> list(PackageName packageName);

    /**
     * List all the packages with the type of a namespace.
     *
     * @param type the package type
     * @param tenant the tenant name
     * @param namespace the namespace name
     * @return
     *      the packages name under the specified namespace
     */
    CompletableFuture<List<String>> list(PackageType type, String tenant, String namespace);
}
