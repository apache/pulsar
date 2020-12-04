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
package org.apache.pulsar.client.admin;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;

/**
 * Administration operations of the packages management service.
 */
public interface Packages {
    /**
     * Get a package metadata information.
     *
     * @param packageName
     *          the package name of the package metadata you want to find
     * @return the package metadata information
     */
    PackageMetadata getMetadata(String packageName) throws PulsarAdminException;

    /**
     * Get a package metadata information asynchronously.
     *
     * @param packageName
     *          the package name of the package metadata you want to find
     * @return  the package metadata information
     */
    CompletableFuture<PackageMetadata> getMetadataAsync(String packageName);

    /**
     * Update a package metadata information.
     *
     * @param packageName
     *          the package name of the package metadata you want to update
     * @param metadata
     *          the updated metadata information
     */
    void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException;

    /**
     * Update a package metadata information asynchronously.
     *
     * @param packageName
     *          the package name of the package metadata you want to update
     * @param metadata
     *          the updated metadata information
     * @return nothing
     */
    CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata);

    /**
     * Upload a package to the package management service.
     *
     * @param packageName
     *          the package name of the upload file
     * @param path
     *          the upload file path
     */
    void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException;

    /**
     * Upload a package to the package management service asynchronously.
     *
     * @param packageName
     *          the package name you want to upload
     * @param path
     *          the path you want to upload from
     * @return nothing
     */
    CompletableFuture<Void> uploadAsync(PackageMetadata metadata, String packageName, String path);

    /**
     * Download a package from the package management service.
     *
     * @param packageName
     *          the package name you want to download
     * @param path
     *          the path you want to download to
     */
    void download(String packageName, String path) throws PulsarAdminException;

    /**
     * Download a package from the package management service asynchronously.
     *
     * @param packageName
     *          the package name you want to download
     * @param path
     *          the path you want to download to
     * @return nothing
     */
    CompletableFuture<Void> downloadAsync(String packageName, String path);

    /**
     * Delete the specified package.
     *
     * @param packageName
     *          the package name which you want to delete
     */
    void delete(String packageName) throws PulsarAdminException;

    /**
     * Delete the specified package asynchronously.
     *
     * @param packageName
     *          the package name which you want to delete
     * @return nothing
     */
    CompletableFuture<Void> deleteAsync(String packageName);

    /**
     * List all the versions of a package.
     *
     * @param packageName
     *          the package name which you want to get all the versions
     * @return all the versions of the package
     */
    List<String> listPackageVersions(String packageName) throws PulsarAdminException;

    /**
     * List all the versions of a package asynchronously.
     *
     * @param packageName
     *          the package name which you want to get all the versions
     * @return all the versions of the package
     */
    CompletableFuture<List<String>> listPackageVersionsAsync(String packageName);

    /**
     * List all the packages with the given type in a namespace.
     *
     * @param type
     *          the type you want to get the packages
     * @param namespace
     *          the namespace you want to get the packages
     * @return all the packages of the given type which in the given namespace
     */

    List<String> listPackages(String type, String namespace) throws PulsarAdminException;
    /**
     * List all the packages with the given type in a namespace asynchronously.
     *
     * @param type
     *          the type you want to get the packages
     * @param namespace
     *          the namespace you want to get the packages
     * @return all the packages of the given type which in the given namespace
     */
    CompletableFuture<List<String>> listPackagesAsync(String type, String namespace);
}
