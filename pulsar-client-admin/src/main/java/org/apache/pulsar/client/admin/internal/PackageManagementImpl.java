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
package org.apache.pulsar.client.admin.internal;

import org.apache.pulsar.client.admin.PackageManagement;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PackageManagementImpl implements PackageManagement {
    @Override
    public PackageMetadata getMetadata(String packageName) {
        return null;
    }

    @Override
    public CompletableFuture<PackageMetadta> getMetadataAsync(String packageName) {
        return null;
    }

    @Override
    public void updateMetadata(String packageName, PackageMetadata metadata) {

    }

    @Override
    public CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata) {
        return null;
    }

    @Override
    public void upload() {

    }

    @Override
    public CompletableFuture<Void> uploadAsync() {
        return null;
    }

    @Override
    public CompletableFuture<Void> downloadAsync() {
        return null;
    }

    @Override
    public List<String> listPackageVersions(String packageName) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> listPackageVersionsAsync(String packageName) {
        return null;
    }

    @Override
    public List<String> listPackages(String type, String namespace) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> listPackagesAsync(String type, String namespace) {
        return null;
    }
}
