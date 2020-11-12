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
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.packages.manager.PackageMetadata;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PackageManagementImpl extends BaseResource implements PackageManagement {

    private final WebTarget packageManagement;

    public PackageManagementImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.packageManagement = web.path("/admin/v3/packages");
    }

    @Override
    public PackageMetadata getMetadata(String packageName) {
        return null;
    }

    @Override
    public CompletableFuture<PackageMetadata> getMetadataAsync(String packageName) {
        WebTarget path = packageManagement.path(packageName);
        return asyncGetRequest(path);
    }

    @Override
    public void updateMetadata(String packageName, PackageMetadata metadata) {
        WebTarget path = packageManagement.path(packageName);

    }

    @Override
    public CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata) {
        WebTarget path = packageManagement.path(packageName);
        return asyncPutRequest(path, Entity.entity(metadata, MediaType.APPLICATION_JSON));
    }

    @Override
    public void upload(String packageName, String path) {
        uploadAsync(packageName, path);
    }

    @Override
    public CompletableFuture<Void> uploadAsync(String packageName, String path) {
        return null;
    }

    @Override
    public void download(String packageName, String path) {

    }

    @Override
    public CompletableFuture<Void> downloadAsync(String packageName, String path) {
        return null;
    }

    @Override
    public List<String> listPackageVersions(String packageName) {
        try {
            return listPackageVersionsAsync(packageName).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public CompletableFuture<List<String>> listPackageVersionsAsync(String packageName) {
        WebTarget path = packageManagement.path();
        return asyncGetRequest(path);
    }

    @Override
    public List<String> listPackages(String type, String namespace) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> listPackagesAsync(String type, String namespace) {
        WebTarget path = packageManagement.path();
        return asyncGetRequest(path);
    }
}
