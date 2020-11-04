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
package org.apache.pulsar.packages.manager.impl;

import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.manager.Package;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The implementation of package manager.
 */
public class PackageImpl implements Package {
    private final PackageStorage packageStorage;

    PackageImpl(PackageStorage packageStorage) {
        this.packageStorage = packageStorage;
    }

    @Override
    public CompletableFuture<PackageMetadata> getMeta(PackageName packageName) {
        return packageStorage.readAsync(getMetadataPath(packageName), )
    }

    @Override
    public CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata) {
        return null;
    }

    @Override
    public CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream) {
        return null;
    }

    @Override
    public CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream) {
        return null;
    }

    @Override
    public CompletableFuture<Void> delete(PackageName packageName) {
        return null;
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageName packageName) {
        return null;
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageType type, NamespaceName namespace) {
        return null;
    }

    private String getMetadataPath(PackageName packageName) {
        return "";
    }
}
