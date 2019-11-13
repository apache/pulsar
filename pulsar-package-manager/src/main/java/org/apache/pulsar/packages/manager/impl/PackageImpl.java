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

import com.google.gson.Gson;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.Package;
import org.apache.pulsar.packages.manager.exception.PackageAlreadyExistsException;
import org.apache.pulsar.packages.manager.exception.PackageMetaAlreadyExistsException;
import org.apache.pulsar.packages.manager.exception.PackageMetaNotFoundException;
import org.apache.pulsar.packages.manager.exception.PackageNotFoundException;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;

/**
 * The implementation of package management.
 */
public class PackageImpl implements Package {

    private final PackageStorage packageStorage;

    public PackageImpl(PackageStorage packageStorage) {
        this.packageStorage = packageStorage;
    }

    @Override
    public CompletableFuture<PackageMetadata> getMeta(PackageName packageName) {
        String metadataPath = getMetadataStoragePath(packageName);
        return packageStorage.existAsync(metadataPath)
            .thenCompose(exists -> exists
                ? packageStorage.readAsync(metadataPath)
                : FutureUtil.failedFuture(new PackageMetaNotFoundException("Package metadata does not existAsync")))
            .thenApply(meta -> new Gson().fromJson(new String(meta), PackageMetadata.class));
    }

    @Override
    public CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata) {
        String metadataPath = getMetadataStoragePath(packageName);
        return packageStorage.existAsync(metadataPath)
            .thenCompose(exists -> exists
                ? packageStorage.writeAsync(metadataPath, new Gson().toJson(metadata).getBytes())
                : FutureUtil.failedFuture(new PackageMetaNotFoundException("Package metadata does not existAsync")));
    }

    @Override
    public CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream) {
        String pacakgePath = getPackageStoragePath(packageName);
        return packageStorage.existAsync(pacakgePath)
            .thenCompose(exists -> exists
                ? packageStorage.readAsync(pacakgePath, outputStream)
                : FutureUtil.failedFuture(new PackageNotFoundException("Package does not existAsync")));
    }

    @Override
    public CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream) {
        String metadataPath = getMetadataStoragePath(packageName);
        String packagePath = getPackageStoragePath(packageName);
        return packageStorage.existAsync(metadataPath)
            .thenCompose(metaExists -> metaExists
                ? FutureUtil.failedFuture(new PackageMetaAlreadyExistsException("Package metadata already exists"))
                : packageStorage.existAsync(packagePath))
            .thenCompose(dataExists -> dataExists
                ? FutureUtil.failedFuture(new PackageAlreadyExistsException("Package already exists"))
                : packageStorage.writeAsync(metadataPath, new Gson().toJson(metadata).getBytes()))
            .thenCompose(ignore -> packageStorage.writeAsync(packagePath, inputStream));
    }

    @Override
    public CompletableFuture<Void> delete(PackageName packageName) {
        String metadataPath = getMetadataStoragePath(packageName);
        String packagePath = getPackageStoragePath(packageName);
        return packageStorage.existAsync(metadataPath)
            .thenCompose(metaExists -> metaExists
                ? packageStorage.deleteAsync(metadataPath)
                : FutureUtil.failedFuture(new PackageMetaNotFoundException("Package metadata does not exists")))
            .thenCompose(ignore -> packageStorage.existAsync(packagePath))
            .thenCompose(dataExists -> dataExists
                ? packageStorage.deleteAsync(packagePath)
                : FutureUtil.failedFuture(new PackageNotFoundException("Package does not existAsync")));
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageName packageName) {
        String packageWithoutVersionPath = getPackagePathWithoutVersion(packageName);
        return packageStorage.existAsync(packageWithoutVersionPath)
            .thenCompose(exists -> exists
                ? packageStorage.listAsync(packageWithoutVersionPath)
                : FutureUtil.failedFuture(new PackageNotFoundException("Package does not existAsync")))
            .thenApply(names -> names.stream().map(PackageName::get).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageType type, NamespaceName namespace) {
        String path = type + "/" + namespace.toString();
        return packageStorage.existAsync(path)
            .thenCompose(exists -> exists
                ? packageStorage.listAsync(path)
                : FutureUtil.failedFuture(new PackageNotFoundException(
                    "There are no " + type.toString() + " packages in the namespace " +  namespace.toString())))
            .thenApply(names -> names.stream().map(PackageName::get).collect(Collectors.toList()));
    }

    private String getMetadataStoragePath(PackageName packageName) {
        return String.format("%s/%s", getPackageStoragePath(packageName), "meta");
    }

    private String getPackageStoragePath(PackageName packageName) {
        return String.format("%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getNamespaceName().toString(),
            packageName.getName(),
            packageName.getVersion());
    }

    private String getPackagePathWithoutVersion(PackageName packageName) {
        return String.format("%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getNamespaceName().toString(),
            packageName.getName());
    }
}
