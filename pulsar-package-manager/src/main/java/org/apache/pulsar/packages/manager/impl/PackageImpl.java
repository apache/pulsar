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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.packages.manager.Package;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.exception.PackageMetaNotFoundException;
import org.apache.pulsar.packages.manager.exception.PackageNotFoundException;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;

/**
 * The implementation of package management.
 */
@Slf4j
public class PackageImpl implements Package {

    private final PackageStorage packageStorage;
    private final static Gson gson = new Gson();

    public PackageImpl(PackageStorage packageStorage) {
        this.packageStorage = packageStorage;
    }

    public CompletableFuture<Void> setMeta(PackageName packageName, PackageMetadata metadata) {
        String metadataPath = getMetadataStoragePath(packageName);
        log.info("save the package metadata to the path {}", metadataPath);
        System.out.println("save package metadta to the path " + metadataPath);
        ByteArrayInputStream meta = new ByteArrayInputStream(gson.toJson(metadata).getBytes(StandardCharsets.UTF_8));
        return packageStorage.writeAsync(metadataPath, meta);
    }

    @Override
    public CompletableFuture<PackageMetadata> getMeta(PackageName packageName) {
        String metadataPath = getMetadataStoragePath(packageName);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        return packageStorage.existAsync(metadataPath)
            .thenCompose(exists -> exists
                ? packageStorage.readAsync(metadataPath, outputStream)
                : FutureUtil.failedFuture(
                    new PackageMetaNotFoundException(
                        "The metadata of package " + packageName.toString() + " is not found")))
            .thenApply(ignore ->
                gson.fromJson(new String(outputStream.toByteArray(), StandardCharsets.UTF_8), PackageMetadata.class));
    }

    @Override
    public CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata) {
        String metadataPath = getMetadataStoragePath(packageName);
        ByteArrayInputStream meta = new ByteArrayInputStream(gson.toJson(metadata).getBytes(StandardCharsets.UTF_8));
        return packageStorage.existAsync(metadataPath)
            .thenCompose(exists -> exists
                ? packageStorage.deleteAsync(metadataPath)
                    .thenCompose(ignore -> packageStorage.writeAsync(metadataPath, meta))
                : FutureUtil.failedFuture(
                    new PackageMetaNotFoundException(
                        "The metadata of package " + packageName.toString() + " is not found")));
    }

    @Override
    public CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream) {
        String packagePath = getPackageStoragePath(packageName);
        return packageStorage.existAsync(packagePath)
            .thenCompose(exists -> exists
                ? packageStorage.readAsync(packagePath, outputStream)
                : FutureUtil.failedFuture(
                    new PackageNotFoundException(
                        "The package " + packageName.toString() + " is not found")));
    }

    @Override
    public CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream) {
        String metadataPath = getMetadataStoragePath(packageName);
        String packagePath = getPackageStoragePath(packageName);
        ByteArrayInputStream meta = new ByteArrayInputStream(gson.toJson(metadata).getBytes(StandardCharsets.UTF_8));
        return packageStorage.writeAsync(metadataPath, meta)
            .thenCompose(ignore -> packageStorage.writeAsync(packagePath, inputStream));
//        return packageStorage.existAsync(metadataPath)
//            .thenCompose(metaExists -> metaExists
//                ? FutureUtil.failedFuture(
//                    new PackageMetaAlreadyExistsException(
//                        "The metadata of package " + packageName.toString() + " already exists"))
//                : packageStorage.existAsync(packagePath)).thenCompose(dataExists -> dataExists
//                ? FutureUtil.failedFuture(
//                    new PackageAlreadyExistsException("The package " + packageName.toString() + " already exists"))
//                : packageStorage
//                    .writeAsync(metadataPath,
//                        new ByteArrayInputStream(gson.toJson(metadata).getBytes(StandardCharsets.UTF_8))))
//                    .thenCompose(ignore -> packageStorage.writeAsync(packagePath, inputStream));
    }

    @Override
    public CompletableFuture<Void> delete(PackageName packageName) {
        CompletableFuture<Void> result = new CompletableFuture<>();

        String metadataPath = getMetadataStoragePath(packageName);
        String packagePath = getPackageStoragePath(packageName);

        packageStorage.existAsync(metadataPath)
            .thenCombine(packageStorage.existAsync(packagePath), (metaExist, packageExist) -> {
                if (metaExist) {
                    return packageStorage.deleteAsync(metadataPath)
                        .whenComplete((ignore, e) -> {
                            if (e != null) {
                                result.completeExceptionally(e);
                            }
                            packageStorage.deleteAsync(packagePath)
                                .whenComplete((aVoid, throwable) -> {
                                    if (throwable != null) {
                                        if (!result.isCompletedExceptionally()) {
                                            result.completeExceptionally(throwable);
                                        }
                                    } else {
                                        result.complete(null);
                                    }
                                });
                        });
                } else if (packageExist) {
                    return packageStorage.deleteAsync(packagePath).thenApply(ignore -> result.complete(null));
                }
                result.complete(null);
                return CompletableFuture.completedFuture(null);
            });

        return result;
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageName packageName) {
        String packageWithoutVersionPath = getPackagePathWithoutVersion(packageName);
        return packageStorage.listAsync(packageWithoutVersionPath)
            .thenApply(names -> names.stream()
                .map(v ->
                    PackageName.get(
                        String.format("%s://%s@%s", packageName.getPkgType().toString(),
                            packageName.getCompleteName(), v)))
                .collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<PackageName>> list(PackageType type, NamespaceName namespace) {
        String path = type + "/" + namespace.toString();
        return packageStorage.listAsync(path)
            .thenApply(names -> names.stream()
                .map(p -> PackageName.get(String.format("%s://%s/%s", type.toString(), namespace.toString(), p)))
                .collect(Collectors.toList()));
    }

    private static String getMetadataStoragePath(PackageName packageName) {
        return String.format("%s/%s", getPackageStoragePath(packageName), "meta");
    }

    private static String getPackageStoragePath(PackageName packageName) {
        return String.format("%s/%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getTenant(),
            packageName.getNamespace(),
            packageName.getName(),
            packageName.getVersion());
    }

    private static String getPackagePathWithoutVersion(PackageName packageName) {
        return String.format("%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getTenant(),
            packageName.getNamespace(),
            packageName.getName());
    }
}
