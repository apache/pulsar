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
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.packages.manager.PackageManager;
import org.apache.pulsar.packages.manager.PackageMetadata;
import org.apache.pulsar.packages.manager.PackageStorage;
import org.apache.pulsar.packages.manager.exceptions.PackageManagerException;
import org.apache.pulsar.packages.manager.naming.PackageName;
import org.apache.pulsar.packages.manager.naming.PackageType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * The implementation of package manager.
 */
public class PackageManagerImpl implements PackageManager {
    private final PackageStorage packageStorage;

    public PackageManagerImpl(PackageStorage packageStorage) {
        this.packageStorage = packageStorage;
    }

    @Override
    public CompletableFuture<PackageMetadata> getMetadata(PackageName packageName) {
        CompletableFuture<PackageMetadata> future = new CompletableFuture<>();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        packageStorage.existAsync(getMetadataPath(packageName))
            .whenComplete((exists, err) -> {
                if (err != null) {
                    future.completeExceptionally(err);
                    return;
                }
                if (!exists) {
                    future.completeExceptionally(new PackageManagerException.MetadataNotFoundException(
                        "Package " + packageName.toString() + " metadata does not found"));
                    return;
                }
                packageStorage.readAsync(getMetadataPath(packageName), outputStream)
                    .whenComplete((ignore, e) -> {
                        if (e != null) {
                            future.completeExceptionally(e);
                            return;
                        }
                        CompletableFuture.runAsync(() -> {
                            try {
                                future.complete(PackageMetadata.fromByteArray(outputStream.toByteArray()));
                            } catch (PackageManagerException.MetadataSerializationException mse) {
                                future.completeExceptionally(mse);
                            }
                        });
                    });
            });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata) {
        return packageStorage.existAsync(getMetadataPath(packageName))
            .thenCompose(exists -> exists ? saveMetadata(packageName, metadata) :
                FutureUtil.failedFuture(new PackageManagerException.MetadataNotFoundException(
                    "Package " + packageName.toString() + " metadata does not exist.")));
    }

    @Override
    public CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream) {
        return packageStorage.existAsync(packageName.toFilePath())
            .thenCompose(exists -> exists ? packageStorage.readAsync(packageName.toFilePath(), outputStream)
                : FutureUtil.failedFuture(new PackageManagerException.PackageNotFoundException(
                    "Package " + packageName.toString() + " does not exist.")));
    }

    @Override
    public CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream) {
        return saveMetadata(packageName, metadata)
            .thenCompose(ignore -> packageStorage.writeAsync(packageName.toFilePath(), inputStream));
    }

    private CompletableFuture<Void> saveMetadata(PackageName packageName, PackageMetadata metadata) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            return packageStorage.writeAsync(getMetadataPath(packageName),
                new ByteArrayInputStream(metadata.toByteArray()));
        } catch (PackageManagerException.MetadataSerializationException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> delete(PackageName packageName) {
        CompletableFuture<Void> deleteMetadataFuture = packageStorage.existAsync(getMetadataPath(packageName))
            .thenCompose(exists -> exists ? packageStorage.deleteAsync(getMetadataPath(packageName))
                : CompletableFuture.completedFuture(null));
        CompletableFuture<Void> deletePackageFuture = packageStorage.existAsync(packageName.toFilePath())
            .thenCompose(exists -> exists ? packageStorage.deleteAsync(packageName.toFilePath())
                : CompletableFuture.completedFuture(null));
        return FutureUtil.waitForAll(Arrays.asList(deleteMetadataFuture, deletePackageFuture));
    }

    @Override
    public CompletableFuture<List<PackageName>> listVersions(PackageName packageName) {
        return packageStorage.listAsync(packageName.toFilePath())
            .thenApply(paths ->
                paths.stream().map(PackageName::get).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<PackageName>> listPackages(PackageType type, NamespaceName namespace) {
        String path = type.toString() + "/" + namespace.toString();
        return packageStorage.listAsync(path)
            .thenApply(paths -> paths.stream().map(PackageName::get).collect(Collectors.toList()));
    }
    
    private String getMetadataPath(PackageName packageName) {
        return packageName.toFilePath() + "_meta";
    }
}
