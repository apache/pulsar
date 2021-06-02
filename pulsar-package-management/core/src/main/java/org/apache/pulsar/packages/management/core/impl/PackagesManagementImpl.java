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
package org.apache.pulsar.packages.management.core.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.packages.management.core.PackagesManagement;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.packages.management.core.common.PackageMetadataUtil;
import org.apache.pulsar.packages.management.core.common.PackageName;
import org.apache.pulsar.packages.management.core.common.PackageType;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException;
import org.apache.pulsar.packages.management.core.exceptions.PackagesManagementException.NotFoundException;

/**
 * Packages management implementation.
 */
public class PackagesManagementImpl implements PackagesManagement {

    private PackagesStorage storage;

    @Override
    public void initialize(PackagesStorage storage) {
        this.storage = storage;
    }

    @Override
    public CompletableFuture<PackageMetadata> getMeta(PackageName packageName) {
        CompletableFuture<PackageMetadata> future = new CompletableFuture<>();
        String metadataPath = metadataPath(packageName);
        checkMetadataNotExistsAndThrowException(packageName)
            .whenComplete((ignore, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                    storage.readAsync(metadataPath, outputStream)
                        .thenCompose(aVoid -> metadataReadFromStream(outputStream))
                        .whenComplete((metadata, t) -> {
                            if (t != null) {
                                future.completeExceptionally(t);
                            } else {
                                future.complete(metadata);
                            }
                        });
                } catch (IOException e) {
                    future.completeExceptionally(new PackagesManagementException(
                        String.format("Read package '%s' metadata failed", packageName.toString()), e));
                }
            });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateMeta(PackageName packageName, PackageMetadata metadata) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String metadataPath = metadataPath(packageName);
        checkMetadataNotExistsAndThrowException(packageName)
            .whenComplete((ignore, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(PackageMetadataUtil.toBytes(metadata))) {
                    storage.deleteAsync(metadataPath)
                        .thenCompose(aVoid -> storage.writeAsync(metadataPath, inputStream))
                        .whenComplete((aVoid, t) -> {
                            if (t != null) {
                                future.completeExceptionally(new PackagesManagementException(
                                    String.format("Update package '%s' metadata failed", packageName.toString()), t));
                            } else {
                                future.complete(null);
                            }
                        });
                } catch (IOException e) {
                    future.completeExceptionally(new PackagesManagementException(
                        String.format("Read package '%s' metadata failed", packageName.toString()), e));
                }
            });
        return future;
    }

    private CompletableFuture<Void> writeMeta(PackageName packageName, PackageMetadata metadata) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String metadataPath = metadataPath(packageName);
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(PackageMetadataUtil.toBytes(metadata))) {
            storage.writeAsync(metadataPath, inputStream)
                .whenComplete((aVoid, t) -> {
                    if (t != null) {
                        future.completeExceptionally(new PackagesManagementException(
                            String.format("Update package '%s' metadata failed", packageName.toString()), t));
                    } else {
                        future.complete(null);
                    }
                });
        } catch (IOException e) {
            future.completeExceptionally(new PackagesManagementException(
                String.format("Read package '%s' metadata failed", packageName.toString()), e));
        }
        return future;
    }

    @Override
    public CompletableFuture<Void> download(PackageName packageName, OutputStream outputStream) {
        String packagePath = packagePath(packageName);
        return checkPackageNotExistsAndThrowException(packageName)
            .thenCompose(ignore -> storage.readAsync(packagePath, outputStream));
    }

    @Override
    public CompletableFuture<Void> upload(PackageName packageName, PackageMetadata metadata, InputStream inputStream) {
        return CompletableFuture.allOf(
            checkMetadataExistsAndThrowException(packageName),
            checkPackageExistsAndThrowException(packageName)
        ).thenCompose(ignore -> writeMeta(packageName, metadata))
            .thenCompose(ignore -> storage.writeAsync(packagePath(packageName), inputStream));
    }

    @Override
    public CompletableFuture<Void> delete(PackageName packageName) {
        return CompletableFuture.allOf(
            storage.deleteAsync(metadataPath(packageName)),
            storage.deleteAsync(packagePath(packageName)));
    }

    @Override
    public CompletableFuture<List<String>> list(PackageName packageName) {
        return storage.listAsync(packageWithoutVersionPath(packageName));
    }

    @Override
    public CompletableFuture<List<String>> list(PackageType type, String tenant, String namespace) {
        return storage.listAsync(String.format("%s/%s/%s", type, tenant, namespace));
    }

    private CompletableFuture<Void> checkMetadataNotExistsAndThrowException(PackageName packageName) {
        String path = metadataPath(packageName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        storage.existAsync(path)
            .whenComplete((exist, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (exist) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new NotFoundException(String.format("Package '%s' metadata does not exist", packageName)));
                }
            });
        return future;
    }

    private CompletableFuture<Void> checkMetadataExistsAndThrowException(PackageName packageName) {
        String path = metadataPath(packageName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        storage.existAsync(path)
            .whenComplete((exist, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (!exist) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new NotFoundException(String.format("Package '%s' metadata already exists", packageName)));
                }
            });
        return future;
    }

    private CompletableFuture<Void> checkPackageNotExistsAndThrowException(PackageName packageName) {
        String path = packagePath(packageName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        storage.existAsync(path)
            .whenComplete((exist, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (exist) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new NotFoundException(String.format("Package '%s' does not exist", packageName.toString())));
                }
            });
        return future;
    }

    private CompletableFuture<Void> checkPackageExistsAndThrowException(PackageName packageName) {
        String path = packagePath(packageName);
        CompletableFuture<Void> future = new CompletableFuture<>();
        storage.existAsync(path)
            .whenComplete((exist, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (!exist) {
                    future.complete(null);
                } else {
                    future.completeExceptionally(
                        new NotFoundException(String.format("Package '%s' already exists", packageName.toString())));
                }
            });
        return future;
    }

    private CompletableFuture<PackageMetadata> metadataReadFromStream(ByteArrayOutputStream outputStream) {
        CompletableFuture<PackageMetadata> future = new CompletableFuture<>();
        try {
            PackageMetadata metadata = PackageMetadataUtil.fromBytes(outputStream.toByteArray());
            future.complete(metadata);
        } catch (PackagesManagementException.MetadataFormatException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    private String metadataPath(PackageName packageName) {
        return packageName.toRestPath() + "/meta";
    }

    private String packagePath(PackageName packageName) {
        return packageName.toRestPath();
    }

    private String packageWithoutVersionPath(PackageName packageName) {
        return String.format("%s/%s/%s/%s",
            packageName.getPkgType().toString(),
            packageName.getTenant(),
            packageName.getNamespace(),
            packageName.getName());
    }
}
