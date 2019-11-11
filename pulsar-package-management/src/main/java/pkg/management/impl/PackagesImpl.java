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

package pkg.management.impl;

import com.google.gson.Gson;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;
import pkg.management.Packages;
import pkg.management.PkgMetadata;
import pkg.management.exception.PackageException;
import pkg.management.exception.PackageMetaNotFoundException;
import pkg.management.exception.PackageNotFoundException;
import pkg.management.naming.PkgName;
import pkg.management.PkgStorage;
import pkg.management.naming.PkgType;

// The implementation of package management.
public class PackagesImpl implements Packages {

    private PkgStorage pkgStorage;

    public PackagesImpl(PkgStorage pkgStorage) {
        this.pkgStorage = pkgStorage;
    }

    @Override
    public CompletableFuture<PkgMetadata> getMeta(PkgName pkgName) {
        return pkgStorage.exist(pkgName.getMetadataPath())
                         .thenCompose(exists -> exists
                                ? pkgStorage.read(pkgName.getMetadataPath())
                                : FutureUtil.failedFuture(
                                    new PackageMetaNotFoundException("Package metadata does not exist")))
                         .thenApply(meta -> new Gson().fromJson(new String(meta), PkgMetadata.class));
    }

    @Override
    public CompletableFuture<Void> updateMeta(PkgName pkgName, PkgMetadata metadata) {
        return pkgStorage.exist(pkgName.getMetadataPath())
                         .thenCompose(exists -> exists
                                ? pkgStorage.write(pkgName.getMetadataPath(), new Gson().toJson(metadata).getBytes())
                                : FutureUtil.failedFuture(
                                    new PackageMetaNotFoundException("Package metadata does not exist")));
    }

    @Override
    public CompletableFuture<Void> download(PkgName pkgName, OutputStream outputStream) {
        return pkgStorage.exist(pkgName.getRestPath())
                         .thenCompose(exists -> exists
                                ? pkgStorage.read(pkgName.getRestPath(), outputStream)
                                : FutureUtil.failedFuture(
                                    new PackageNotFoundException("Package does not exist")));
    }

    @Override
    public CompletableFuture<Void> upload(PkgName pkgName, PkgMetadata metadata, InputStream inputStream) {
        return pkgStorage.write(pkgName.getRestPath(), inputStream);
    }

    @Override
    public CompletableFuture<Void> delete(PkgName pkgName) {
        return pkgStorage.exist(pkgName.getRestPath())
                         .thenCompose(exists -> exists
                                ? pkgStorage.delete(pkgName.getRestPath())
                                : FutureUtil.failedFuture(
                                    new PackageNotFoundException("Package does not exist")));
    }

    @Override
    public CompletableFuture<List<PkgName>> list(PkgName pkgName) {
        return pkgStorage.exist(pkgName.getPathWithoutVersion())
                         .thenCompose(exists -> exists
                                ? pkgStorage.list(pkgName.getPathWithoutVersion())
                                : FutureUtil.failedFuture(
                                    new PackageNotFoundException("Package does not exist")))
                         .thenApply(names -> names.stream().map(PkgName::get).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<List<PkgName>> list(NamespaceName namespace) {
        List<CompletableFuture<List<PkgName>>> pkgs =
            Arrays.stream(PkgType.values()).map(t -> list(t, namespace)).collect(Collectors.toList());

        CompletableFuture<List<PkgName>> result = new CompletableFuture<>();
        List<PkgName> pkgNames = new ArrayList<>();
        pkgs.forEach(pkg -> {
            try {
                pkgNames.addAll(pkg.get());
            } catch (Exception e) {
                if (e.getCause() instanceof PackageException) {
                    // it's ok that there are some files not found
                } else {
                    result.completeExceptionally(e);
                }
            }
        });
        result.complete(pkgNames);

        return result;
    }

    private CompletableFuture<List<PkgName>> list(PkgType type, NamespaceName namespace) {
        String path = type + "/" + namespace.toString();
        return pkgStorage.exist(path)
                         .thenCompose(exists -> exists
                                ? pkgStorage.list(path)
                                : FutureUtil.failedFuture(new PackageNotFoundException("Package does not exist")))
                         .thenApply(names -> names.stream().map(PkgName::get).collect(Collectors.toList()));
    }
}
