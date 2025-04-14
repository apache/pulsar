/*
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

import static org.asynchttpclient.Dsl.get;
import com.google.gson.Gson;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.internal.http.AsyncHttpRequestExecutor;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.packages.management.core.common.PackageName;
import org.asynchttpclient.AsyncCompletionHandlerBase;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;

/**
 * The implementation of the packages management service administration operations.
 */
public class PackagesImpl extends ComponentResource implements Packages {

    private final WebTarget packages;
    private final AsyncHttpRequestExecutor asyncHttpRequestExecutor;

    public PackagesImpl(WebTarget webTarget, Authentication auth, AsyncHttpRequestExecutor asyncHttpRequestExecutor,
                        long requestTimeoutMs) {
        super(auth, requestTimeoutMs);
        this.asyncHttpRequestExecutor = asyncHttpRequestExecutor;
        this.packages = webTarget.path("/admin/v3/packages");
    }

    @Override
    public PackageMetadata getMetadata(String packageName) throws PulsarAdminException {
        return sync(() -> getMetadataAsync(packageName));
    }

    @Override
    public CompletableFuture<PackageMetadata> getMetadataAsync(String packageName) {
        WebTarget path = packages.path(PackageName.get(packageName).toRestPath() + "/metadata");
        return asyncGetRequest(path, new FutureCallback<PackageMetadata>(){});
    }

    @Override
    public void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException {
        sync(() -> updateMetadataAsync(packageName, metadata));
    }

    @Override
    public CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata) {
        WebTarget path = packages.path(PackageName.get(packageName).toRestPath() + "/metadata");
        return asyncPutRequest(path, Entity.entity(metadata, MediaType.APPLICATION_JSON));
    }

    @Override
    public void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException {
        sync(() -> uploadAsync(metadata, packageName, path));
    }

    @Override
    public CompletableFuture<Void> uploadAsync(PackageMetadata metadata, String packageName, String path) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder = Dsl
                .post(packages.path(PackageName.get(packageName).toRestPath()).getUri().toASCIIString())
                .addBodyPart(new FilePart("file", new File(path), MediaType.APPLICATION_OCTET_STREAM))
                .addBodyPart(new StringPart("metadata", new Gson().toJson(metadata), MediaType.APPLICATION_JSON));
            asyncHttpRequestExecutor.executeRequest(addAuthHeaders(packages, builder).build())
                .toCompletableFuture()
                .thenAccept(response -> {
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        future.completeExceptionally(
                            getApiException(Response
                                .status(response.getStatusCode())
                                .entity(response.getResponseBody())
                                .build()));
                    } else {
                        future.complete(null);
                    }
                })
                .exceptionally(throwable -> {
                    future.completeExceptionally(getApiException(throwable));
                    return null;
                });
        } catch (PulsarAdminException e) {
            future.completeExceptionally(e);
        }
        return future;    }

    @Override
    public void download(String packageName, String path) throws PulsarAdminException {
        sync(() -> downloadAsync(packageName, path));
    }

    @Override
    public CompletableFuture<Void> downloadAsync(String packageName, String path) {
        WebTarget webTarget = packages.path(PackageName.get(packageName).toRestPath());
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            Path destinyPath = Paths.get(path);
            if (destinyPath.getParent() != null) {
                Files.createDirectories(destinyPath.getParent());
            }

            FileChannel os = new FileOutputStream(destinyPath.toFile()).getChannel();
            RequestBuilder builder = get(webTarget.getUri().toASCIIString());

            CompletableFuture<org.asynchttpclient.Response> responseFuture =
                asyncHttpRequestExecutor.executeRequest(addAuthHeaders(webTarget, builder).build(),
                        () -> new AsyncCompletionHandlerBase() {

                            @Override
                            public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                                os.write(bodyPart.getBodyByteBuffer());
                                return State.CONTINUE;
                            }
                    });
            responseFuture
                .whenComplete((response, throwable) -> {
                    try {
                        os.close();
                    } catch (IOException e) {
                        future.completeExceptionally(getApiException(throwable));
                    }
                })
                .thenAccept(response -> {
                    if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                        future.completeExceptionally(
                            getApiException(Response
                                .status(response.getStatusCode())
                                .entity(response.getStatusText())
                                .build()));
                    } else {
                        future.complete(null);
                    }
                })
                .exceptionally(throwable -> {
                    future.completeExceptionally(getApiException(throwable));
                    return null;
                });
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void delete(String packageName) throws PulsarAdminException {
        sync(() -> deleteAsync(packageName));
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String packageName) {
        PackageName name = PackageName.get(packageName);
        WebTarget path = packages.path(name.toRestPath());
        return asyncDeleteRequest(path);
    }

    @Override
    public List<String> listPackageVersions(String packageName) throws PulsarAdminException {
        return sync(() -> listPackageVersionsAsync(packageName));
    }


    @Override
    public CompletableFuture<List<String>> listPackageVersionsAsync(String packageName) {
        PackageName name = PackageName.get(packageName);
        WebTarget path = packages.path(String.format("%s/%s/%s/%s",
            name.getPkgType().toString(), name.getTenant(), name.getNamespace(), name.getName()));
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }

    @Override
    public List<String> listPackages(String type, String namespace) throws PulsarAdminException {
        return sync(() -> listPackagesAsync(type, namespace));
    }

    @Override
    public CompletableFuture<List<String>> listPackagesAsync(String type, String namespace) {
        WebTarget path = packages.path(type + "/" + NamespaceName.get(namespace).toString());
        return asyncGetRequest(path, new FutureCallback<List<String>>(){});
    }
}
