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

import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.pulsar.client.admin.Packages;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.packages.management.core.common.PackageMetadata;
import org.apache.pulsar.packages.management.core.common.PackageName;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;

/**
 * The implementation of the packages management service administration operations.
 */
public class PackagesImpl extends ComponentResource implements Packages {

    private final WebTarget packages;
    private final AsyncHttpClient httpClient;

    public PackagesImpl(WebTarget webTarget, Authentication auth, AsyncHttpClient client, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.httpClient = client;
        this.packages = webTarget.path("/admin/v3/packages");
    }

    @Override
    public PackageMetadata getMetadata(String packageName) throws PulsarAdminException {
        try {
            return getMetadataAsync(packageName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<PackageMetadata> getMetadataAsync(String packageName) {
        WebTarget path = packages.path(PackageName.get(packageName).toRestPath() + "/metadata");
        final CompletableFuture<PackageMetadata> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<PackageMetadata>() {
            @Override
            public void completed(PackageMetadata metadata) {
                future.complete(metadata);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;    }

    @Override
    public void updateMetadata(String packageName, PackageMetadata metadata) throws PulsarAdminException {
        try {
            updateMetadataAsync(packageName, metadata).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        }
    }

    @Override
    public CompletableFuture<Void> updateMetadataAsync(String packageName, PackageMetadata metadata) {
        WebTarget path = packages.path(PackageName.get(packageName).toRestPath() + "/metadata");
        return asyncPutRequest(path, Entity.entity(metadata, MediaType.APPLICATION_JSON));
    }

    @Override
    public void upload(PackageMetadata metadata, String packageName, String path) throws PulsarAdminException {
        try {
            uploadAsync(metadata, packageName, path).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        }
    }

    @Override
    public CompletableFuture<Void> uploadAsync(PackageMetadata metadata, String packageName, String path) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder = Dsl
                .post(packages.path(PackageName.get(packageName).toRestPath()).getUri().toASCIIString())
                .addBodyPart(new FilePart("file", new File(path), MediaType.APPLICATION_OCTET_STREAM))
                .addBodyPart(new StringPart("metadata", new Gson().toJson(metadata), MediaType.APPLICATION_JSON));
            httpClient.executeRequest(addAuthHeaders(packages, builder).build())
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
        try {
            downloadAsync(packageName, path).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PulsarAdminException) {
                throw (PulsarAdminException) cause;
            } else {
                throw new PulsarAdminException(cause);
            }
        }
    }

    @Override
    public CompletableFuture<Void> downloadAsync(String packageName, String path) {
        WebTarget webTarget = packages.path(PackageName.get(packageName).toRestPath());
        final CompletableFuture<Void> future = new CompletableFuture<>();
        asyncGetRequest(webTarget, new InvocationCallback<Response>(){
            @Override
            public void completed(Response response) {
                if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                    try (InputStream inputStream = response.readEntity(InputStream.class)) {
                        Path destinyPath = Paths.get(path);
                        if (destinyPath.getParent() != null) {
                            Files.createDirectories(destinyPath.getParent());
                        }
                        Files.copy(inputStream, destinyPath);
                        future.complete(null);
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                } else {
                    future.completeExceptionally(getApiException(response));
                }
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        return future;
    }

    @Override
    public void delete(String packageName) throws PulsarAdminException {
        try {
            deleteAsync(packageName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String packageName) {
        PackageName name = PackageName.get(packageName);
        WebTarget path = packages.path(name.toRestPath());
        return asyncDeleteRequest(path);
    }

    @Override
    public List<String> listPackageVersions(String packageName) throws PulsarAdminException {
        try {
            return listPackageVersionsAsync(packageName).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        }
    }


    @Override
    public CompletableFuture<List<String>> listPackageVersionsAsync(String packageName) {
        PackageName name = PackageName.get(packageName);
        WebTarget path = packages.path(String.format("%s/%s/%s/%s",
            name.getPkgType().toString(), name.getTenant(), name.getNamespace(), name.getName()));
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<List<String>>() {
            @Override
            public void completed(List<String> strings) {
                future.complete(strings);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }

    @Override
    public List<String> listPackages(String type, String namespace) throws PulsarAdminException {
        try {
            return listPackagesAsync(type, namespace).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (ExecutionException e) {
            throw (PulsarAdminException) e.getCause();
        }
    }

    @Override
    public CompletableFuture<List<String>> listPackagesAsync(String type, String namespace) {
        WebTarget path = packages.path(type + "/" + NamespaceName.get(namespace).toString());
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path, new InvocationCallback<List<String>>() {
            @Override
            public void completed(List<String> strings) {
                future.complete(strings);
            }

            @Override
            public void failed(Throwable throwable) {
                future.completeExceptionally(getApiException(throwable.getCause()));
            }
        });
        return future;
    }
}
