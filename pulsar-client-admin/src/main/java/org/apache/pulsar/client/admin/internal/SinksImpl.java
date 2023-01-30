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

import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;
import com.google.gson.Gson;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Sink;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

@Slf4j
public class SinksImpl extends ComponentResource implements Sinks, Sink {

    private final WebTarget sink;
    private final AsyncHttpClient asyncHttpClient;

    public SinksImpl(WebTarget web, Authentication auth, AsyncHttpClient asyncHttpClient, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.sink = web.path("/admin/v3/sink");
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public List<String> listSinks(String tenant, String namespace) throws PulsarAdminException {
        return sync(() -> listSinksAsync(tenant, namespace));
    }

    @Override
    public CompletableFuture<List<String>> listSinksAsync(String tenant, String namespace) {
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        if (!validateNamespace(tenant, namespace, future)) {
            return future;
        }
        WebTarget path = sink.path(tenant).path(namespace);
        return asyncGetRequest(path, new GenericType<List<String>>() {});
    }

    @Override
    public SinkConfig getSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        return sync(() -> getSinkAsync(tenant, namespace, sinkName));
    }

    @Override
    public CompletableFuture<SinkConfig> getSinkAsync(String tenant, String namespace, String sinkName) {
        final CompletableFuture<SinkConfig> future = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, future)) {
            return future;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName);
        return asyncGetRequest(path, SinkConfig.class);
    }

    @Override
    public SinkStatus getSinkStatus(
            String tenant, String namespace, String sinkName) throws PulsarAdminException {
        return sync(() -> getSinkStatusAsync(tenant, namespace, sinkName));
    }

    @Override
    public CompletableFuture<SinkStatus> getSinkStatusAsync(String tenant, String namespace, String sinkName) {
        final CompletableFuture<SinkStatus> future = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, future)) {
            return future;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path("status");
        return asyncGetRequest(path, SinkStatus.class);
    }

    @Override
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkStatus(
            String tenant, String namespace, String sinkName, int id) throws PulsarAdminException {
        return sync(() -> getSinkStatusAsync(tenant, namespace, sinkName, id));
    }

    @Override
    public CompletableFuture<SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> getSinkStatusAsync(
            String tenant, String namespace, String sinkName, int id) {
        final CompletableFuture<SinkStatus.SinkInstanceStatus.SinkInstanceStatusData> future =
                new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, future)) {
            return future;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(id)).path("status");
        return asyncGetRequest(path, SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class);
    }

    @Override
    public void createSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException {
        sync(() -> createSinkAsync(sinkConfig, fileName));
    }

    @Override
    public CompletableFuture<Void> createSinkAsync(SinkConfig sinkConfig, String fileName) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (!validateSinkName(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), future)) {
            return future;
        }
        try {
            RequestBuilder builder =
                    post(sink.path(sinkConfig.getTenant())
                            .path(sinkConfig.getNamespace()).path(sinkConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sinkConfig", objectWriter()
                            .writeValueAsString(sinkConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpClient.executeRequest(addAuthHeaders(sink, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
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
    public void createSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException {
        sync(() -> createSinkWithUrlAsync(sinkConfig, pkgUrl));
    }

    @Override
    public CompletableFuture<Void> createSinkWithUrlAsync(SinkConfig sinkConfig, String pkgUrl) {
        final FormDataMultiPart mp = new FormDataMultiPart();
        mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
        mp.bodyPart(new FormDataBodyPart("sinkConfig",
                new Gson().toJson(sinkConfig),
                MediaType.APPLICATION_JSON_TYPE));
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(sinkConfig.getTenant(), sinkConfig.getNamespace(),
                sinkConfig.getName(), validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace()).path(sinkConfig.getName());
        return asyncPostRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
    }

    @Override
    public void deleteSink(String cluster, String namespace, String function) throws PulsarAdminException {
        sync(() -> deleteSinkAsync(cluster, namespace, function));
    }

    @Override
    public CompletableFuture<Void> deleteSinkAsync(String tenant, String namespace, String function) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, function, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(function);
        return asyncDeleteRequest(path);
    }

    @Override
    public void updateSink(SinkConfig sinkConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException {
        sync(() -> updateSinkAsync(sinkConfig, fileName, updateOptions));
    }

    @Override
    public CompletableFuture<Void> updateSinkAsync(
            SinkConfig sinkConfig, String fileName, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (!validateSinkName(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), future)) {
            return future;
        }
        try {
            RequestBuilder builder =
                    put(sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace())
                            .path(sinkConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sinkConfig", objectWriter()
                            .writeValueAsString(sinkConfig), MediaType.APPLICATION_JSON));

            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                builder.addBodyPart(new StringPart("updateOptions",
                        objectWriter().writeValueAsString(options), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpClient.executeRequest(addAuthHeaders(sink, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(response.getStatusCode())
                                            .entity(response.getResponseBody()).build()));
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
    public void updateSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException {
       updateSink(sinkConfig, fileName, null);
    }

    @Override
    public CompletableFuture<Void> updateSinkAsync(SinkConfig sinkConfig, String fileName) {
        return updateSinkAsync(sinkConfig, fileName, null);
    }

    @Override
    public void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl, UpdateOptions updateOptions)
            throws PulsarAdminException {
        sync(() -> updateSinkWithUrlAsync(sinkConfig, pkgUrl, updateOptions));
    }

    @Override
    public CompletableFuture<Void> updateSinkWithUrlAsync(
            SinkConfig sinkConfig, String pkgUrl, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        if (!validateSinkName(sinkConfig.getTenant(), sinkConfig.getNamespace(), sinkConfig.getName(), future)) {
            return future;
        }
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
            mp.bodyPart(new FormDataBodyPart(
                    "sinkConfig",
                    new Gson().toJson(sinkConfig),
                    MediaType.APPLICATION_JSON_TYPE));
            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        objectWriter().writeValueAsString(options),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            WebTarget path = sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace())
                    .path(sinkConfig.getName());
            return asyncPutRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException {
        updateSinkWithUrl(sinkConfig, pkgUrl, null);
    }

    @Override
    public CompletableFuture<Void> updateSinkWithUrlAsync(SinkConfig sinkConfig, String pkgUrl) {
        return updateSinkWithUrlAsync(sinkConfig, pkgUrl, null);
    }

    @Override
    public void restartSink(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        sync(() -> restartSinkAsync(tenant, namespace, functionName, instanceId));
    }

    @Override
    public CompletableFuture<Void> restartSinkAsync(
            String tenant, String namespace, String functionName, int instanceId) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, functionName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                .path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void restartSink(String tenant, String namespace, String functionName) throws PulsarAdminException {
        sync(() -> restartSinkAsync(tenant, namespace, functionName));
    }

    @Override
    public CompletableFuture<Void> restartSinkAsync(String tenant, String namespace, String functionName) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, functionName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(functionName).path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopSink(String tenant, String namespace, String sinkName, int instanceId)
            throws PulsarAdminException {
        sync(() -> stopSinkAsync(tenant, namespace, sinkName, instanceId));
    }

    @Override
    public CompletableFuture<Void> stopSinkAsync(String tenant, String namespace, String sinkName, int instanceId) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(instanceId))
                .path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        sync(() -> stopSinkAsync(tenant, namespace, sinkName));
    }

    @Override
    public CompletableFuture<Void> stopSinkAsync(String tenant, String namespace, String sinkName) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startSink(String tenant, String namespace, String sinkName, int instanceId)
            throws PulsarAdminException {
        sync(() -> startSinkAsync(tenant, namespace, sinkName, instanceId));
    }

    @Override
    public CompletableFuture<Void> startSinkAsync(String tenant, String namespace, String sinkName, int instanceId) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(instanceId))
                .path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        sync(() -> startSinkAsync(tenant, namespace, sinkName));
    }

    @Override
    public CompletableFuture<Void> startSinkAsync(String tenant, String namespace, String sinkName) {
        CompletableFuture<Void> validationFuture = new CompletableFuture<>();
        if (!validateSinkName(tenant, namespace, sinkName, validationFuture)) {
            return validationFuture;
        }
        WebTarget path = sink.path(tenant).path(namespace).path(sinkName).path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public List<ConnectorDefinition> getBuiltInSinks() throws PulsarAdminException {
        return sync(() -> getBuiltInSinksAsync());
    }

    @Override
    public CompletableFuture<List<ConnectorDefinition>> getBuiltInSinksAsync() {
        WebTarget path = sink.path("builtinsinks");
        return asyncGetRequest(path, new GenericType<List<ConnectorDefinition>>() {});
    }

    @Override
    public void reloadBuiltInSinks() throws PulsarAdminException {
        sync(() -> reloadBuiltInSinksAsync());
    }

    @Override
    public CompletableFuture<Void> reloadBuiltInSinksAsync() {
        WebTarget path = sink.path("reloadBuiltInSinks");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    private static boolean validateNamespace(String tenant, String namespace, CompletableFuture<?> future) {
        if (StringUtils.isBlank(tenant)) {
            future.completeExceptionally(new PulsarAdminException("tenant is required"));
            return false;
        }
        if (StringUtils.isBlank(namespace)) {
            future.completeExceptionally(new PulsarAdminException("namespace is required"));
            return false;
        }
        return true;
    }

    private static boolean validateSinkName(String tenant, String namespace,
                                          String sinkName, CompletableFuture<?> future) {
        if (!validateNamespace(tenant, namespace, future)) {
            return false;
        }
        if (StringUtils.isBlank(sinkName)) {
            future.completeExceptionally(new PulsarAdminException("sink name is required"));
            return false;
        }
        return true;
    }

}
