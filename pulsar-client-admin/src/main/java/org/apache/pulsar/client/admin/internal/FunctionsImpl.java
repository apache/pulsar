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

import static org.asynchttpclient.Dsl.get;
import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpHeaders;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.UpdateOptionsImpl;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsData;
import org.apache.pulsar.common.policies.data.FunctionInstanceStatsDataImpl;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatsImpl;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.ByteArrayPart;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

@Slf4j
public class FunctionsImpl extends ComponentResource implements Functions {

    private final WebTarget functions;
    private final AsyncHttpClient asyncHttpClient;

    public FunctionsImpl(WebTarget web, Authentication auth, AsyncHttpClient asyncHttpClient, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.functions = web.path("/admin/v3/functions");
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public List<String> getFunctions(String tenant, String namespace) throws PulsarAdminException {
        try {
            return getFunctionsAsync(tenant, namespace).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> getFunctionsAsync(String tenant, String namespace) {
        WebTarget path = functions.path(tenant).path(namespace);
        final CompletableFuture<List<String>> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            List<String> functions = response.readEntity(new GenericType<List<String>>() {});
                            future.complete(functions);
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            return getFunctionAsync(tenant, namespace, function).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionConfig> getFunctionAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function);
        final CompletableFuture<FunctionConfig> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(FunctionConfig.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FunctionStatus getFunctionStatus(
            String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            return getFunctionStatusAsync(tenant, namespace, function).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatusAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("status");
        final CompletableFuture<FunctionStatus> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(FunctionStatus.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionStatus(
            String tenant, String namespace, String function, int id) throws PulsarAdminException {
        try {
            return getFunctionStatusAsync(tenant, namespace, function, id)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> getFunctionStatusAsync(
            String tenant, String namespace, String function, int id) {
        WebTarget path =
                functions.path(tenant).path(namespace).path(function).path(Integer.toString(id)).path("status");
        final CompletableFuture<FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData> future =
                new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(
                                    FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FunctionInstanceStatsData getFunctionStats(
            String tenant, String namespace, String function, int id) throws PulsarAdminException {
        try {
            return getFunctionStatsAsync(tenant, namespace, function, id)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionInstanceStatsData> getFunctionStatsAsync(
            String tenant, String namespace, String function, int id) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path(Integer.toString(id)).path("stats");
        final CompletableFuture<FunctionInstanceStatsData> future =
                new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(FunctionInstanceStatsDataImpl.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public FunctionStats getFunctionStats(String tenant, String namespace, String function)
            throws PulsarAdminException {
        try {
            return getFunctionStatsAsync(tenant, namespace, function).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionStats> getFunctionStatsAsync(String tenant,
                                                                  String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("stats");
        final CompletableFuture<FunctionStats> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(FunctionStatsImpl.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        try {
            createFunctionAsync(functionConfig, fileName).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createFunctionAsync(FunctionConfig functionConfig, String fileName) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    post(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace())
                            .path(functionConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("functionConfig", ObjectMapperFactory.getThreadLocal()
                            .writeValueAsString(functionConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build())
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

        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override
    public void createFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException {
        try {
            createFunctionWithUrlAsync(functionConfig, pkgUrl).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> createFunctionWithUrlAsync(FunctionConfig functionConfig, String pkgUrl) {
        WebTarget path = functions.path(functionConfig.getTenant())
                .path(functionConfig.getNamespace()).path(functionConfig.getName());

        final FormDataMultiPart mp = new FormDataMultiPart();
        mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
        mp.bodyPart(new FormDataBodyPart(
                "functionConfig", new Gson().toJson(functionConfig), MediaType.APPLICATION_JSON_TYPE));

        return asyncPostRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
    }

    @Override
    public void deleteFunction(String cluster, String namespace, String function) throws PulsarAdminException {
        try {
            deleteFunctionAsync(cluster, namespace, function).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> deleteFunctionAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function);
        return asyncDeleteRequest(path);
    }

    @Override
    public void updateFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        updateFunction(functionConfig, fileName, null);
    }

    @Override
    public CompletableFuture<Void> updateFunctionAsync(FunctionConfig functionConfig, String fileName) {
        return updateFunctionAsync(functionConfig, fileName, null);
    }

    @Override
    public void updateFunction(FunctionConfig functionConfig, String fileName, UpdateOptions updateOptions)
            throws PulsarAdminException {
        try {
            updateFunctionAsync(functionConfig, fileName, updateOptions)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateFunctionAsync(
            FunctionConfig functionConfig, String fileName, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    put(functions.path(functionConfig.getTenant())
                            .path(functionConfig.getNamespace())
                            .path(functionConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("functionConfig", ObjectMapperFactory.getThreadLocal()
                            .writeValueAsString(functionConfig), MediaType.APPLICATION_JSON));

            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                builder.addBodyPart(new StringPart("updateOptions", ObjectMapperFactory.getThreadLocal()
                        .writeValueAsString(options), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }

            asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build())
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
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl,
                                      UpdateOptions updateOptions)
            throws PulsarAdminException {
        try {
            updateFunctionWithUrlAsync(functionConfig, pkgUrl, updateOptions)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> updateFunctionWithUrlAsync(
            FunctionConfig functionConfig, String pkgUrl, UpdateOptions updateOptions) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));
            mp.bodyPart(new FormDataBodyPart(
                    "functionConfig",
                    ObjectMapperFactory.getThreadLocal().writeValueAsString(functionConfig),
                    MediaType.APPLICATION_JSON_TYPE));
            UpdateOptionsImpl options = (UpdateOptionsImpl) updateOptions;
            if (options != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        ObjectMapperFactory.getThreadLocal().writeValueAsString(options),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            WebTarget path = functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace())
                    .path(functionConfig.getName());
            return asyncPutRequest(path, Entity.entity(mp, MediaType.MULTIPART_FORM_DATA));
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException {
        updateFunctionWithUrl(functionConfig, pkgUrl, null);
    }

    @Override
    public CompletableFuture<Void> updateFunctionWithUrlAsync(FunctionConfig functionConfig, String pkgUrl) {
        return updateFunctionWithUrlAsync(functionConfig, pkgUrl, null);
    }

    @Override
    public String triggerFunction(
            String tenant, String namespace, String functionName,
            String topic, String triggerValue, String triggerFile) throws PulsarAdminException {
        try {
            return triggerFunctionAsync(tenant, namespace, functionName, topic, triggerValue, triggerFile)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    private void handlePulsarAdminException(ExecutionException e) throws PulsarAdminException {
        if (e.getCause() instanceof PulsarAdminException) {
            throw (PulsarAdminException) e.getCause();
        }
    }

    @Override
    public CompletableFuture<String> triggerFunctionAsync(
            String tenant, String namespace, String function,
            String topic, String triggerValue, String triggerFile) {
        final FormDataMultiPart mp = new FormDataMultiPart();
        if (triggerFile != null) {
            mp.bodyPart(new FileDataBodyPart("dataStream",
                    new File(triggerFile),
                    MediaType.APPLICATION_OCTET_STREAM_TYPE));
        }
        if (triggerValue != null) {
            mp.bodyPart(new FormDataBodyPart("data", triggerValue, MediaType.TEXT_PLAIN_TYPE));
        }
        if (topic != null && !topic.isEmpty()) {
            mp.bodyPart(new FormDataBodyPart("topic", topic, MediaType.TEXT_PLAIN_TYPE));
        }
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("trigger");

        final CompletableFuture<String> future = new CompletableFuture<>();
        try {
            request(path).async().post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA),
                    new InvocationCallback<String>() {
                        @Override
                        public void completed(String response) {
                            future.complete(response);
                        }

                        @Override
                        public void failed(Throwable throwable) {
                            log.warn("[{}] Failed to perform http post request: {}",
                                    path.getUri(), throwable.getMessage());
                            future.completeExceptionally(getApiException(throwable.getCause()));
                        }
                    });
        } catch (PulsarAdminException cae) {
            future.completeExceptionally(cae);
        }
        return future;
    }

    @Override
    public void restartFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            restartFunctionAsync(tenant, namespace, functionName, instanceId)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> restartFunctionAsync(
            String tenant, String namespace, String function, int instanceId) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path(Integer.toString(instanceId))
                .path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void restartFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            restartFunctionAsync(tenant, namespace, functionName)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> restartFunctionAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("restart");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            stopFunctionAsync(tenant, namespace, functionName, instanceId)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> stopFunctionAsync(String tenant, String namespace, String function, int instanceId) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path(Integer.toString(instanceId))
                .path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void stopFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            stopFunctionAsync(tenant, namespace, functionName)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> stopFunctionAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("stop");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            startFunctionAsync(tenant, namespace, functionName, instanceId)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startFunctionAsync(
            String tenant, String namespace, String function, int instanceId) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path(Integer.toString(instanceId))
                .path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void startFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            startFunctionAsync(tenant, namespace, functionName)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> startFunctionAsync(String tenant, String namespace, String function) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("start");
        return asyncPostRequest(path, Entity.entity("", MediaType.APPLICATION_JSON));
    }

    @Override
    public void uploadFunction(String sourceFile, String path) throws PulsarAdminException {
        try {
            uploadFunctionAsync(sourceFile, path).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> uploadFunctionAsync(String sourceFile, String path) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder = post(functions.path("upload").getUri().toASCIIString())
                    .addBodyPart(new FilePart("data", new File(sourceFile), MediaType.APPLICATION_OCTET_STREAM))
                    .addBodyPart(new StringPart("path", path, MediaType.TEXT_PLAIN));

            asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build()).toCompletableFuture()
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
        } catch (Exception e) {
            future.completeExceptionally(getApiException(e));
        }
        return future;
    }

    @Override
    public void downloadFunction(String destinationPath, String tenant, String namespace, String functionName)
            throws PulsarAdminException {
        downloadFile(destinationPath, functions.path(tenant).path(namespace).path(functionName).path("download"));
    }

    @Override
    public CompletableFuture<Void> downloadFunctionAsync(
            String destinationPath, String tenant, String namespace, String functionName) {
        return downloadFileAsync(destinationPath,
                functions.path(tenant).path(namespace).path(functionName).path("download"));
    }

    @Override
    public void downloadFunction(String destinationPath, String path) throws PulsarAdminException {
        downloadFile(destinationPath, functions.path("download").queryParam("path", path));
    }

    @Override
    public CompletableFuture<Void> downloadFunctionAsync(String destinationFile, String path) {
        return downloadFileAsync(destinationFile, functions.path("download").queryParam("path", path));
    }

    private void downloadFile(String destinationPath, WebTarget target) throws PulsarAdminException {
        try {
            downloadFileAsync(destinationPath, target).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    private CompletableFuture<Void> downloadFileAsync(String destinationPath, WebTarget target) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            File file = new File(destinationPath);
            if (!file.exists()) {
                if (file.getParentFile() != null && !file.getParentFile().exists()) {
                    file.getParentFile().mkdirs();
                }
                file.createNewFile();
            }
            FileChannel os = new FileOutputStream(new File(destinationPath)).getChannel();

            RequestBuilder builder = get(target.getUri().toASCIIString());

            CompletableFuture<HttpResponseStatus> statusFuture =
                    asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build(),
                        new AsyncHandler<HttpResponseStatus>() {
                            private HttpResponseStatus status;

                            @Override
                            public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
                                status = responseStatus;
                                if (status.getStatusCode() != Response.Status.OK.getStatusCode()) {
                                    return State.ABORT;
                                }
                                return State.CONTINUE;
                            }

                            @Override
                            public State onHeadersReceived(HttpHeaders headers) throws Exception {
                                return State.CONTINUE;
                            }

                            @Override
                            public State onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                                os.write(bodyPart.getBodyByteBuffer());
                                return State.CONTINUE;
                            }

                            @Override
                            public HttpResponseStatus onCompleted() throws Exception {
                                return status;
                            }

                            @Override
                            public void onThrowable(Throwable t) {
                            }
                        }).toCompletableFuture();

            statusFuture
                    .thenAccept(status -> {
                        try {
                            os.close();
                        } catch (Exception e) {
                            future.completeExceptionally(getApiException(e));
                            return;
                        }

                        if (status.getStatusCode() < 200 || status.getStatusCode() >= 300) {
                            future.completeExceptionally(
                                    getApiException(Response
                                            .status(status.getStatusCode())
                                            .entity(status.getStatusText())
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
    public List<ConnectorDefinition> getConnectorsList() throws PulsarAdminException {
        try {
            Response response = request(functions.path("connectors")).get();
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw getApiException(response);
            }
            return response.readEntity(new GenericType<List<ConnectorDefinition>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Set<String> getSources() throws PulsarAdminException {
        return getConnectorsList().stream().filter(c -> !StringUtils.isEmpty(c.getSourceClass()))
                .map(ConnectorDefinition::getName).collect(Collectors.toSet());
    }

    @Override
    public Set<String> getSinks() throws PulsarAdminException {
        return getConnectorsList().stream().filter(c -> !StringUtils.isEmpty(c.getSinkClass()))
                .map(ConnectorDefinition::getName).collect(Collectors.toSet());
    }

    public List<WorkerInfo> getCluster() throws PulsarAdminException {
        try {
            return request(functions.path("cluster")).get(new GenericType<List<WorkerInfo>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionState getFunctionState(String tenant, String namespace, String function, String key)
        throws PulsarAdminException {
        try {
            return getFunctionStateAsync(tenant, namespace, function, key)
                    .get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<FunctionState> getFunctionStateAsync(
            String tenant, String namespace, String function, String key) {
        WebTarget path = functions.path(tenant).path(namespace).path(function).path("state").path(key);
        final CompletableFuture<FunctionState> future = new CompletableFuture<>();
        asyncGetRequest(path,
                new InvocationCallback<Response>() {
                    @Override
                    public void completed(Response response) {
                        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                            future.completeExceptionally(getApiException(response));
                        } else {
                            future.complete(response.readEntity(FunctionState.class));
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        future.completeExceptionally(getApiException(throwable.getCause()));
                    }
                });
        return future;
    }

    @Override
    public void putFunctionState(String tenant, String namespace, String function, FunctionState state)
            throws PulsarAdminException {
        try {
            putFunctionStateAsync(tenant, namespace, function, state).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    @Override
    public CompletableFuture<Void> putFunctionStateAsync(
            String tenant, String namespace, String function, FunctionState state) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    post(functions.path(tenant).path(namespace).path(function)
                            .path("state").path(state.getKey()).getUri().toASCIIString());
            builder.addBodyPart(new StringPart("state", ObjectMapperFactory.getThreadLocal()
                    .writeValueAsString(state), MediaType.APPLICATION_JSON));
            asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build())
                    .toCompletableFuture()
                    .thenAccept(response -> {
                        if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                            future.completeExceptionally(getApiException(
                                    Response.status(response.getStatusCode())
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
            future.completeExceptionally(e);
        }
        return future;
    }

    public void updateOnWorkerLeader(String tenant, String namespace,
                                     String function, byte[] functionMetaData,
                                     boolean delete) throws PulsarAdminException {
        try {
            updateOnWorkerLeaderAsync(tenant, namespace, function,
                    functionMetaData, delete).get(this.readTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            handlePulsarAdminException(e);
            throw new PulsarAdminException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarAdminException(e);
        } catch (TimeoutException e) {
            throw new PulsarAdminException.TimeoutException(e);
        }
    }

    public CompletableFuture<Void> updateOnWorkerLeaderAsync(String tenant, String namespace,
                                                             String function, byte[] functionMetaData,
                                                             boolean delete) {
        final CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            RequestBuilder builder =
                    put(functions.path("leader").path(tenant).path(namespace)
                            .path(function).getUri().toASCIIString())
                            .addBodyPart(new ByteArrayPart("functionMetaData", functionMetaData))
                    .addBodyPart(new StringPart("delete", Boolean.toString(delete)));

            asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build())
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

        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }
}
