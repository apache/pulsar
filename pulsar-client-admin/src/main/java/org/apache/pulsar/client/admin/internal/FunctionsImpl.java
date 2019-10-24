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
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpHeaders;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionState;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.FunctionStats;
import org.apache.pulsar.common.policies.data.FunctionStatus;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.HttpResponseBodyPart;
import org.asynchttpclient.HttpResponseStatus;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.asynchttpclient.Dsl.get;
import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;

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
            Response response = request(functions.path(tenant).path(namespace)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
             Response response = request(functions.path(tenant).path(namespace).path(function)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionConfig.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionStatus getFunctionStatus(
            String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            Response response = request(functions.path(tenant).path(namespace).path(function).path("status")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData getFunctionStatus(
            String tenant, String namespace, String function, int id) throws PulsarAdminException {
        try {
            Response response = request(
                    functions.path(tenant).path(namespace).path(function).path(Integer.toString(id)).path("status"))
                            .get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionStatus.FunctionInstanceStatus.FunctionInstanceStatusData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData getFunctionStats(String tenant, String namespace, String function, int id) throws PulsarAdminException {
        try {
            Response response = request(
                    functions.path(tenant).path(namespace).path(function).path(Integer.toString(id)).path("stats")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionStats.FunctionInstanceStats.FunctionInstanceStatsData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionStats getFunctionStats(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            Response response = request(
                    functions.path(tenant).path(namespace).path(function).path("stats")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionStats.class);
        } catch (Exception e) {
            throw getApiException(e);
        }    }

    @Override
    public void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        try {
            RequestBuilder builder = post(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("functionConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(functionConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
               builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build()).get();

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }

        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionConfig",
                new Gson().toJson(functionConfig),
                MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteFunction(String cluster, String namespace, String function) throws PulsarAdminException {
        try {
            request(functions.path(cluster).path(namespace).path(function))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        updateFunction(functionConfig, fileName, null);
    }

        @Override
    public void updateFunction(FunctionConfig functionConfig, String fileName, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            RequestBuilder builder = put(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("functionConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(functionConfig), MediaType.APPLICATION_JSON));

            if (updateOptions != null) {
                   builder.addBodyPart(new StringPart("updateOptions", ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build()).get();

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart(
                    "functionConfig",
                    ObjectMapperFactory.getThreadLocal().writeValueAsString(functionConfig),
                    MediaType.APPLICATION_JSON_TYPE));

            if (updateOptions != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions),
                        MediaType.APPLICATION_JSON_TYPE));
            }

            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace())
                    .path(functionConfig.getName())).put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateFunctionWithUrl(FunctionConfig functionConfig, String pkgUrl) throws PulsarAdminException {
        updateFunctionWithUrl(functionConfig, pkgUrl, null);
    }

    @Override
    public String triggerFunction(String tenant, String namespace, String functionName, String topic, String triggerValue, String triggerFile) throws PulsarAdminException {
        try {
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
            return request(functions.path(tenant).path(namespace).path(functionName).path("trigger"))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), String.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void restartFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                    .path("restart")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void restartFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path("restart"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                    .path("stop")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path("stop"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startFunction(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                    .path("start")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startFunction(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            request(functions.path(tenant).path(namespace).path(functionName).path("start"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void uploadFunction(String sourceFile, String path) throws PulsarAdminException {
        try {
            RequestBuilder builder = post(functions.path("upload").getUri().toASCIIString())
                    .addBodyPart(new FilePart("data", new File(sourceFile), MediaType.APPLICATION_OCTET_STREAM))
                    .addBodyPart(new StringPart("path", path, MediaType.TEXT_PLAIN));

            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build()).get();
            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void downloadFunction(String destinationPath, String tenant, String namespace, String functionName) throws PulsarAdminException {
        downloadFile(destinationPath, functions.path(tenant).path(namespace).path(functionName).path("download"));
    }

    @Override
    public void downloadFunction(String destinationPath, String path) throws PulsarAdminException {
        downloadFile(destinationPath, functions.path("download").queryParam("path", path));
    }

    private void downloadFile(String destinationPath, WebTarget target) throws PulsarAdminException {
        HttpResponseStatus status;
        try {
            File file = new File(destinationPath);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileChannel os = new FileOutputStream(new File(destinationPath)).getChannel();

            RequestBuilder builder = get(target.getUri().toASCIIString());

            Future<HttpResponseStatus> whenStatusCode
                    = asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build(), new AsyncHandler<HttpResponseStatus>() {
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
            });

            status = whenStatusCode.get();
            os.close();

            if (status.getStatusCode() < 200 || status.getStatusCode() >= 300) {
                throw getApiException(Response.status(status.getStatusCode()).entity(status.getStatusText()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<ConnectorDefinition> getConnectorsList() throws PulsarAdminException {
        try {
            Response response = request(functions.path("connectors")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
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
            Response response = request(functions.path(tenant)
                .path(namespace).path(function).path("state").path(key)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(FunctionState.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void putFunctionState(String tenant, String namespace, String function, FunctionState state)
            throws PulsarAdminException {
        try {
             RequestBuilder builder = post(functions.path(tenant).path(namespace).path(function).path("state").path(state.getKey()).getUri().toASCIIString());
             builder.addBodyPart(new StringPart("state", ObjectMapperFactory.getThreadLocal().writeValueAsString(state), MediaType.APPLICATION_JSON));
             org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(functions, builder).build()).get();

             if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                 throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
             }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
