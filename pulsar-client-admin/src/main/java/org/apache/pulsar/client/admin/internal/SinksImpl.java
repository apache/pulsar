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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Sink;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.SinkStatus;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.request.body.multipart.FilePart;
import org.asynchttpclient.request.body.multipart.StringPart;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.List;

import static org.asynchttpclient.Dsl.post;
import static org.asynchttpclient.Dsl.put;

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
        try {
            Response response = request(sink.path(tenant).path(namespace)).get();
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
    public SinkConfig getSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        try {
             Response response = request(sink.path(tenant).path(namespace).path(sinkName)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SinkConfig.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SinkStatus getSinkStatus(
            String tenant, String namespace, String sinkName) throws PulsarAdminException {
        try {
            Response response = request(sink.path(tenant).path(namespace).path(sinkName).path("status")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SinkStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SinkStatus.SinkInstanceStatus.SinkInstanceStatusData getSinkStatus(
            String tenant, String namespace, String sinkName, int id) throws PulsarAdminException {
        try {
            Response response = request(
                    sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(id)).path("status"))
                            .get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SinkStatus.SinkInstanceStatus.SinkInstanceStatusData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException {
        try {
            RequestBuilder builder = post(sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace()).path(sinkConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sinkConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(sinkConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(sink, builder).build()).get();

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }

        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart("sinkConfig",
                    new Gson().toJson(sinkConfig),
                MediaType.APPLICATION_JSON_TYPE));
            request(sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace()).path(sinkConfig.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteSink(String cluster, String namespace, String function) throws PulsarAdminException {
        try {
            request(sink.path(cluster).path(namespace).path(function))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSink(SinkConfig sinkConfig, String fileName, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            RequestBuilder builder = put(sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace()).path(sinkConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sinkConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(sinkConfig), MediaType.APPLICATION_JSON));

            if (updateOptions != null) {
                builder.addBodyPart(new StringPart("updateOptions", ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(sink, builder).build()).get();

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSink(SinkConfig sinkConfig, String fileName) throws PulsarAdminException {
       updateSink(sinkConfig, fileName, null);
    }

    @Override
    public void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart(
                    "sinkConfig",
                    new Gson().toJson(sinkConfig),
                    MediaType.APPLICATION_JSON_TYPE));

            if (updateOptions != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions),
                        MediaType.APPLICATION_JSON_TYPE));
            }

            request(sink.path(sinkConfig.getTenant()).path(sinkConfig.getNamespace())
                    .path(sinkConfig.getName())).put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSinkWithUrl(SinkConfig sinkConfig, String pkgUrl) throws PulsarAdminException {
        updateSinkWithUrl(sinkConfig, pkgUrl, null);
    }

    @Override
    public void restartSink(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                    .path("restart")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void restartSink(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(functionName).path("restart"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopSink(String tenant, String namespace, String sinkName, int instanceId)
            throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(instanceId))
                    .path("stop")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(sinkName).path("stop"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startSink(String tenant, String namespace, String sinkName, int instanceId)
            throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(sinkName).path(Integer.toString(instanceId))
                    .path("start")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startSink(String tenant, String namespace, String sinkName) throws PulsarAdminException {
        try {
            request(sink.path(tenant).path(namespace).path(sinkName).path("start"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<ConnectorDefinition> getBuiltInSinks() throws PulsarAdminException {
        try {
            Response response = request(sink.path("builtinsinks")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(new GenericType<List<ConnectorDefinition>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
