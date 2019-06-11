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
import org.apache.pulsar.client.admin.Source;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.functions.UpdateOptions;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.policies.data.SourceStatus;
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
public class SourcesImpl extends ComponentResource implements Sources, Source {

    private final WebTarget source;
    private final AsyncHttpClient asyncHttpClient;

    public SourcesImpl(WebTarget web, Authentication auth, AsyncHttpClient asyncHttpClient, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.source = web.path("/admin/v3/source");
        this.asyncHttpClient = asyncHttpClient;
    }

    @Override
    public List<String> listSources(String tenant, String namespace) throws PulsarAdminException {
        try {
            Response response = request(source.path(tenant).path(namespace)).get();
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
    public SourceConfig getSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        try {
             Response response = request(source.path(tenant).path(namespace).path(sourceName)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SourceConfig.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SourceStatus getSourceStatus(
            String tenant, String namespace, String sourceName) throws PulsarAdminException {
        try {
            Response response = request(source.path(tenant).path(namespace).path(sourceName).path("status")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SourceStatus.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SourceStatus.SourceInstanceStatus.SourceInstanceStatusData getSourceStatus(
            String tenant, String namespace, String sourceName, int id) throws PulsarAdminException {
        try {
            Response response = request(
                    source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(id)).path("status"))
                            .get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(SourceStatus.SourceInstanceStatus.SourceInstanceStatusData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException {
        try {
            RequestBuilder builder = post(source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace()).path(sourceConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sourceConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(sourceConfig), MediaType.APPLICATION_JSON));

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(source, builder).build()).get();
            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart("sourceConfig",
                    new Gson().toJson(sourceConfig),
                MediaType.APPLICATION_JSON_TYPE));
            request(source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace()).path(sourceConfig.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteSource(String cluster, String namespace, String function) throws PulsarAdminException {
        try {
            request(source.path(cluster).path(namespace).path(function))
                    .delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSource(SourceConfig sourceConfig, String fileName, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            RequestBuilder builder = put(source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace()).path(sourceConfig.getName()).getUri().toASCIIString())
                    .addBodyPart(new StringPart("sourceConfig", ObjectMapperFactory.getThreadLocal().writeValueAsString(sourceConfig), MediaType.APPLICATION_JSON));

            if (updateOptions != null) {
                builder.addBodyPart(new StringPart("updateOptions", ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions), MediaType.APPLICATION_JSON));
            }

            if (fileName != null && !fileName.startsWith("builtin://")) {
                // If the function code is built in, we don't need to submit here
                builder.addBodyPart(new FilePart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM));
            }
            org.asynchttpclient.Response response = asyncHttpClient.executeRequest(addAuthHeaders(source, builder).build()).get();

            if (response.getStatusCode() < 200 || response.getStatusCode() >= 300) {
                throw getApiException(Response.status(response.getStatusCode()).entity(response.getResponseBody()).build());
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSource(SourceConfig sourceConfig, String fileName) throws PulsarAdminException {
        updateSource(sourceConfig, fileName, null);
    }

    @Override
    public void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl, UpdateOptions updateOptions) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart(
                    "sourceConfig",
                    new Gson().toJson(sourceConfig),
                    MediaType.APPLICATION_JSON_TYPE));

            if (updateOptions != null) {
                mp.bodyPart(new FormDataBodyPart(
                        "updateOptions",
                        ObjectMapperFactory.getThreadLocal().writeValueAsString(updateOptions),
                        MediaType.APPLICATION_JSON_TYPE));
            }

            request(source.path(sourceConfig.getTenant()).path(sourceConfig.getNamespace())
                    .path(sourceConfig.getName())).put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateSourceWithUrl(SourceConfig sourceConfig, String pkgUrl) throws PulsarAdminException {
        updateSourceWithUrl(sourceConfig, pkgUrl, null);
    }

    @Override
    public void restartSource(String tenant, String namespace, String functionName, int instanceId)
            throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(functionName).path(Integer.toString(instanceId))
                    .path("restart")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void restartSource(String tenant, String namespace, String functionName) throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(functionName).path("restart"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopSource(String tenant, String namespace, String sourceName, int instanceId)
            throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(instanceId))
                    .path("stop")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void stopSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(sourceName).path("stop"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startSource(String tenant, String namespace, String sourceName, int instanceId)
            throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(sourceName).path(Integer.toString(instanceId))
                    .path("start")).post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void startSource(String tenant, String namespace, String sourceName) throws PulsarAdminException {
        try {
            request(source.path(tenant).path(namespace).path(sourceName).path("start"))
                    .post(Entity.entity("", MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<ConnectorDefinition> getBuiltInSources() throws PulsarAdminException {
        try {
            Response response = request(source.path("builtinsources")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw getApiException(response);
            }
            return response.readEntity(new GenericType<List<ConnectorDefinition>>() {});
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
