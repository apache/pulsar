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

import com.google.protobuf.AbstractMessage.Builder;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.StandardCopyOption;
import java.util.List;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

@Slf4j
public class FunctionsImpl extends BaseResource implements Functions {

    private final WebTarget functions;

    public FunctionsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.functions = web.path("/admin/functions");
    }

    @Override
    public List<String> getFunctions(String tenant, String namespace) throws PulsarAdminException {
        try {
            Response response = request(functions.path(tenant).path(namespace)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            return response.readEntity(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionDetails getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
             Response response = request(functions.path(tenant).path(namespace).path(function)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            String jsonResponse = response.readEntity(String.class);
            FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();
            mergeJson(jsonResponse, functionDetailsBuilder);
            return functionDetailsBuilder.build();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionStatusList getFunctionStatus(
            String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            Response response = request(functions.path(tenant).path(namespace).path(function).path("status")).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            String jsonResponse = response.readEntity(String.class);
            FunctionStatusList.Builder functionStatusBuilder = FunctionStatusList.newBuilder();
            mergeJson(jsonResponse, functionStatusBuilder);
            return functionStatusBuilder.build();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionDetails",
                printJson(functionDetails),
                MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionDetails.getTenant()).path(functionDetails.getNamespace()).path(functionDetails.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunctionWithUrl(FunctionDetails functionDetails, String pkgUrl) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionDetails",
                printJson(functionDetails),
                MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionDetails.getTenant()).path(functionDetails.getNamespace()).path(functionDetails.getName()))
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
    public void updateFunction(FunctionDetails functionDetails, String fileName) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            if (fileName != null) {
                mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
            mp.bodyPart(new FormDataBodyPart("functionDetails",
                printJson(functionDetails),
                MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionDetails.getTenant()).path(functionDetails.getNamespace()).path(functionDetails.getName()))
                    .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateFunctionWithUrl(FunctionDetails functionDetails, String pkgUrl) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FormDataBodyPart("url", pkgUrl, MediaType.TEXT_PLAIN_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionDetails", printJson(functionDetails),
                    MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionDetails.getTenant()).path(functionDetails.getNamespace())
                    .path(functionDetails.getName())).put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA),
                            ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
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
    public void uploadFunction(String sourceFile, String path) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FileDataBodyPart("data", new File(sourceFile), MediaType.APPLICATION_OCTET_STREAM_TYPE));

            mp.bodyPart(new FormDataBodyPart("path", path, MediaType.TEXT_PLAIN_TYPE));
            request(functions.path("upload"))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void downloadFunction(String destinationPath, String path) throws PulsarAdminException {
        try {
            InputStream response = request(functions.path("download")
                    .queryParam("path", path)).get(InputStream.class);
            if (response != null) {
                File targetFile = new File(destinationPath);
                java.nio.file.Files.copy(
                        response,
                        targetFile.toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
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
                throw new ClientErrorException(response);
            }
            return response.readEntity(new GenericType<List<ConnectorDefinition>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public static void mergeJson(String json, Builder builder) throws IOException {
        JsonFormat.parser().merge(json, builder);
    }

    public static String printJson(MessageOrBuilder msg) throws IOException {
        return JsonFormat.printer().print(msg);
    }
}