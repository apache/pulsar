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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatusList;
import org.apache.pulsar.functions.utils.Utils;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.List;

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
    public FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
             Response response = request(functions.path(tenant).path(namespace).path(function)).get();
            if (!response.getStatusInfo().equals(Response.Status.OK)) {
                throw new ClientErrorException(response);
            }
            String jsonResponse = response.readEntity(String.class);
            FunctionConfig.Builder functionConfigBuilder = FunctionConfig.newBuilder();
            Utils.mergeJson(jsonResponse, functionConfigBuilder);
            return functionConfigBuilder.build();
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
            Utils.mergeJson(jsonResponse, functionStatusBuilder);
            return functionStatusBuilder.build();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunction(FunctionConfig functionConfig, String fileName) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();

            mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));

            mp.bodyPart(new FormDataBodyPart("functionConfig",
                Utils.printJson(functionConfig),
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
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            if (fileName != null) {
                mp.bodyPart(new FileDataBodyPart("data", new File(fileName), MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
            mp.bodyPart(new FormDataBodyPart("functionConfig",
                Utils.printJson(functionConfig),
                MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNamespace()).path(functionConfig.getName()))
                    .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String triggerFunction(String tenant, String namespace, String functionName, String triggerValue, String triggerFile) throws PulsarAdminException {
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
            String response = request(functions.path(tenant).path(namespace).path(functionName).path("trigger"))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), String.class);
            return response;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
