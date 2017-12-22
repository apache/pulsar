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

import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.*;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.glassfish.jersey.media.multipart.FormDataBodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import java.util.List;

public class FunctionsImpl extends BaseResource implements Functions {

    private final WebTarget functions;

    public FunctionsImpl(WebTarget web, Authentication auth) {
        super(auth);
        this.functions = web.path("/functions");
    }

    @Override
    public List<String> getFunctions(String tenant, String namespace) throws PulsarAdminException {
        try {
            return request(functions.path(tenant).path(namespace)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public FunctionConfig getFunction(String tenant, String namespace, String function) throws PulsarAdminException {
        try {
            return request(functions.path(tenant).path(namespace).path(function)).get(FunctionConfig.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createFunction(FunctionConfig functionConfig, byte[] code) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            mp.bodyPart(new FormDataBodyPart("data", code, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            mp.bodyPart(new FormDataBodyPart("sourceTopic", functionConfig.getSourceTopic(),
                    MediaType.APPLICATION_JSON_TYPE));
            mp.bodyPart(new FormDataBodyPart("sinkTopic", functionConfig.getSinkTopic(),
                    MediaType.APPLICATION_JSON_TYPE));
            mp.bodyPart(new FormDataBodyPart("inputSerdeClassName", functionConfig.getInputSerdeClassName(),
                    MediaType.APPLICATION_JSON_TYPE));
            mp.bodyPart(new FormDataBodyPart("outputSerdeClassName", functionConfig.getOutputSerdeClassName(),
                    MediaType.APPLICATION_JSON_TYPE));
            mp.bodyPart(new FormDataBodyPart("className", functionConfig.getClassName(),
                    MediaType.APPLICATION_JSON_TYPE));
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNameSpace()).path(functionConfig.getName()))
                    .put(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
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
    public void updateFunction(FunctionConfig functionConfig, byte[] code) throws PulsarAdminException {
        try {
            final FormDataMultiPart mp = new FormDataMultiPart();
            if (code != null) {
                mp.bodyPart(new FormDataBodyPart("data", code, MediaType.APPLICATION_OCTET_STREAM_TYPE));
            }
            if (functionConfig.getSourceTopic() != null) {
                mp.bodyPart(new FormDataBodyPart("sourceTopic", functionConfig.getSourceTopic(),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            if (functionConfig.getSinkTopic() != null) {
                mp.bodyPart(new FormDataBodyPart("sinkTopic", functionConfig.getSinkTopic(),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            if (functionConfig.getInputSerdeClassName() != null) {
                mp.bodyPart(new FormDataBodyPart("inputSerdeClassName", functionConfig.getInputSerdeClassName(),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            if (functionConfig.getOutputSerdeClassName() != null) {
                mp.bodyPart(new FormDataBodyPart("outputSerdeClassName", functionConfig.getOutputSerdeClassName(),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            if (functionConfig.getClassName() != null) {
                mp.bodyPart(new FormDataBodyPart("className", functionConfig.getClassName(),
                        MediaType.APPLICATION_JSON_TYPE));
            }
            request(functions.path(functionConfig.getTenant()).path(functionConfig.getNameSpace()).path(functionConfig.getName()))
                    .post(Entity.entity(mp, MediaType.MULTIPART_FORM_DATA), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
