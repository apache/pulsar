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

import static java.nio.charset.StandardCharsets.UTF_8;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;

public class SchemasImpl extends BaseResource implements Schemas {

    private final WebTarget target;

    public SchemasImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        this.target = web.path("/admin/v2/schemas");
    }

    @Override
    public SchemaInfo getSchemaInfo(String topic) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            GetSchemaResponse response = request(schemaPath(tn)).get(GetSchemaResponse.class);
            return convertGetSchemaResponseToSchemaInfo(tn, response);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo(String topic, long version) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            GetSchemaResponse response = request(schemaPath(tn).path(Long.toString(version)))
                .get(GetSchemaResponse.class);
            return convertGetSchemaResponseToSchemaInfo(tn, response);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteSchema(String topic) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            request(schemaPath(tn)).delete(DeleteSchemaResponse.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSchema(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        PostSchemaPayload payload = new PostSchemaPayload();
        payload.setType(schemaInfo.getType().name());
        payload.setProperties(schemaInfo.getProperties());
        // for backward compatibility concern, we convert `bytes` to `string`
        // we can consider fixing it in a new version of rest endpoint
        payload.setSchema(convertSchemaDataToStringLegacy(schemaInfo.getSchema()));

        createSchema(topic, payload);
    }

    @Override
    public void createSchema(String topic, PostSchemaPayload payload) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            request(schemaPath(tn))
                .post(Entity.json(payload), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private WebTarget schemaPath(TopicName topicName) {
        return target
            .path(topicName.getTenant())
            .path(topicName.getNamespacePortion())
            .path(topicName.getEncodedLocalName())
            .path("schema");
    }

    // the util function converts `GetSchemaResponse` to `SchemaInfo`
    static SchemaInfo convertGetSchemaResponseToSchemaInfo(TopicName tn,
                                                           GetSchemaResponse response) {
        SchemaInfo info = new SchemaInfo();
        info.setSchema(response.getData().getBytes(UTF_8));
        info.setType(response.getType());
        info.setProperties(response.getProperties());
        info.setName(tn.getLocalName());
        return info;
    }


    // the util function exists for backward compatibility concern
    static String convertSchemaDataToStringLegacy(byte[] schemaData) {
        if (null == schemaData) {
            return "";
        }

        return new String(schemaData, UTF_8);
    }
}
