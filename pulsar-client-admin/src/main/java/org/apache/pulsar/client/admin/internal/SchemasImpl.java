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
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.protocol.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetAllVersionsSchemaResponse;
import org.apache.pulsar.common.protocol.schema.GetSchemaResponse;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.LongSchemaVersionResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.List;
import java.util.stream.Collectors;

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
    public SchemaInfoWithVersion getSchemaInfoWithVersion(String topic) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            GetSchemaResponse response = request(schemaPath(tn)).get(GetSchemaResponse.class);
            return convertGetSchemaResponseToSchemaInfoWithVersion(tn, response);
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

        createSchema(topic, convertSchemaInfoToPostSchemaPayload(schemaInfo));
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

    @Override
    public IsCompatibilityResponse testCompatibility(String topic, PostSchemaPayload payload) throws PulsarAdminException {
        try {
            TopicName tn = TopicName.get(topic);
            return request(compatibilityPath(tn)).post(Entity.json(payload), IsCompatibilityResponse.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Long getVersionBySchema(String topic, PostSchemaPayload payload) throws PulsarAdminException {
        try {
            return request(versionPath(TopicName.get(topic))).post(Entity.json(payload), LongSchemaVersionResponse.class).getVersion();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public IsCompatibilityResponse testCompatibility(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        try {
            return request(compatibilityPath(TopicName.get(topic)))
                    .post(Entity.json(convertSchemaInfoToPostSchemaPayload(schemaInfo)), IsCompatibilityResponse.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Long getVersionBySchema(String topic, SchemaInfo schemaInfo) throws PulsarAdminException {
        try {
            return request(versionPath(TopicName.get(topic)))
                    .post(Entity.json(convertSchemaInfoToPostSchemaPayload(schemaInfo)), LongSchemaVersionResponse.class).getVersion();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<SchemaInfo> getAllSchemas(String topic) throws PulsarAdminException {
        try {
            TopicName topicName = TopicName.get(topic);
            return request(schemasPath(TopicName.get(topic)))
                    .get(GetAllVersionsSchemaResponse.class)
                    .getGetSchemaResponses().stream()
                    .map(getSchemaResponse -> convertGetSchemaResponseToSchemaInfo(topicName, getSchemaResponse))
                    .collect(Collectors.toList());

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

    private WebTarget versionPath(TopicName topicName) {
        return target
                .path(topicName.getTenant())
                .path(topicName.getNamespacePortion())
                .path(topicName.getEncodedLocalName())
                .path("version");
    }

    private WebTarget schemasPath(TopicName topicName) {
        return target
                .path(topicName.getTenant())
                .path(topicName.getNamespacePortion())
                .path(topicName.getEncodedLocalName())
                .path("schemas");
    }

    private WebTarget compatibilityPath(TopicName topicName) {
        return target
                .path(topicName.getTenant())
                .path(topicName.getNamespacePortion())
                .path(topicName.getEncodedLocalName())
                .path("compatibility");
    }

    // the util function converts `GetSchemaResponse` to `SchemaInfo`
    static SchemaInfo convertGetSchemaResponseToSchemaInfo(TopicName tn,
                                                           GetSchemaResponse response) {
        SchemaInfo info = new SchemaInfo();
        byte[] schema;
        if (response.getType() == SchemaType.KEY_VALUE) {
            schema = DefaultImplementation.convertKeyValueDataStringToSchemaInfoSchema(response.getData().getBytes(UTF_8));
        } else {
            schema = response.getData().getBytes(UTF_8);
        }
        info.setSchema(schema);
        info.setType(response.getType());
        info.setProperties(response.getProperties());
        info.setName(tn.getLocalName());
        return info;
    }

    static SchemaInfoWithVersion convertGetSchemaResponseToSchemaInfoWithVersion(TopicName tn,
                                                           GetSchemaResponse response) {

        return  SchemaInfoWithVersion
                .builder()
                .schemaInfo(convertGetSchemaResponseToSchemaInfo(tn, response))
                .version(response.getVersion())
                .build();
    }




    // the util function exists for backward compatibility concern
    static String convertSchemaDataToStringLegacy(SchemaInfo schemaInfo) {
        byte[] schemaData = schemaInfo.getSchema();
        if (null == schemaInfo.getSchema()) {
            return "";
        }

        if (schemaInfo.getType() == SchemaType.KEY_VALUE) {
           return DefaultImplementation.convertKeyValueSchemaInfoDataToString(DefaultImplementation.decodeKeyValueSchemaInfo(schemaInfo));
        }

        return new String(schemaData, UTF_8);
    }

    static PostSchemaPayload convertSchemaInfoToPostSchemaPayload(SchemaInfo schemaInfo) {

        PostSchemaPayload payload = new PostSchemaPayload();
        payload.setType(schemaInfo.getType().name());
        payload.setProperties(schemaInfo.getProperties());
        // for backward compatibility concern, we convert `bytes` to `string`
        // we can consider fixing it in a new version of rest endpoint
        payload.setSchema(convertSchemaDataToStringLegacy(schemaInfo));
        return payload;
    }
}
