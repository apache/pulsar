/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.admin.internal;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.schema.DeleteSchemaResponse;
import org.apache.pulsar.common.schema.GetSchemaResponse;
import org.apache.pulsar.common.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.PostSchemaResponse;
import org.apache.pulsar.common.schema.SchemaInfo;

public class SchemasImpl extends BaseResource implements Schemas {

    private final WebTarget target;

    public SchemasImpl(WebTarget target, Authentication auth) {
        super(auth);
        this.target = target;
    }

    @Override
    public SchemaInfo getSchemaInfo(String tennant, String namespace, String topic) throws PulsarAdminException {
        try {
            GetSchemaResponse response = request(schemaPath(tennant, namespace, topic)).get(GetSchemaResponse.class);
            SchemaInfo info = new SchemaInfo();
            info.setSchema(response.getData().getBytes());
            info.setType(response.getType());
            info.setProperties(response.getProperties());
            info.setName(topic);
            return info;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public SchemaInfo getSchemaInfo(String tennant, String namespace, String topic, long version) throws PulsarAdminException {
        try {
            GetSchemaResponse response = request(schemaPath(tennant, namespace, topic).path(Long.toString(version))).get(GetSchemaResponse.class);
            SchemaInfo info = new SchemaInfo();
            info.setSchema(response.getData().getBytes());
            info.setType(response.getType());
            info.setProperties(response.getProperties());
            info.setName(topic);
            return info;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteSchema(String tennant, String namespace, String topic) throws PulsarAdminException {
        try {
            request(schemaPath(tennant, namespace, topic)).delete(DeleteSchemaResponse.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createSchema(String tennant, String namespace, String topic, SchemaInfo info) throws PulsarAdminException {
        try {
            PostSchemaPayload payload = new PostSchemaPayload();
            request(schemaPath(tennant, namespace, topic))
                .post(Entity.json(payload))
                .readEntity(PostSchemaResponse.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private WebTarget schemaPath(String tennant, String namespace, String topic) {
        return target.path(tennant).path(namespace).path(topic).path("schema");
    }
}
