/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.admin.internal;

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import com.yahoo.pulsar.client.admin.Properties;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.common.policies.data.ErrorData;
import com.yahoo.pulsar.common.policies.data.PropertyAdmin;

public class PropertiesImpl extends BaseResource implements Properties {
    private final WebTarget properties;

    public PropertiesImpl(WebTarget web, Authentication auth) {
        super(auth);
        properties = web.path("/properties");
    }

    @Override
    public List<String> getProperties() throws PulsarAdminException {
        try {
            return request(properties).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public PropertyAdmin getPropertyAdmin(String property) throws PulsarAdminException {
        try {
            return request(properties.path(property)).get(PropertyAdmin.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createProperty(String property, PropertyAdmin config) throws PulsarAdminException {
        try {
            request(properties.path(property)).put(Entity.entity(config, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateProperty(String property, PropertyAdmin config) throws PulsarAdminException {
        try {
            request(properties.path(property)).post(Entity.entity(config, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteProperty(String property) throws PulsarAdminException {
        try {
            request(properties.path(property)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public WebTarget getWebTarget() {
        return properties;
    }
}
