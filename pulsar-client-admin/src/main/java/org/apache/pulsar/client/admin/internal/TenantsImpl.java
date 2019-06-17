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

import java.util.List;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Properties;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.TenantInfo;

@SuppressWarnings("deprecation")
public class TenantsImpl extends BaseResource implements Tenants, Properties {
    private final WebTarget adminTenants;

    public TenantsImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminTenants = web.path("/admin/v2/tenants");
    }

    @Override
    public List<String> getTenants() throws PulsarAdminException {
        try {
            return request(adminTenants).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public TenantInfo getTenantInfo(String tenant) throws PulsarAdminException {
        try {
            return request(adminTenants.path(tenant)).get(TenantInfo.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void createTenant(String tenant, TenantInfo config) throws PulsarAdminException {
        try {
            request(adminTenants.path(tenant)).put(Entity.entity(config, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateTenant(String tenant, TenantInfo config) throws PulsarAdminException {
        try {
            request(adminTenants.path(tenant)).post(Entity.entity(config, MediaType.APPLICATION_JSON),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteTenant(String tenant) throws PulsarAdminException {
        try {
            request(adminTenants.path(tenant)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    // Compat method names

    @Override
    public void createProperty(String tenant, TenantInfo config) throws PulsarAdminException {
        createTenant(tenant, config);
    }

    @Override
    public void updateProperty(String tenant, TenantInfo config) throws PulsarAdminException {
        updateTenant(tenant, config);
    }

    @Override
    public void deleteProperty(String tenant) throws PulsarAdminException {
        deleteTenant(tenant);
    }

    @Override
    public List<String> getProperties() throws PulsarAdminException {
        return getTenants();
    }

    @Override
    public TenantInfo getPropertyAdmin(String tenant) throws PulsarAdminException {
        return getTenantInfo(tenant);
    }

    public WebTarget getWebTarget() {
        return adminTenants;
    }
}
