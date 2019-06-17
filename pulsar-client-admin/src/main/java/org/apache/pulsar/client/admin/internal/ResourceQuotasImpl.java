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

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.ResourceQuotas;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.ResourceQuota;

public class ResourceQuotasImpl extends BaseResource implements ResourceQuotas {

    private final WebTarget adminQuotas;
    private final WebTarget adminV2Quotas;

    public ResourceQuotasImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminQuotas = web.path("/admin/resource-quotas");
        adminV2Quotas = web.path("/admin/v2/resource-quotas");
    }

    public ResourceQuota getDefaultResourceQuota() throws PulsarAdminException {
        try {
            return request(adminV2Quotas).get(ResourceQuota.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public void setDefaultResourceQuota(ResourceQuota quota) throws PulsarAdminException {
        try {
            request(adminV2Quotas).post(Entity.entity(quota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public ResourceQuota getNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle);
            return request(path).get(ResourceQuota.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public void setNamespaceBundleResourceQuota(String namespace, String bundle, ResourceQuota quota)
            throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle);
            request(path).post(Entity.entity(quota, MediaType.APPLICATION_JSON), ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public void resetNamespaceBundleResourceQuota(String namespace, String bundle) throws PulsarAdminException {
        try {
            NamespaceName ns = NamespaceName.get(namespace);
            WebTarget path = namespacePath(ns, bundle);
            request(path).delete();
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    private WebTarget namespacePath(NamespaceName namespace, String... parts) {
        final WebTarget base = namespace.isV2() ? adminV2Quotas : adminQuotas;
        WebTarget namespacePath = base.path(namespace.toString());
        namespacePath = WebTargets.addParts(namespacePath, parts);
        return namespacePath;
    }
}

