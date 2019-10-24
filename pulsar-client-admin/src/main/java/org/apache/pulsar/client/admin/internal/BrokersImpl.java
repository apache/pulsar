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
import java.util.Map;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;

import org.apache.pulsar.client.admin.Brokers;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.common.conf.InternalConfigurationData;
import org.apache.pulsar.common.policies.data.ErrorData;
import org.apache.pulsar.common.policies.data.NamespaceOwnershipStatus;
import org.apache.pulsar.common.util.Codec;

public class BrokersImpl extends BaseResource implements Brokers {
    private final WebTarget adminBrokers;

    public BrokersImpl(WebTarget web, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminBrokers = web.path("/admin/v2/brokers");
    }

    @Override
    public List<String> getActiveBrokers(String cluster) throws PulsarAdminException {
        try {
            return request(adminBrokers.path(cluster)).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, NamespaceOwnershipStatus> getOwnedNamespaces(String cluster, String brokerUrl)
            throws PulsarAdminException {
        try {
            return request(adminBrokers.path(cluster).path(brokerUrl).path("ownedNamespaces")).get(
                    new GenericType<Map<String, NamespaceOwnershipStatus>>() {
                    });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void updateDynamicConfiguration(String configName, String configValue) throws PulsarAdminException {
        try {
            String value = Codec.encode(configValue);
            request(adminBrokers.path("/configuration/").path(configName).path(value)).post(Entity.json(""),
                    ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void deleteDynamicConfiguration(String configName) throws PulsarAdminException {
        try {
            request(adminBrokers.path("/configuration/").path(configName)).delete(ErrorData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
    
    @Override
    public Map<String, String> getAllDynamicConfigurations() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/configuration/").path("values")).get(new GenericType<Map<String, String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public List<String> getDynamicConfigurationNames() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/configuration")).get(new GenericType<List<String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public Map<String, String> getRuntimeConfigurations() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/configuration").path("runtime")).get(new GenericType<Map<String, String>>() {
            });
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public InternalConfigurationData getInternalConfigurationData() throws PulsarAdminException {
        try {
            return request(adminBrokers.path("/internal-configuration")).get(InternalConfigurationData.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public void healthcheck() throws PulsarAdminException {
        try {
            String result = request(adminBrokers.path("/health")).get(String.class);
            if (!result.trim().toLowerCase().equals("ok")) {
                throw new PulsarAdminException("Healthcheck returned unexpected result: " + result);
            }
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
