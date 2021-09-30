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

import javax.ws.rs.client.WebTarget;
import org.apache.pulsar.client.admin.ProxyStats;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;

/**
 * Pulsar Admin API client.
 *
 *
 */
public class ProxyStatsImpl extends BaseResource implements ProxyStats {

    private final WebTarget adminProxyStats;

    public ProxyStatsImpl(WebTarget target, Authentication auth, long readTimeoutMs) {
        super(auth, readTimeoutMs);
        adminProxyStats = target.path("/proxy-stats");
    }

    @Override
    public String getConnections() throws PulsarAdminException {
        try {
            String json = request(adminProxyStats.path("/connections")).get(String.class);
            return json;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public String getTopics() throws PulsarAdminException {
        try {
            String json = request(adminProxyStats.path("/topics")).get(String.class);
            return json;
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
