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

import javax.ws.rs.client.WebTarget;

import org.json.JSONArray;
import org.json.JSONObject;

import com.yahoo.pulsar.client.admin.BrokerStats;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.Authentication;
import com.yahoo.pulsar.common.stats.AllocatorStats;

/**
 * Pulsar Admin API client.
 *
 *
 */
public class BrokerStatsImpl extends BaseResource implements BrokerStats {

    private final WebTarget brokerStats;

    public BrokerStatsImpl(WebTarget target, Authentication auth) {
        super(auth);
        brokerStats = target.path("/broker-stats");
    }
    
    @Override
    public JSONArray getMetrics() throws PulsarAdminException {
        try {
            String json = request(brokerStats.path("/metrics")).get(String.class);
            return new JSONArray(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public AllocatorStats getAllocatorStats(String allocatorName) throws PulsarAdminException {
        try {
            return request(brokerStats.path("/allocator-stats").path(allocatorName)).get(AllocatorStats.class);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public JSONArray getMBeans() throws PulsarAdminException {
        try {
            String json = request(brokerStats.path("/mbeans")).get(String.class);
            return new JSONArray(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public JSONObject getDestinations() throws PulsarAdminException {
        try {
            String json = request(brokerStats.path("/destinations")).get(String.class);
            return new JSONObject(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public JSONObject getLoadReport() throws PulsarAdminException {
        try {
            String json = request(brokerStats.path("/load-report")).get(String.class);
            return new JSONObject(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    @Override
    public JSONObject getPendingBookieOpsStats() throws PulsarAdminException {
        try {
            String json = request(brokerStats.path("/bookieops")).get(String.class);
            return new JSONObject(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }

    public JSONObject getBrokerResourceAvailability(String property, String cluster, String namespace)
            throws PulsarAdminException {
        try {
            String json = request(
                    brokerStats.path("/broker-resource-availability").path(property).path(cluster).path(namespace))
                            .get(String.class);
            return new JSONObject(json);
        } catch (Exception e) {
            throw getApiException(e);
        }
    }
}
