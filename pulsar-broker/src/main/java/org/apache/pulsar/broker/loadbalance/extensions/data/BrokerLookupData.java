/*
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
package org.apache.pulsar.broker.loadbalance.extensions.data;

import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

/**
 * Defines the information required to broker lookup.
 */
public record BrokerLookupData (String webServiceUrl,
                                String webServiceUrlTls,
                                String pulsarServiceUrl,
                                String pulsarServiceUrlTls,
                                Map<String, AdvertisedListener> advertisedListeners,
                                Map<String, String> protocols,
                                boolean persistentTopicsEnabled,
                                boolean nonPersistentTopicsEnabled,
                                String brokerVersion) implements ServiceLookupData {
    @Override
    public String getWebServiceUrl() {
        return this.webServiceUrl();
    }

    @Override
    public String getWebServiceUrlTls() {
        return this.webServiceUrlTls();
    }

    @Override
    public String getPulsarServiceUrl() {
        return this.pulsarServiceUrl();
    }

    @Override
    public String getPulsarServiceUrlTls() {
        return this.pulsarServiceUrlTls();
    }

    @Override
    public Map<String, String> getProtocols() {
        return this.protocols();
    }

    @Override
    public Optional<String> getProtocol(String protocol) {
        return Optional.ofNullable(this.protocols().get(protocol));
    }

    public LookupResult toLookupResult() {
        return new LookupResult(webServiceUrl, webServiceUrlTls, pulsarServiceUrl, pulsarServiceUrlTls,
                LookupResult.Type.BrokerUrl, false);
    }
}
