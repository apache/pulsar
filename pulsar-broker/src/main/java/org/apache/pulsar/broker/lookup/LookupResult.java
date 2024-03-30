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
package org.apache.pulsar.broker.lookup;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.common.lookup.data.LookupData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;

/**
 * Represent a lookup result.
 *
 * Result can either be the lookup data describing the broker that owns the broker or the HTTP endpoint to where we need
 * to redirect the client to try again.
 */
public class LookupResult {
    public enum Type {
        BrokerUrl, RedirectUrl
    }

    private final Type type;
    private final LookupData lookupData;
    private final boolean authoritativeRedirect;

    public LookupResult(NamespaceEphemeralData namespaceEphemeralData) {
        this.type = Type.BrokerUrl;
        this.authoritativeRedirect = false;
        this.lookupData = new LookupData(namespaceEphemeralData.getNativeUrl(),
                namespaceEphemeralData.getNativeUrlTls(), namespaceEphemeralData.getHttpUrl(),
                namespaceEphemeralData.getHttpUrlTls());
    }

    public LookupResult(String httpUrl, String httpUrlTls, String brokerServiceUrl, String brokerServiceUrlTls,
            boolean authoritativeRedirect) {
        this.type = Type.RedirectUrl; // type = redirect => as current broker is
                                      // not owner and prepares LookupResult
                                      // with other broker's urls
        this.authoritativeRedirect = authoritativeRedirect;
        this.lookupData = new LookupData(brokerServiceUrl, brokerServiceUrlTls, httpUrl, httpUrlTls);
    }

    public LookupResult(String httpUrl, String httpUrlTls, String nativeUrl, String nativeUrlTls, Type type,
            boolean authoritativeRedirect) {
        this.type = type;
        this.authoritativeRedirect = authoritativeRedirect;
        this.lookupData = new LookupData(nativeUrl, nativeUrlTls, httpUrl, httpUrlTls);
    }

    public LookupResult(NamespaceEphemeralData namespaceEphemeralData, String nativeUrl, String nativeUrlTls) {
        this.type = Type.BrokerUrl;
        this.authoritativeRedirect = false;
        this.lookupData = new LookupData(nativeUrl, nativeUrlTls, namespaceEphemeralData.getHttpUrl(),
                namespaceEphemeralData.getHttpUrlTls());
    }

    public static LookupResult create(BrokerLookupData selectedBroker, String advertisedListenerName,
                                      boolean authoritativeRedirect) {
        String httpUrl = selectedBroker.getWebServiceUrl();
        String httpUrlTls = selectedBroker.getWebServiceUrlTls();
        String brokerServiceUrl = selectedBroker.getPulsarServiceUrl();
        String brokerServiceUrlTls = selectedBroker.getPulsarServiceUrlTls();

        if (StringUtils.isNotBlank(advertisedListenerName)) {
            var advertisedListener = selectedBroker.advertisedListeners().get(advertisedListenerName);
            if (advertisedListener != null) {
                if (advertisedListener.getBrokerHttpUrl() != null) {
                    httpUrl = advertisedListener.getBrokerHttpUrl().toString();
                }
                if (advertisedListener.getBrokerHttpsUrl() != null) {
                    httpUrlTls = advertisedListener.getBrokerHttpsUrl().toString();
                }
                if (advertisedListener.getBrokerServiceUrl() != null) {
                    brokerServiceUrl = advertisedListener.getBrokerServiceUrl().toString();
                }
                if (advertisedListener.getBrokerServiceUrlTls() != null) {
                    brokerServiceUrlTls = advertisedListener.getBrokerServiceUrlTls().toString();
                }
            }
        }

        return new LookupResult(httpUrl, httpUrlTls, brokerServiceUrl, brokerServiceUrlTls, authoritativeRedirect);
    }

    public static LookupResult create(LocalBrokerData selectedBroker, String advertisedListenerName,
                                      boolean authoritativeRedirect) {
        String httpUrl = selectedBroker.getWebServiceUrl();
        String httpUrlTls = selectedBroker.getWebServiceUrlTls();
        String brokerServiceUrl = selectedBroker.getPulsarServiceUrl();
        String brokerServiceUrlTls = selectedBroker.getPulsarServiceUrlTls();

        if (StringUtils.isNotBlank(advertisedListenerName)) {
            var advertisedListener = selectedBroker.getAdvertisedListeners().get(advertisedListenerName);
            if (advertisedListener != null) {
                if (advertisedListener.getBrokerHttpUrl() != null) {
                    httpUrl = advertisedListener.getBrokerHttpUrl().toString();
                }
                if (advertisedListener.getBrokerHttpsUrl() != null) {
                    httpUrlTls = advertisedListener.getBrokerHttpsUrl().toString();
                }
                if (advertisedListener.getBrokerServiceUrl() != null) {
                    brokerServiceUrl = advertisedListener.getBrokerServiceUrl().toString();
                }
                if (advertisedListener.getBrokerServiceUrlTls() != null) {
                    brokerServiceUrlTls = advertisedListener.getBrokerServiceUrlTls().toString();
                }
            }
        }

        return new LookupResult(httpUrl, httpUrlTls, brokerServiceUrl, brokerServiceUrlTls, authoritativeRedirect);
    }

    public boolean isBrokerUrl() {
        return type == Type.BrokerUrl;
    }

    public boolean isRedirect() {
        return type == Type.RedirectUrl;
    }

    public boolean isAuthoritativeRedirect() {
        return authoritativeRedirect;
    }

    public LookupData getLookupData() {
        return lookupData;
    }

    @Override
    public String toString() {
        return "LookupResult [type=" + type + ", lookupData=" + lookupData + "]";
    }

}
