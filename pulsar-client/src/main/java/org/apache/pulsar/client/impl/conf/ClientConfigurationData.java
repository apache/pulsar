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
package org.apache.pulsar.client.impl.conf;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.Sets;
import java.time.Clock;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.ServiceUrlProvider;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.util.Secret;

/**
 * This is a simple holder of the client configuration values.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClientConfigurationData implements Serializable, Cloneable {
    private static final long serialVersionUID = 1L;

    private String serviceUrl;
    @JsonIgnore
    private transient ServiceUrlProvider serviceUrlProvider;

    @JsonIgnore
    private Authentication authentication;
    private String authPluginClassName;

    @Secret
    private String authParams;
    @Secret
    private Map<String, String> authParamMap;

    private long operationTimeoutMs = 30000;
    private long statsIntervalSeconds = 60;

    private int numIoThreads = 1;
    private int numListenerThreads = 1;
    private int connectionsPerBroker = 1;

    private boolean useTcpNoDelay = true;

    private boolean useTls = false;
    private String tlsTrustCertsFilePath = "";
    private boolean tlsAllowInsecureConnection = false;
    private boolean tlsHostnameVerificationEnable = false;
    private int concurrentLookupRequest = 5000;
    private int maxLookupRequest = 50000;
    private int maxLookupRedirects = 20;
    private int maxNumberOfRejectedRequestPerConnection = 50;
    private int keepAliveIntervalSeconds = 30;
    private int connectionTimeoutMs = 10000;
    private int requestTimeoutMs = 60000;
    private long initialBackoffIntervalNanos = TimeUnit.MILLISECONDS.toNanos(100);
    private long maxBackoffIntervalNanos = TimeUnit.SECONDS.toNanos(60);
    private boolean enableBusyWait = false;
    //
    private String listenerName;

    // set TLS using KeyStore way.
    private boolean useKeyStoreTls = false;
    private String sslProvider = null;
    // needed when client auth is required
    private String tlsTrustStoreType = "JKS";
    private String tlsTrustStorePath = null;
    private String tlsTrustStorePassword = null;
    private Set<String> tlsCiphers = Sets.newTreeSet();
    private Set<String> tlsProtocols = Sets.newTreeSet();

    private long memoryLimitBytes = 0;

    /** proxyServiceUrl and proxyProtocol must be mutually inclusive **/
    private String proxyServiceUrl;
    private ProxyProtocol proxyProtocol;

    // transaction
    private boolean enableTransaction = false;

    @JsonIgnore
    private Clock clock = Clock.systemDefaultZone();

    public Authentication getAuthentication() {
        if (authentication == null) {
            this.authentication = AuthenticationDisabled.INSTANCE;
        }
        return authentication;
    }

    public void setAuthentication(Authentication authentication) {
        this.authentication = authentication;
    }
    public boolean isUseTls() {
        if (useTls)
            return true;
        if (getServiceUrl() != null && (this.getServiceUrl().startsWith("pulsar+ssl") || this.getServiceUrl().startsWith("https"))) {
            this.useTls = true;
            return true;
        }
        return false;
    }

    public ClientConfigurationData clone() {
        try {
            return (ClientConfigurationData) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Failed to clone ClientConfigurationData");
        }
    }


}
