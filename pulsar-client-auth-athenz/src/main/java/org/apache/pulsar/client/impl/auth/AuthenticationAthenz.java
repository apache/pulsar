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
package org.apache.pulsar.client.impl.auth;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException;

import java.security.PrivateKey;

import com.yahoo.athenz.zts.RoleToken;
import com.yahoo.athenz.zts.ZTSClient;
import com.yahoo.athenz.auth.ServiceIdentityProvider;
import com.yahoo.athenz.auth.impl.SimpleServiceIdentityProvider;
import com.yahoo.athenz.auth.util.Crypto;

public class AuthenticationAthenz implements Authentication {

    private transient ZTSClient ztsClient = null;
    private String tenantDomain;
    private String tenantService;
    private String providerDomain;
    private String privateKeyPath;
    private String keyId = "0";
    private long cachedRoleTokenTimestamp;
    private String roleToken;
    private final int minValidity = 2 * 60 * 60; // athenz will only give this token if it's at least valid for 2hrs
    private final int maxValidity = 24 * 60 * 60; // token has upto 24 hours validity
    private final int cacheDurationInHour = 1; // we will cache role token for an hour then ask athenz lib again

    public AuthenticationAthenz() {
    }

    @Override
    public String getAuthMethodName() {
        return "athenz";
    }

    @Override
    synchronized public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        if (cachedRoleTokenIsValid()) {
            return new AuthenticationDataAthenz(roleToken, getZtsClient().getHeader());
        }
        try {
            // the following would set up the API call that requests tokens from the server
            // that can only be used if they are 10 minutes from expiration and last twenty four hours
            RoleToken token = getZtsClient().getRoleToken(providerDomain, null, minValidity, maxValidity, false);
            roleToken = token.getToken();
            cachedRoleTokenTimestamp = System.nanoTime();
            return new AuthenticationDataAthenz(roleToken, getZtsClient().getHeader());
        } catch (Throwable t) {
            throw new GettingAuthenticationDataException(t);
        }
    }

    private boolean cachedRoleTokenIsValid() {
        if (roleToken == null) {
            return false;
        }
        // Ensure we refresh the Athenz role token every hour to avoid using an expired role token
        return (System.nanoTime() - cachedRoleTokenTimestamp) < TimeUnit.HOURS.toNanos(cacheDurationInHour);
    }

    @Override
    public void configure(Map<String, String> authParams) {
        this.tenantDomain = authParams.get("tenantDomain");
        this.tenantService = authParams.get("tenantService");
        this.providerDomain = authParams.get("providerDomain");
        this.privateKeyPath = authParams.get("privateKeyPath");
        this.keyId = authParams.getOrDefault("keyId", "0");
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
    }

    ZTSClient getZtsClient() {
        if (ztsClient == null) {
            PrivateKey privateKey = Crypto.loadPrivateKey(new File(privateKeyPath));
            ServiceIdentityProvider siaProvider = new SimpleServiceIdentityProvider(tenantDomain, tenantService,
                    privateKey, keyId);
            ztsClient = new ZTSClient(null, tenantDomain, tenantService, siaProvider);
        }
        return ztsClient;
    }
}
