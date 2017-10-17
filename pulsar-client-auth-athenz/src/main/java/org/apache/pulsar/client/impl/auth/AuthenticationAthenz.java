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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException;
import org.apache.pulsar.common.util.ObjectMapperFactory;

import java.security.PrivateKey;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;

import com.yahoo.athenz.zts.RoleToken;
import com.yahoo.athenz.zts.ZTSClient;
import com.yahoo.athenz.auth.ServiceIdentityProvider;
import com.yahoo.athenz.auth.impl.SimpleServiceIdentityProvider;
import com.yahoo.athenz.auth.util.Crypto;

public class AuthenticationAthenz implements Authentication, EncodedAuthenticationParameterSupport {

    private transient ZTSClient ztsClient = null;
    private String tenantDomain;
    private String tenantService;
    private String providerDomain;
    private PrivateKey privateKey;
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
    public void configure(String encodedAuthParamString) {

        if (isBlank(encodedAuthParamString)) {
            throw new IllegalArgumentException("authParams must not be empty");
        }

        // Convert JSON to Map
        try {
            ObjectMapper jsonMapper = ObjectMapperFactory.create();
            Map<String, String> authParamsMap = jsonMapper.readValue(encodedAuthParamString, new TypeReference<HashMap<String, String>>() {});
            setAuthParams(authParamsMap);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse authParams");
        }
    }

    @Override
    public void configure(Map<String, String> authParams) {
        setAuthParams(authParams);
    }

    private void setAuthParams(Map<String, String> authParams){
        this.tenantDomain = authParams.get("tenantDomain");
        this.tenantService = authParams.get("tenantService");
        this.providerDomain = authParams.get("providerDomain");
        // privateKeyPath is deprecated, this is for compatibility
        if (isBlank(authParams.get("privateKey")) && isNotBlank(authParams.get("privateKeyPath"))) {
            this.privateKey = loadPrivateKey(authParams.get("privateKeyPath"));
        } else {
            this.privateKey = loadPrivateKey(authParams.get("privateKey"));
        }
        
        if(this.privateKey == null) {
            throw new IllegalArgumentException("Failed to load private key from privateKey or privateKeyPath field");
        }
        
        this.keyId = authParams.getOrDefault("keyId", "0");
        if (authParams.containsKey("athenzConfPath")) {
            System.setProperty("athenz.athenz_conf", authParams.get("athenzConfPath"));
        }
        if (authParams.containsKey("principalHeader")) {
            System.setProperty("athenz.auth.principal.header", authParams.get("principalHeader"));
        }
        if (authParams.containsKey("roleHeader")) {
            System.setProperty("athenz.auth.role.header", authParams.get("roleHeader"));
        }
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
    }

    private ZTSClient getZtsClient() {
        if (ztsClient == null) {
            ServiceIdentityProvider siaProvider = new SimpleServiceIdentityProvider(tenantDomain, tenantService,
                    privateKey, keyId);
            ztsClient = new ZTSClient(null, tenantDomain, tenantService, siaProvider);
        }
        return ztsClient;
    }

    private PrivateKey loadPrivateKey(String privateKeyURL) {
        PrivateKey privateKey = null;
        try {
            URI uri = new URI(privateKeyURL);
            if (isBlank(uri.getScheme())) {
                // We treated as file path
                privateKey = Crypto.loadPrivateKey(new File(privateKeyURL));
            } else if (uri.getScheme().equals("file")) {
                privateKey = Crypto.loadPrivateKey(new File(uri.getPath()));
            } else if(uri.getScheme().equals("data")) {
                List<String> dataParts = Splitter.on(",").splitToList(uri.getSchemeSpecificPart());
                if (dataParts.get(0).equals("application/x-pem-file;base64")) {
                    privateKey = Crypto.loadPrivateKey(new String(Base64.getDecoder().decode(dataParts.get(1))));
                } else {
                    throw new IllegalArgumentException("Unsupported media type or encoding format: " + dataParts.get(0));
                }
            }
        } catch(URISyntaxException e) {
            throw new IllegalArgumentException("Invalid privateKey format");
        }
        return privateKey;
    }
}
