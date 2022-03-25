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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.io.CharStreams;
import com.yahoo.athenz.auth.ServiceIdentityProvider;
import com.yahoo.athenz.auth.impl.SimpleServiceIdentityProvider;
import com.yahoo.athenz.auth.util.Crypto;
import com.yahoo.athenz.auth.util.CryptoException;
import com.yahoo.athenz.zts.RoleToken;
import com.yahoo.athenz.zts.ZTSClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.PrivateKey;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.GettingAuthenticationDataException;
import org.apache.pulsar.client.api.url.URL;
import org.apache.pulsar.client.impl.AuthenticationUtil;

public class AuthenticationAthenz implements Authentication, EncodedAuthenticationParameterSupport {

    private static final long serialVersionUID = 1L;

    private static final String APPLICATION_X_PEM_FILE = "application/x-pem-file";

    private transient ZTSClient ztsClient = null;
    private String ztsUrl;
    private String tenantDomain;
    private String tenantService;
    private String providerDomain;
    private PrivateKey privateKey;
    private String keyId = "0";
    private String roleHeader = null;
    // If auto prefetching is enabled, application will not complete until the static method
    // ZTSClient.cancelPrefetch() is called.
    // cf. https://github.com/AthenZ/athenz/issues/544
    private boolean autoPrefetchEnabled = false;
    private long cachedRoleTokenTimestamp;
    private String roleToken;
    // athenz will only give this token if it's at least valid for 2hrs
    private static final int minValidity = 2 * 60 * 60;
    private static final int maxValidity = 24 * 60 * 60; // token has upto 24 hours validity
    private static final int cacheDurationInHour = 1; // we will cache role token for an hour then ask athenz lib again

    private final ReadWriteLock cachedRoleTokenLock = new ReentrantReadWriteLock();

    public AuthenticationAthenz() {
    }

    @Override
    public String getAuthMethodName() {
        return "athenz";
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        Lock readLock = cachedRoleTokenLock.readLock();
        readLock.lock();
        try {
            if (cachedRoleTokenIsValid()) {
                return new AuthenticationDataAthenz(roleToken,
                        isNotBlank(roleHeader) ? roleHeader : ZTSClient.getHeader());
            }
        } finally {
            readLock.unlock();
        }

        Lock writeLock = cachedRoleTokenLock.writeLock();
        writeLock.lock();
        try {
            // the following would set up the API call that requests tokens from the server
            // that can only be used if they are 10 minutes from expiration and last twenty
            // four hours
            RoleToken token = getZtsClient().getRoleToken(providerDomain, null, minValidity, maxValidity, false);
            roleToken = token.getToken();
            cachedRoleTokenTimestamp = System.nanoTime();
            return new AuthenticationDataAthenz(roleToken, isNotBlank(roleHeader) ? roleHeader : ZTSClient.getHeader());
        } catch (Throwable t) {
            throw new GettingAuthenticationDataException(t);
        } finally {
            writeLock.unlock();
        }
    }

    private boolean cachedRoleTokenIsValid() {
        if (roleToken == null) {
            return false;
        }
        // Ensure we refresh the Athenz role token every hour to avoid using an expired
        // role token
        return (System.nanoTime() - cachedRoleTokenTimestamp) < TimeUnit.HOURS.toNanos(cacheDurationInHour);
    }

    @Override
    public void configure(String encodedAuthParamString) {

        if (isBlank(encodedAuthParamString)) {
            throw new IllegalArgumentException("authParams must not be empty");
        }

        try {
            setAuthParams(AuthenticationUtil.configureFromJsonString(encodedAuthParamString));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse authParams", e);
        }
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        setAuthParams(authParams);
    }

    private void setAuthParams(Map<String, String> authParams) {
        this.tenantDomain = authParams.get("tenantDomain");
        this.tenantService = authParams.get("tenantService");
        this.providerDomain = authParams.get("providerDomain");
        // privateKeyPath is deprecated, this is for compatibility
        if (isBlank(authParams.get("privateKey")) && isNotBlank(authParams.get("privateKeyPath"))) {
            this.privateKey = loadPrivateKey(authParams.get("privateKeyPath"));
        } else {
            this.privateKey = loadPrivateKey(authParams.get("privateKey"));
        }

        if (this.privateKey == null) {
            throw new IllegalArgumentException("Failed to load private key from privateKey or privateKeyPath field");
        }

        this.keyId = authParams.getOrDefault("keyId", "0");
        this.autoPrefetchEnabled = Boolean.parseBoolean(authParams.getOrDefault("autoPrefetchEnabled", "false"));

        if (isNotBlank(authParams.get("athenzConfPath"))) {
            System.setProperty("athenz.athenz_conf", authParams.get("athenzConfPath"));
        }
        if (isNotBlank(authParams.get("principalHeader"))) {
            System.setProperty("athenz.auth.principal.header", authParams.get("principalHeader"));
        }
        if (isNotBlank(authParams.get("roleHeader"))) {
            this.roleHeader = authParams.get("roleHeader");
            System.setProperty("athenz.auth.role.header", this.roleHeader);
        }
        if (isNotBlank(authParams.get("ztsUrl"))) {
            this.ztsUrl = authParams.get("ztsUrl");
        }
    }

    @Override
    public void start() throws PulsarClientException {
    }

    @Override
    public void close() throws IOException {
        if (ztsClient != null) {
            ztsClient.close();
        }
    }

    private ZTSClient getZtsClient() {
        if (ztsClient == null) {
            ServiceIdentityProvider siaProvider = new SimpleServiceIdentityProvider(tenantDomain, tenantService,
                    privateKey, keyId);
            ztsClient = new ZTSClient(ztsUrl, tenantDomain, tenantService, siaProvider);
            ztsClient.setPrefetchAutoEnable(this.autoPrefetchEnabled);
        }
        return ztsClient;
    }

    private PrivateKey loadPrivateKey(String privateKeyURL) {
        PrivateKey privateKey = null;
        try {
            URLConnection urlConnection = new URL(privateKeyURL).openConnection();
            String protocol = urlConnection.getURL().getProtocol();
            if ("data".equals(protocol) && !APPLICATION_X_PEM_FILE.equals(urlConnection.getContentType())) {
                throw new IllegalArgumentException(
                        "Unsupported media type or encoding format: " + urlConnection.getContentType());
            }
            String keyData = CharStreams.toString(new InputStreamReader((InputStream) urlConnection.getContent(),
                    Charset.defaultCharset()));
            privateKey = Crypto.loadPrivateKey(keyData);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid privateKey format", e);
        } catch (CryptoException | InstantiationException | IllegalAccessException | IOException e) {
            privateKey = null;
        }
        return privateKey;
    }
}
