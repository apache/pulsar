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

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;

/**
 *
 * This plugin requires these parameters
 *
 * tlsCertFile: A file path for a client certificate. tlsKeyFile: A file path for a client private key.
 *
 */
public class AuthenticationTls implements Authentication, EncodedAuthenticationParameterSupport {
    private static final String AUTH_NAME = "tls";
    private static final long serialVersionUID = 1L;

    private String certFilePath;
    private String keyFilePath;
    @SuppressFBWarnings(value = "SE_BAD_FIELD", justification = "Using custom serializer which Findbugs can't detect")
    private Supplier<ByteArrayInputStream> certStreamProvider, keyStreamProvider, trustStoreStreamProvider;

    public AuthenticationTls() {
    }

    public AuthenticationTls(String certFilePath, String keyFilePath) {
        this.certFilePath = certFilePath;
        this.keyFilePath = keyFilePath;
    }

    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider) {
        this(certStreamProvider, keyStreamProvider, null);
    }

    public AuthenticationTls(Supplier<ByteArrayInputStream> certStreamProvider,
            Supplier<ByteArrayInputStream> keyStreamProvider, Supplier<ByteArrayInputStream> trustStoreStreamProvider) {
        this.certStreamProvider = certStreamProvider;
        this.keyStreamProvider = keyStreamProvider;
        this.trustStoreStreamProvider = trustStoreStreamProvider;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public String getAuthMethodName() {
        return AUTH_NAME;
    }

    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {
        try {
            if (certFilePath != null && keyFilePath != null) {
                return new AuthenticationDataTls(certFilePath, keyFilePath);
            } else if (certStreamProvider != null && keyStreamProvider != null) {
                return new AuthenticationDataTls(certStreamProvider, keyStreamProvider, trustStoreStreamProvider);
            }
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
        throw new IllegalArgumentException("cert/key file path or cert/key stream must be present");
    }

    @Override
    public void configure(String encodedAuthParamString) {
        Map<String, String> authParamsMap = null;
        try {
            authParamsMap = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (Exception e) {
            // auth-param is not in json format
        }
        authParamsMap = (authParamsMap == null || authParamsMap.isEmpty())
                ? AuthenticationUtil.configureFromPulsar1AuthParamString(encodedAuthParamString)
                : authParamsMap;
        setAuthParams(authParamsMap);
    }

    @Override
    @Deprecated
    public void configure(Map<String, String> authParams) {
        setAuthParams(authParams);
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

    private void setAuthParams(Map<String, String> authParams) {
        certFilePath = authParams.get("tlsCertFile");
        keyFilePath = authParams.get("tlsKeyFile");
    }

    @VisibleForTesting
    public String getCertFilePath() {
        return certFilePath;
    }

    @VisibleForTesting
    public String getKeyFilePath() {
        return keyFilePath;
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getCertStreamProvider() {
        return certStreamProvider;
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getKeyStreamProvider() {
        return keyStreamProvider;
    }

    @VisibleForTesting
    Supplier<ByteArrayInputStream> getTrustStoreStreamProvider() {
        return trustStoreStreamProvider;
    }
}
