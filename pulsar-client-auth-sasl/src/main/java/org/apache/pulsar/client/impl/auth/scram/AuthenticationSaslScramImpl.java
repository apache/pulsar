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
package org.apache.pulsar.client.impl.auth.scram;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.common.util.DecryptionUtils;

import java.io.IOException;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Authentication provider for SASL SCRAM authentication.
 */
@Slf4j
public class AuthenticationSaslScramImpl implements Authentication, EncodedAuthenticationParameterSupport {
    private static final long serialVersionUID = 1L;

    private static final String APPID_KEY = "saslRole";

    private static final String APP_SECRET_KEY = "roleSecret";

    private static final String SELF_DECRYPT_CLASS = "decryptClass";

    private Map<String, String> configuration;

    public AuthenticationSaslScramImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(String encodedAuthParamString) {
        if (isBlank(encodedAuthParamString)) {
            log.warn("SASL-SCRAM-CLIENT authParams is empty");
        }

        try {
            setAuthParams(AuthenticationUtil.configureFromJsonString(encodedAuthParamString));
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse SASL authParams", e);
        }
    }

    // use passed in parameter to config ange get jaasCredentialsContainer.
    private void setAuthParams(Map<String, String> authParams) {
        this.configuration = authParams;
        if (!configuration.containsKey(APPID_KEY) || !configuration.containsKey(APP_SECRET_KEY)) {
            throw new IllegalArgumentException("Failed to parse SASL authParams credential or secret");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(Map<String, String> authParams) {
        setAuthParams(authParams);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() throws PulsarClientException {
        log.info("SASL-SCRAM-CLIENT start ");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
        log.info("SASL-SCRAM-CLIENT close ");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getAuthMethodName() {
        return "scram";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuthenticationDataProvider getAuthData(String brokerHostName) throws PulsarClientException {
        return this.getAuthData();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuthenticationDataProvider getAuthData() throws PulsarClientException {

        String userName = configuration.get(APPID_KEY);
        String secret = configuration.get(APP_SECRET_KEY);
        String selfDecryptClass = configuration.getOrDefault(SELF_DECRYPT_CLASS, "");

        log.info("SASL-SCRAM-CLIENT NEW getAuthData with user: {} and selfDecryptClass :{}", userName, selfDecryptClass);

        String secretAfterDecrypt = DecryptionUtils.getDecryptionClass(selfDecryptClass).decrypt(secret);
        return new SaslAuthenticationDataProviderScramImpl(userName,
                secretAfterDecrypt);
    }
}
