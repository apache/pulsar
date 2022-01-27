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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.KeyStoreParams;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.AuthenticationUtil;

/**
 * This plugin requires these parameters: keyStoreType, keyStorePath, and  keyStorePassword.
 * This parameter will construct a AuthenticationDataProvider
 */
@Slf4j
public class AuthenticationKeyStoreTls implements Authentication, EncodedAuthenticationParameterSupport {
    private static final long serialVersionUID = 1L;

    private static final String AUTH_NAME = "tls";

    // parameter name
    public static final String KEYSTORE_TYPE = "keyStoreType";
    public static final String KEYSTORE_PATH = "keyStorePath";
    public static final String KEYSTORE_PW = "keyStorePassword";
    private static final String DEFAULT_KEYSTORE_TYPE = "JKS";

    private transient KeyStoreParams keyStoreParams;

    public AuthenticationKeyStoreTls() {
    }

    public AuthenticationKeyStoreTls(String keyStoreType, String keyStorePath, String keyStorePassword) {
        this.keyStoreParams = KeyStoreParams.builder()
                .keyStoreType(keyStoreType)
                .keyStorePath(keyStorePath)
                .keyStorePassword(keyStorePassword)
                .build();
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
            return new AuthenticationDataKeyStoreTls(this.keyStoreParams);
        } catch (Exception e) {
            throw new PulsarClientException(e);
        }
    }

    // passed in KEYSTORE_TYPE/KEYSTORE_PATH/KEYSTORE_PW to construct parameters.
    // e.g. {"keyStoreType":"JKS","keyStorePath":"/path/to/keystorefile","keyStorePassword":"keystorepw"}
    //  or: "keyStoreType":"JKS","keyStorePath":"/path/to/keystorefile","keyStorePassword":"keystorepw"
    @Override
    public void configure(String paramsString) {
        Map<String, String> params = null;
        try {
            params = AuthenticationUtil.configureFromJsonString(paramsString);
        } catch (Exception e) {
            // auth-param is not in json format
            log.info("parameter not in Json format: {}", paramsString);
        }

        // in ":" "," format.
        params = (params == null || params.isEmpty())
                ? AuthenticationUtil.configureFromPulsar1AuthParamString(paramsString)
                : params;

        configure(params);
    }

    @Override
    public void configure(Map<String, String> params) {
        String keyStoreType = params.get(KEYSTORE_TYPE);
        String keyStorePath = params.get(KEYSTORE_PATH);
        String keyStorePassword = params.get(KEYSTORE_PW);

        if (Strings.isNullOrEmpty(keyStorePath)
            || Strings.isNullOrEmpty(keyStorePassword)) {
            throw new IllegalArgumentException("Passed in parameter empty. "
                                               + KEYSTORE_PATH + ": " + keyStorePath
                                               + " " + KEYSTORE_PW + ": " + keyStorePassword);
        }

        if (Strings.isNullOrEmpty(keyStoreType)) {
            keyStoreType = DEFAULT_KEYSTORE_TYPE;
        }

        this.keyStoreParams = KeyStoreParams.builder()
                .keyStoreType(keyStoreType)
                .keyStorePath(keyStorePath)
                .keyStorePassword(keyStorePassword)
                .build();
    }

    @Override
    public void start() throws PulsarClientException {
        // noop
    }

    // return strings like : "key1":"value1", "key2":"value2", ...
    public static String mapToString(Map<String, String> map) {
        return Joiner.on(',').withKeyValueSeparator(':').join(map);
    }
}
