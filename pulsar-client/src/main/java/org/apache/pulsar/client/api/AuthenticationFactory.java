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
package org.apache.pulsar.client.api;

import java.util.Map;
import java.util.function.Supplier;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;

public final class AuthenticationFactory {

    /**
     * Create an authentication provider for token based authentication.
     *
     * @param token
     *            the client auth token
     */
    public static Authentication token(String token) {
        return new AuthenticationToken(token);
    }

    /**
     * Create an authentication provider for token based authentication.
     *
     * @param tokenSupplier
     *            a supplier of the client auth token
     */
    public static Authentication token(Supplier<String> tokenSupplier) {
        return new AuthenticationToken(tokenSupplier);
    }

    /**
     * Create an authentication provider for TLS based authentication.
     *
     * @param certFilePath
     *            the path to the TLS client public key
     * @param keyFilePath
     *            the path to the TLS client private key
     */
    public static Authentication TLS(String certFilePath, String keyFilePath) {
        return new AuthenticationTls(certFilePath, keyFilePath);
    }

    /**
     * Create an instance of the Authentication-Plugin
     *
     * @param authPluginClassName name of the Authentication-Plugin you want to use
     * @param authParamsString    string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
    @SuppressWarnings("deprecation")
    public static final Authentication create(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        try {
            if (isNotBlank(authPluginClassName)) {
                Class<?> authClass = Class.forName(authPluginClassName);
                Authentication auth = (Authentication) authClass.newInstance();
                if (auth instanceof EncodedAuthenticationParameterSupport) {
                    // Parse parameters on plugin side.
                    ((EncodedAuthenticationParameterSupport) auth).configure(authParamsString);
                } else {
                    // Parse parameters by default parse logic.
                    auth.configure(AuthenticationUtil.configureFromPulsar1AuthParamString(authParamsString));
                }
                return auth;
            } else {
                return new AuthenticationDisabled();
            }
        } catch (Throwable t) {
            throw new UnsupportedAuthenticationException(t);
        }
    }

    /**
     * Create an instance of the Authentication-Plugin
     *
     * @param authPluginClassName name of the Authentication-Plugin you want to use
     * @param authParams          map which represents parameters for the Authentication-Plugin
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
    @SuppressWarnings("deprecation")
    public static final Authentication create(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        try {
            if (isNotBlank(authPluginClassName)) {
                Class<?> authClass = Class.forName(authPluginClassName);
                Authentication auth = (Authentication) authClass.newInstance();
                auth.configure(authParams);
                return auth;
            } else {
                return new AuthenticationDisabled();
            }
        } catch (Throwable t) {
            throw new UnsupportedAuthenticationException(t);
        }
    }
}
