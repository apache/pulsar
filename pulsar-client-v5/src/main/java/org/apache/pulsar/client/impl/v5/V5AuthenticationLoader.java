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
package org.apache.pulsar.client.impl.v5;

import java.io.IOException;
import java.util.Map;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.v5.auth.Authentication;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.v5.auth.LegacyV4AuthenticationAdapter;

/**
 * Loads an {@code authPluginClassName} into a v5 {@link Authentication}, detecting the v5-native case
 * (PIP-478 In-Scope #2). This is the string-config counterpart to programmatically building a plugin: a
 * class that implements only the v5 {@link Authentication} SPI is instantiated no-arg, configured with the
 * parsed {@code authParams}, and returned directly; anything else is a legacy v4 plugin, built through the
 * v4 {@link AuthenticationUtil} and bridged with {@link LegacyV4AuthenticationAdapter#wrap}.
 *
 * <p>Before this, every reflective load blind-cast the loaded class to the v4
 * {@code org.apache.pulsar.client.api.Authentication}, so a v5-native plugin deployed by class name threw
 * {@link ClassCastException} — the flagship "implement a v5 plugin, deploy it by class name" story worked
 * only when the instance was passed programmatically.
 */
final class V5AuthenticationLoader {

    private V5AuthenticationLoader() {
    }

    /**
     * Build a v5 {@link Authentication} from a plugin class name and a parameter string. A v5-native class
     * is instantiated and configured with the parsed params; a legacy v4 class keeps the existing v4 path
     * (including the raw-string handling that {@code EncodedAuthenticationParameterSupport} plugins rely on).
     *
     * @param authPluginClassName the plugin class name (blank/unknown routes to the v4 path)
     * @param authParamsString    the parameters as a JSON object or {@code key:val,key:val} string
     * @return the configured v5 authentication
     * @throws PulsarClientException if the plugin cannot be loaded, instantiated, or configured
     */
    static Authentication create(String authPluginClassName, String authParamsString)
            throws PulsarClientException {
        Class<?> v5Class = v5NativeClassOrNull(authPluginClassName);
        if (v5Class != null) {
            return newConfiguredV5(v5Class, parseParams(authParamsString));
        }
        return LegacyV4AuthenticationAdapter.wrap(AuthenticationUtil.create(authPluginClassName, authParamsString));
    }

    /**
     * Build a v5 {@link Authentication} from a plugin class name and a parameter map. A v5-native class is
     * instantiated and configured with the map; a legacy v4 class keeps the existing v4 path.
     *
     * @param authPluginClassName the plugin class name (blank/unknown routes to the v4 path)
     * @param authParams          the parameters as key-value pairs
     * @return the configured v5 authentication
     * @throws PulsarClientException if the plugin cannot be loaded, instantiated, or configured
     */
    static Authentication create(String authPluginClassName, Map<String, String> authParams)
            throws PulsarClientException {
        Class<?> v5Class = v5NativeClassOrNull(authPluginClassName);
        if (v5Class != null) {
            return newConfiguredV5(v5Class, authParams == null ? Map.of() : authParams);
        }
        return LegacyV4AuthenticationAdapter.wrap(AuthenticationUtil.create(authPluginClassName, authParams));
    }

    /**
     * The class named by {@code authPluginClassName} when it is a v5-native {@link Authentication}, else
     * {@code null}. A blank name, a class that cannot be loaded, or a legacy v4 class all return
     * {@code null} so the caller falls through to the v4 path — which surfaces the identical class-not-found
     * failure a v4 deployment would, keeping the legacy behaviour byte-for-byte.
     */
    private static Class<?> v5NativeClassOrNull(String authPluginClassName) {
        if (authPluginClassName == null || authPluginClassName.isBlank()) {
            return null;
        }
        Class<?> authClass;
        try {
            authClass = Class.forName(authPluginClassName);
        } catch (ClassNotFoundException e) {
            return null;
        }
        return Authentication.class.isAssignableFrom(authClass) ? authClass : null;
    }

    private static Authentication newConfiguredV5(Class<?> v5Class, Map<String, String> params)
            throws PulsarClientException {
        try {
            Authentication v5 = (Authentication) v5Class.getDeclaredConstructor().newInstance();
            v5.configure(params);
            return v5;
        } catch (Throwable t) {
            throw new PulsarClientException.UnsupportedAuthenticationException(t);
        }
    }

    /**
     * Parse an {@code authParams} string into a map, accepting either a JSON object or the legacy
     * {@code key:val,key:val} form (matching {@link AuthenticationUtil}).
     *
     * @param authParamsString the parameters string (may be {@code null}/blank, yielding an empty map)
     * @return the parsed parameters
     * @throws PulsarClientException if a JSON payload cannot be parsed
     */
    static Map<String, String> parseParams(String authParamsString) throws PulsarClientException {
        if (authParamsString == null || authParamsString.isBlank()) {
            return Map.of();
        }
        try {
            if (authParamsString.trim().startsWith("{")) {
                return AuthenticationUtil.configureFromJsonString(authParamsString);
            }
            return AuthenticationUtil.configureFromPulsar1AuthParamString(authParamsString);
        } catch (IOException e) {
            throw new PulsarClientException.UnsupportedAuthenticationException(e);
        }
    }
}
