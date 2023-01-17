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
package org.apache.pulsar.client.impl;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.EncodedAuthenticationParameterSupport;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.common.util.ObjectMapperFactory;

public class AuthenticationUtil {
    private static final ObjectReader HASHMAP_READER =
            ObjectMapperFactory.getMapper().reader().forType(new TypeReference<HashMap<String, String>>() {
            });

    public static Map<String, String> configureFromJsonString(String authParamsString) throws IOException {
        return HASHMAP_READER.readValue(authParamsString);
    }

    public static Map<String, String> configureFromPulsar1AuthParamString(String authParamsString) {
        Map<String, String> authParams = new HashMap<>();

        if (isNotBlank(authParamsString)) {
            String[] params = authParamsString.split(",");
            for (String p : params) {
                // The value could be a file path, which could contain a colon like "C:\\path\\to\\file" on Windows.
                int index = p.indexOf(':');
                if (index < 0) {
                    continue;
                }
                String key = p.substring(0, index);
                if (!key.isEmpty()) {
                    authParams.put(key, p.substring(index + 1));
                }
            }
        }
        return authParams;
    }

    /**
     * Create an instance of the Authentication-Plugin.
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
    @SuppressWarnings("deprecation")
    public static final Authentication create(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        try {
            if (isNotBlank(authPluginClassName)) {
                Class<?> authClass = Class.forName(authPluginClassName);
                Authentication auth = (Authentication) authClass.getDeclaredConstructor().newInstance();
                if (auth instanceof EncodedAuthenticationParameterSupport) {
                    // Parse parameters on plugin side.
                    ((EncodedAuthenticationParameterSupport) auth).configure(authParamsString);
                } else {
                    // Parse parameters by default parse logic.
                    auth.configure(configureFromPulsar1AuthParamString(authParamsString));
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
     * Create an instance of the Authentication-Plugin.
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
    @SuppressWarnings("deprecation")
    public static final Authentication create(String authPluginClassName, Map<String, String> authParams)
            throws UnsupportedAuthenticationException {
        try {
            if (isNotBlank(authPluginClassName)) {
                Class<?> authClass = Class.forName(authPluginClassName);
                Authentication auth = (Authentication) authClass.getDeclaredConstructor().newInstance();
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
