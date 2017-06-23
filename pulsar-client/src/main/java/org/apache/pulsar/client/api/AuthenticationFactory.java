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

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;

import java.util.HashMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public final class AuthenticationFactory {

    /**
     * Create an instance of the Authentication-Plugin
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParamsString
     *            string which represents parameters for the Authentication-Plugin, e.g., "key1:val1,key2:val2"
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
    public static final Authentication create(String authPluginClassName, String authParamsString)
            throws UnsupportedAuthenticationException {
        Map<String, String> authParams = new HashMap<String, String>();
        if (isNotBlank(authParamsString)) {
            String[] params = authParamsString.split(",");
            for (String p : params) {
                String[] kv = p.split(":");
                if (kv.length == 2) {
                    authParams.put(kv[0], kv[1]);
                }
            }
        }
        return AuthenticationFactory.create(authPluginClassName, authParams);
    }

    /**
     * Create an instance of the Authentication-Plugin
     *
     * @param authPluginClassName
     *            name of the Authentication-Plugin you want to use
     * @param authParams
     *            map which represents parameters for the Authentication-Plugin
     * @return instance of the Authentication-Plugin
     * @throws UnsupportedAuthenticationException
     */
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
