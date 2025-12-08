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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.impl.AuthenticationUtil;
import org.apache.pulsar.client.impl.auth.oauth2.protocol.DefaultMetadataResolver;

/**
 * Pulsar client authentication provider based on OAuth 2.0 using RFC 8414 standard metadata path.
 * This class is identical to {@link AuthenticationOAuth2} but it always uses the standard
 * "/.well-known/oauth-authorization-server" metadata path as defined in RFC 8414.
 */
public class AuthenticationOAuth2StandardAuthzServer extends AuthenticationOAuth2 {

    private static final long serialVersionUID = 1L;

    public AuthenticationOAuth2StandardAuthzServer() {
        super();
    }

    AuthenticationOAuth2StandardAuthzServer(Flow flow, Clock clock) {
        super(flow, clock);
    }

    @Override
    public void configure(String encodedAuthParamString) {
        if (StringUtils.isBlank(encodedAuthParamString)) {
            throw new IllegalArgumentException("No authentication parameters were provided");
        }
        Map<String, String> params;
        try {
            params = AuthenticationUtil.configureFromJsonString(encodedAuthParamString);
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed authentication parameters", e);
        }

        // Always set the OAuth 2.0 standard metadata path
        params.put(FlowBase.CONFIG_PARAM_WELL_KNOWN_METADATA_PATH,
                DefaultMetadataResolver.getOAuthWellKnownMetadataPath());

        String type = params.getOrDefault(CONFIG_PARAM_TYPE, TYPE_CLIENT_CREDENTIALS);
        switch(type) {
            case TYPE_CLIENT_CREDENTIALS:
                this.flow = ClientCredentialsFlow.fromParameters(params);
                break;
            default:
                throw new IllegalArgumentException("Unsupported authentication type: " + type);
        }
    }
}