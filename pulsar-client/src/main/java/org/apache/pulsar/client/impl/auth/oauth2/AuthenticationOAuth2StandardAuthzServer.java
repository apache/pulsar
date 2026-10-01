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

import java.time.Clock;
import java.util.Map;
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
    protected Map<String, String> parseAuthParameters(String encodedAuthParamString) {
        Map<String, String> params = super.parseAuthParameters(encodedAuthParamString);
        // Always set the OAuth 2.0 standard metadata path
        params.put(FlowBase.CONFIG_PARAM_WELL_KNOWN_METADATA_PATH,
                DefaultMetadataResolver.OAUTH_WELL_KNOWN_METADATA_PATH);
        return params;
    }
}
