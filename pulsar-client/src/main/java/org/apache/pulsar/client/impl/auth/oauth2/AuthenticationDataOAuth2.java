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
package org.apache.pulsar.client.impl.auth.oauth2;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

/**
 * Provide OAuth 2.0 authentication data.
 */
class AuthenticationDataOAuth2 implements AuthenticationDataProvider {
    public static final String HTTP_HEADER_NAME = "Authorization";

    private final String accessToken;
    private final Set<Map.Entry<String, String>> headers;

    public AuthenticationDataOAuth2(String accessToken) {
        this.accessToken = accessToken;
        this.headers = Collections.singletonMap(HTTP_HEADER_NAME, "Bearer " + accessToken).entrySet();
    }

    @Override
    public boolean hasDataForHttp() {
        return true;
    }

    @Override
    public Set<Map.Entry<String, String>> getHttpHeaders() {
        return this.headers;
    }

    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    @Override
    public String getCommandData() {
        return this.accessToken;
    }

}
