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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.AuthenticationDataProvider;

public class AuthenticationDataBasic implements AuthenticationDataProvider {
    private static final String HTTP_HEADER_NAME = "Authorization";
    private String httpAuthToken;
    private String commandAuthToken;

    public AuthenticationDataBasic(String userId, String password) {
        httpAuthToken = "Basic " + Base64.getEncoder().encodeToString((userId + ":" + password).getBytes());
        commandAuthToken = userId + ":" + password;
    }

    @Override
    public boolean hasDataForHttp() {
        return true;
    }

    @Override
    public Set<Map.Entry<String, String>> getHttpHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put(HTTP_HEADER_NAME, httpAuthToken);
        return headers.entrySet();
    }

    @Override
    public boolean hasDataFromCommand() {
        return true;
    }

    @Override
    public String getCommandData() {
        return commandAuthToken;
    }
}
