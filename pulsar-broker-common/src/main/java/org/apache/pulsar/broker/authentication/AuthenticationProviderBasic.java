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

package org.apache.pulsar.broker.authentication;

import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;

import javax.naming.AuthenticationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Base64;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class AuthenticationProviderBasic implements AuthenticationProvider {
    private final static String HTTP_HEADER_NAME = "Authorization";
    private final static String SYSTEM_PROPERTY_KEY = "pulsar.auth.basic.conf";
    private JsonObject roles;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        File confFile = new File(System.getProperty(SYSTEM_PROPERTY_KEY));
        if (!confFile.exists()) {
            throw new IOException("The password auth conf file does not exist");
        } else if (!confFile.isFile()) {
            throw new IOException("The path is not a file");
        }
        BufferedReader reader = new BufferedReader(new FileReader(confFile));
        String jsonString = reader.lines().collect(Collectors.joining());
        reader.close();
        roles = (new Gson()).fromJson(jsonString, JsonObject.class);
    }

    @Override
    public String getAuthMethodName() {
        return "basic";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        AuthParams authParams;
        if (authData.hasDataFromCommand()) {
            authParams = new AuthParams(authData.getCommandData());
        } else if (authData.hasDataFromHttp()) {
            authParams = new AuthParams(authData.getHttpHeader(HTTP_HEADER_NAME));
        } else {
            throw new AuthenticationException("Authentication data source does not have data");
        }

        String userId = authParams.getUserId();
        String password = authParams.getPassword();
        if (roles.get(userId) == null || !roles.get(userId).getAsJsonObject().get("password").getAsString()
                .equals(password)) {
            throw new AuthenticationException("Unknown user or invalid password");
        }

        if (roles.get(userId).getAsJsonObject().get("role") == null || StringUtils
                .isBlank(roles.get(userId).getAsJsonObject().get("role").getAsString())) {
            throw new AuthenticationException("Invalid role definition");
        }

        return roles.get(userId).getAsJsonObject().get("role").getAsString();
    }

    private class AuthParams {
        private String userId;
        private String password;

        public AuthParams(String rawAuthToken) throws AuthenticationException {
            // parsing and validation
            if (StringUtils.isBlank(rawAuthToken) || !rawAuthToken.toUpperCase().startsWith("BASIC ")) {
                throw new AuthenticationException("Authentication token has to be started with \"Basic \"");
            }
            String[] splittedRawAuthToken = rawAuthToken.split(" ");
            if (splittedRawAuthToken.length != 2) {
                throw new AuthenticationException("Base64 encoded token is not found");
            }

            String decodedAuthToken;
            try {
                decodedAuthToken = new String(Base64.getDecoder().decode(splittedRawAuthToken[1]));
            } catch (Exception e) {
                throw new AuthenticationException("Base64 decoding is failure: " + e.getMessage());
            }

            String[] parsedAuthParams = decodedAuthToken.split(":");

            if (parsedAuthParams.length != 2) {
                throw new AuthenticationException("Base64 decoded params are invalid");
            }

            userId = parsedAuthParams[0];
            password = parsedAuthParams[1];
        }

        public String getUserId() {
            return userId;
        }

        public String getPassword() {
            return password;
        }
    }
}
