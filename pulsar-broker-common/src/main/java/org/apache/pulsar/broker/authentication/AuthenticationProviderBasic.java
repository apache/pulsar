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

import org.apache.commons.codec.digest.Crypt;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;

import javax.naming.AuthenticationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class AuthenticationProviderBasic implements AuthenticationProvider {
    private final static String HTTP_HEADER_NAME = "Authorization";
    private final static String CONF_SYSTEM_PROPERTY_KEY = "pulsar.auth.basic.conf";
    private final static String REALM_INFORMATION = "Basic realm=\"Pulsar Web Service\"";
    private Map<String, String> users;

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        File confFile = new File(System.getProperty(CONF_SYSTEM_PROPERTY_KEY));
        if (!confFile.exists()) {
            throw new IOException("The password auth conf file does not exist");
        } else if (!confFile.isFile()) {
            throw new IOException("The path is not a file");
        }
        BufferedReader reader = new BufferedReader(new FileReader(confFile));
        users = new HashMap<>();
        for (String line : reader.lines().toArray(s -> new String[s])) {
            List<String> splitLine = Arrays.asList(line.split(":"));
            if (splitLine.size() != 2) {
                throw new IOException("The format of the password auth conf file is invalid");
            }
            users.put(splitLine.get(0), splitLine.get(1));
        }
        reader.close();
    }

    @Override
    public String getAuthMethodName() {
        return "basic";
    }

    @Override
    public String authenticate(AuthenticationDataSource authData) throws AuthenticationException {
        AuthParams authParams = new AuthParams(authData);
        String userId = authParams.getUserId();
        String password = authParams.getPassword();
        String msg = "Unknown user or invalid password";

        if (users.get(userId) == null) {
            throw new PulsarHttpAuthenticationException(msg, REALM_INFORMATION);
        }

        String encryptedPassword = users.get(userId);

        // For md5 algorithm
        if ((users.get(userId).startsWith("$apr1"))) {
            List<String> splitEncryptedPassword = Arrays.asList(encryptedPassword.split("\\$"));
            if (splitEncryptedPassword.size() != 4 || !encryptedPassword
                    .equals(Md5Crypt.apr1Crypt(password.getBytes(), splitEncryptedPassword.get(2)))) {
                throw new PulsarHttpAuthenticationException(msg, REALM_INFORMATION);
            }
        // For crypt algorithm
        } else if (!encryptedPassword.equals(Crypt.crypt(password.getBytes(), encryptedPassword.substring(0, 2)))) {
            throw new PulsarHttpAuthenticationException(msg, REALM_INFORMATION);
        }

        return userId;
    }

    private class AuthParams {
        private String userId;
        private String password;

        public AuthParams(AuthenticationDataSource authData) throws AuthenticationException {
            String authParams;
            if (authData.hasDataFromCommand()) {
                authParams = authData.getCommandData();
            } else if (authData.hasDataFromHttp()) {
                String rawAuthToken = authData.getHttpHeader(HTTP_HEADER_NAME);
                // parsing and validation
                if (StringUtils.isBlank(rawAuthToken) || !rawAuthToken.toUpperCase().startsWith("BASIC ")) {
                    throw new PulsarHttpAuthenticationException("Authentication token has to be started with \"Basic \"", REALM_INFORMATION);
                }
                String[] splitRawAuthToken = rawAuthToken.split(" ");
                if (splitRawAuthToken.length != 2) {
                    throw new PulsarHttpAuthenticationException("Base64 encoded token is not found", REALM_INFORMATION);
                }

                try {
                    authParams = new String(Base64.getDecoder().decode(splitRawAuthToken[1]));
                } catch (Exception e) {
                    throw new PulsarHttpAuthenticationException("Base64 decoding is failure: " + e.getMessage(), REALM_INFORMATION);
                }
            } else {
                throw new PulsarHttpAuthenticationException("Authentication data source does not have data", REALM_INFORMATION);
            }

            String[] parsedAuthParams = authParams.split(":");
            if (parsedAuthParams.length != 2) {
                throw new PulsarHttpAuthenticationException("Base64 decoded params are invalid", REALM_INFORMATION);
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
