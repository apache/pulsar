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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.commons.codec.digest.Crypt;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.authentication.metrics.AuthenticationMetrics;
import org.apache.pulsar.client.api.url.URL;

public class AuthenticationProviderBasic implements AuthenticationProvider {
    private static final String HTTP_HEADER_NAME = "Authorization";
    private static final String CONF_SYSTEM_PROPERTY_KEY = "pulsar.auth.basic.conf";
    private static final String CONF_PULSAR_PROPERTY_KEY = "basicAuthConf";
    private Map<String, String> users;

    private enum ErrorCode {
        UNKNOWN,
        EMPTY_AUTH_DATA,
        INVALID_HEADER,
        INVALID_AUTH_DATA,
        INVALID_TOKEN,
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    public static byte[] readData(String data)
            throws IOException, URISyntaxException, InstantiationException, IllegalAccessException {
        if (data.startsWith("data:") || data.startsWith("file:")) {
            return IOUtils.toByteArray(URL.createURL(data));
        } else if (Files.exists(Paths.get(data))) {
            return Files.readAllBytes(Paths.get(data));
        } else if (org.apache.commons.codec.binary.Base64.isBase64(data)) {
            return Base64.getDecoder().decode(data);
        } else {
            String msg = "Not supported config";
            throw new IllegalArgumentException(msg);
        }
    }

    @Override
    public void initialize(ServiceConfiguration config) throws IOException {
        String data = config.getProperties().getProperty(CONF_PULSAR_PROPERTY_KEY);
        if (StringUtils.isEmpty(data)) {
            data = System.getProperty(CONF_SYSTEM_PROPERTY_KEY);
        }
        if (StringUtils.isEmpty(data)) {
            throw new IOException("No basic authentication config provided");
        }

        @Cleanup BufferedReader reader = null;
        try {
            byte[] bytes = readData(data);
            reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }

        users = new HashMap<>();
        for (String line : reader.lines().toArray(s -> new String[s])) {
            List<String> splitLine = Arrays.asList(line.split(":"));
            if (splitLine.size() != 2) {
                throw new IOException("The format of the password auth conf file is invalid");
            }
            users.put(splitLine.get(0), splitLine.get(1));
        }
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
        ErrorCode errorCode = ErrorCode.UNKNOWN;
        try {
            if (users.get(userId) == null) {
                errorCode = ErrorCode.INVALID_AUTH_DATA;
                throw new AuthenticationException(msg);
            }

            String encryptedPassword = users.get(userId);

            // For md5 algorithm
            if ((users.get(userId).startsWith("$apr1"))) {
                List<String> splitEncryptedPassword = Arrays.asList(encryptedPassword.split("\\$"));
                if (splitEncryptedPassword.size() != 4 || !encryptedPassword
                        .equals(Md5Crypt.apr1Crypt(password.getBytes(), splitEncryptedPassword.get(2)))) {
                    errorCode = ErrorCode.INVALID_TOKEN;
                    throw new AuthenticationException(msg);
                }
                // For crypt algorithm
            } else if (!encryptedPassword.equals(Crypt.crypt(password.getBytes(), encryptedPassword.substring(0, 2)))) {
                errorCode = ErrorCode.INVALID_TOKEN;
                throw new AuthenticationException(msg);
            }
        } catch (AuthenticationException exception) {
            incrementFailureMetric(errorCode);
            throw exception;
        }
        AuthenticationMetrics.authenticateSuccess(getClass().getSimpleName(), getAuthMethodName());
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
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Authentication token has to be started with \"Basic \"");
                }
                String[] splitRawAuthToken = rawAuthToken.split(" ");
                if (splitRawAuthToken.length != 2) {
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Base64 encoded token is not found");
                }

                try {
                    authParams = new String(Base64.getDecoder().decode(splitRawAuthToken[1]));
                } catch (Exception e) {
                    incrementFailureMetric(ErrorCode.INVALID_HEADER);
                    throw new AuthenticationException("Base64 decoding is failure: " + e.getMessage());
                }
            } else {
                incrementFailureMetric(ErrorCode.EMPTY_AUTH_DATA);
                throw new AuthenticationException("Authentication data source does not have data");
            }

            String[] parsedAuthParams = authParams.split(":");
            if (parsedAuthParams.length != 2) {
                incrementFailureMetric(ErrorCode.INVALID_AUTH_DATA);
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
