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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.function.Supplier;

import org.apache.commons.io.FileUtils;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationDataProvider;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

public class AuthenticationTokenTest {

    @Test
    public void testAuthToken() throws Exception {
        AuthenticationToken authToken = new AuthenticationToken("token-xyz");
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "token-xyz");

        assertFalse(authData.hasDataForTls());
        assertNull(authData.getTlsCertificates());
        assertNull(authData.getTlsPrivateKey());

        assertTrue(authData.hasDataForHttp());
        assertEquals(authData.getHttpHeaders(),
                Collections.singletonMap("Authorization", "Bearer token-xyz").entrySet());

        authToken.close();
    }

    @Test
    public void testAuthTokenClientConfig() throws Exception {
        ClientConfigurationData clientConfig = new ClientConfigurationData();
        clientConfig.setServiceUrl("pulsar://service-url");
        clientConfig.setAuthPluginClassName(AuthenticationToken.class.getName());
        clientConfig.setAuthParams("token-xyz");

        PulsarClientImpl pulsarClient = new PulsarClientImpl(clientConfig);

        Authentication authToken = pulsarClient.getConfiguration().getAuthentication();
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "token-xyz");

        assertFalse(authData.hasDataForTls());
        assertNull(authData.getTlsCertificates());
        assertNull(authData.getTlsPrivateKey());

        assertTrue(authData.hasDataForHttp());
        assertEquals(authData.getHttpHeaders(),
                Collections.singletonMap("Authorization", "Bearer token-xyz").entrySet());

        authToken.close();
    }

    @Test
    public void testAuthTokenConfig() throws Exception {
        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("token:my-test-token-string");
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "my-test-token-string");
        authToken.close();
    }

    @Test
    public void testAuthTokenConfigFromFile() throws Exception {
        File tokenFile = File.createTempFile("pulsar-test-token", ".key");
        tokenFile.deleteOnExit();
        FileUtils.write(tokenFile, "my-test-token-string", StandardCharsets.UTF_8);

        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure(getTokenFileUri(tokenFile));
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "my-test-token-string");

        // Ensure if the file content changes, the token will get refreshed as well
        FileUtils.write(tokenFile, "other-token", StandardCharsets.UTF_8);

        AuthenticationDataProvider authData2 = authToken.getAuthData();
        assertTrue(authData2.hasDataFromCommand());
        assertEquals(authData2.getCommandData(), "other-token");

        authToken.close();
    }

    /**
     * File can have spaces and newlines before or after the token. We should be able to read
     * the token correctly anyway.
     */
    @Test
    public void testAuthTokenConfigFromFileWithNewline() throws Exception {
        File tokenFile = File.createTempFile("pulsar-test-token", ".key");
        tokenFile.deleteOnExit();
        FileUtils.write(tokenFile, "  my-test-token-string  \r\n", StandardCharsets.UTF_8);

        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure(getTokenFileUri(tokenFile));
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "my-test-token-string");

        // Ensure if the file content changes, the token will get refreshed as well
        FileUtils.write(tokenFile, "other-token", StandardCharsets.UTF_8);

        AuthenticationDataProvider authData2 = authToken.getAuthData();
        assertTrue(authData2.hasDataFromCommand());
        assertEquals(authData2.getCommandData(), "other-token");

        authToken.close();
    }

    @Test
    public void testAuthTokenConfigNoPrefix() throws Exception {
        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("my-test-token-string");
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "my-test-token-string");
        authToken.close();
    }

    @Test
    public void testAuthTokenConfigFromJson() throws Exception{
        AuthenticationToken authToken = new AuthenticationToken();
        authToken.configure("{\"token\":\"my-test-token-string\"}");
        assertEquals(authToken.getAuthMethodName(), "token");

        AuthenticationDataProvider authData = authToken.getAuthData();
        assertTrue(authData.hasDataFromCommand());
        assertEquals(authData.getCommandData(), "my-test-token-string");
        authToken.close();
    }

    @Test
    public void testSerializableAuthentication() throws Exception {
        SerializableSupplier tokenSupplier = new SerializableSupplier("cert");
        AuthenticationToken token = new AuthenticationToken(tokenSupplier);

        // serialize
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(outStream);
        out.writeObject(token);
        out.flush();
        byte[] outputBytes = outStream.toByteArray();
        out.close();

        // deserialize
        ByteArrayInputStream bis = new ByteArrayInputStream(outputBytes);
        ObjectInput in = new ObjectInputStream(bis);
        AuthenticationToken ts = (AuthenticationToken) in.readObject();
        in.close();

        // read the deserialized object
        assertEquals(tokenSupplier.token, ts.getAuthData().getCommandData());
    }

    private String getTokenFileUri(File file) {
        return "file:///" + file.toString().replace('\\', '/');
    }

    public static class SerializableSupplier implements Supplier<String>, Serializable {

        private static final long serialVersionUID = 6259616338933150683L;
        private final String token;

        public SerializableSupplier(final String token) {
            super();
            this.token = token;
        }

        @Override
        public String get() {
            return token;
        }
    }
}
