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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import org.testng.annotations.Test;

public class ClientCredentialsFlowTest {

    private static final String KEY_JSON_BASE64 = "ewogICJ0eXBlIjogInR5cGUiLAogICJjbGllbnRfaWQiOiAiY2xpZW50LWlkIiwKICAi"
            + "Y2xpZW50X3NlY3JldCI6ICJjbGllbnQtc2VjcmV0IiwKICAiY2xpZW50X2VtYWlsIjogImNsaWVudC1lbWFpbCIsCiAgImlzc3Vl"
            + "cl91cmwiOiAiaHR0cHM6Ly9pc3N1ZXItdXJsLyIKfQ==";

    private static void assertKeyFile(KeyFile keyFile) {
        assertEquals(keyFile.getType(), "type");
        assertEquals(keyFile.getClientId(), "client-id");
        assertEquals(keyFile.getClientSecret(), "client-secret");
        assertEquals(keyFile.getClientEmail(), "client-email");
        assertEquals(keyFile.getIssuerUrl(), "https://issuer-url/");
    }

    private static URL keyResource() {
        return ClientCredentialsFlowTest.class.getResource("/oauth2_key.json");
    }

    @Test
    public void testLoadPrivateKeyFromFileUrl() throws Exception {
        assertKeyFile(ClientCredentialsFlow.loadPrivateKey(keyResource().toString()));
    }

    @Test
    public void testLoadPrivateKeyFromPath() throws Exception {
        assertKeyFile(ClientCredentialsFlow.loadPrivateKey(new File(keyResource().toURI()).getAbsolutePath()));
    }

    @Test
    public void testLoadPrivateKeyFromDataUrl() throws Exception {
        assertKeyFile(ClientCredentialsFlow.loadPrivateKey("data:application/json;base64," + KEY_JSON_BASE64));
    }

    @Test
    public void testLoadPrivateKeyRejectsDataUrlWithoutJsonContentType() {
        assertThrows(IllegalArgumentException.class,
                () -> ClientCredentialsFlow.loadPrivateKey("data:;base64," + KEY_JSON_BASE64));
    }

    @Test
    public void testLoadPrivateKeyFromConnectionWithoutContentType() throws Exception {
        URL resource = keyResource();
        // Stands in for a connection from a custom URL handler that does not report a content type,
        // for which URLConnection.getContent() fails with "no content-type"
        URLConnection connection = new URLConnection(URI.create("http://localhost/oauth2_key.json").toURL()) {
            @Override
            public void connect() {
            }

            @Override
            public String getContentType() {
                return null;
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return resource.openStream();
            }
        };
        assertNull(connection.getContentType());
        assertKeyFile(ClientCredentialsFlow.loadPrivateKey(connection));
    }
}
