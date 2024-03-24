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

import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

import static org.testng.Assert.assertEquals;

public class ClientCredentialsFlowTest {

    private static void testLoadPrivateKeyFile(String keyFileUrl) {
        try {
            KeyFile keyFile = ClientCredentialsFlow.loadPrivateKey(keyFileUrl);
            assertEquals(keyFile.getType(), "type");
            assertEquals(keyFile.getClientId(), "client-id");
            assertEquals(keyFile.getClientSecret(), "client-secret");
            assertEquals(keyFile.getClientEmail(), "client-email");
            assertEquals(keyFile.getIssuerUrl(), "https://issuer-url/");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLoadPrivateKeyFromResourceUrl() {
        testLoadPrivateKeyFile(ClientCredentialsFlowTest.class.getResource("/oauth2_key.json").toString());
    }

    @Test
    public void testLoadPrivateKeyFromUrlHandlerUrl() {
        ClasspathURLStreamHandler.register();
        testLoadPrivateKeyFile("resource:oauth2_key.json");
    }

    @Test
    public void testLoadPrivateKeyFromDataUrl() {
        testLoadPrivateKeyFile("data:application/json;base64,ewogICJ0eXBlIjogInR5cGUiLAogICJjbGllbnRfaWQiOiAiY2xpZW50LWlkIiwKICAiY2xpZW50X3NlY3JldCI6ICJjbGllbnQtc2VjcmV0IiwKICAiY2xpZW50X2VtYWlsIjogImNsaWVudC1lbWFpbCIsCiAgImlzc3Vlcl91cmwiOiAiaHR0cHM6Ly9pc3N1ZXItdXJsLyIKfQ==");
    }

    public static class ClasspathURLStreamHandler implements URLStreamHandlerFactory {
        private static final URLStreamHandlerFactory INSTANCE = new ClasspathURLStreamHandler();

        public ClasspathURLStreamHandler() {
        }

        public static void register() {
            URL.setURLStreamHandlerFactory(INSTANCE);
        }

        public URLStreamHandler createURLStreamHandler(String protocol) {
            return "resource".equals(protocol) ? new URLStreamHandler() {
                protected URLConnection openConnection(URL u) throws IOException {
                    String path = u.getPath();
                    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                    URL resource = classLoader == null ? null : classLoader.getResource(path);
                    if (resource == null) {
                        resource = ClassLoader.getSystemClassLoader().getResource(path);
                    }

                    return resource != null ? new NoContentTypeResourceUrlConnection(resource) : null;
                }
            } : null;
        }

        static class NoContentTypeResourceUrlConnection extends URLConnection {

            public NoContentTypeResourceUrlConnection(URL url) {
                super(url);
                this.url = url;
            }

            @Override
            public void connect() throws IOException {

            }

            @Override
            public String getContentType() {
                return null;
            }

            @Override
            public String getContentEncoding() {
                return "identity";
            }

            @Override
            public InputStream getInputStream() throws IOException {
                return url.openStream();
            }
        }

    }

}
