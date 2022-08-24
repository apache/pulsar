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

import static org.testng.Assert.assertEquals;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Properties;
import javax.naming.AuthenticationException;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.AuthData;
import org.testng.annotations.Test;

public class AuthenticationProviderBasicTest {
    private final String basicAuthConf = Resources.getResource("authentication/basic/.htpasswd").getPath();
    private final String basicAuthConfBase64 = Base64.getEncoder().encodeToString(Files.readAllBytes(FileSystems.getDefault().getPath(basicAuthConf)));

    public AuthenticationProviderBasicTest() throws IOException {
    }

    private void testAuthenticate(AuthenticationProviderBasic provider) throws AuthenticationException {
        AuthData authData = AuthData.of("superUser2:superpassword".getBytes(StandardCharsets.UTF_8));
        provider.newAuthState(authData, null, null);
    }

    @Test
    public void testLoadFileFromPulsarProperties() throws Exception {
        @Cleanup
        AuthenticationProviderBasic provider = new AuthenticationProviderBasic();
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        Properties properties = new Properties();
        properties.setProperty("basicAuthConf", basicAuthConf);
        serviceConfiguration.setProperties(properties);
        provider.initialize(serviceConfiguration);
        testAuthenticate(provider);
    }

    @Test
    public void testLoadBase64FromPulsarProperties() throws Exception {
        @Cleanup
        AuthenticationProviderBasic provider = new AuthenticationProviderBasic();
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        Properties properties = new Properties();
        properties.setProperty("basicAuthConf", basicAuthConfBase64);
        serviceConfiguration.setProperties(properties);
        provider.initialize(serviceConfiguration);
        testAuthenticate(provider);
    }

    @Test
    public void testLoadFileFromSystemProperties() throws Exception {
        @Cleanup
        AuthenticationProviderBasic provider = new AuthenticationProviderBasic();
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        System.setProperty("pulsar.auth.basic.conf", basicAuthConf);
        provider.initialize(serviceConfiguration);
        testAuthenticate(provider);
    }

    @Test
    public void testLoadBase64FromSystemProperties() throws Exception {
        @Cleanup
        AuthenticationProviderBasic provider = new AuthenticationProviderBasic();
        ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
        System.setProperty("pulsar.auth.basic.conf", basicAuthConfBase64);
        provider.initialize(serviceConfiguration);
        testAuthenticate(provider);
    }

    @Test
    public void testReadData() throws Exception {
        byte[] data = Files.readAllBytes(FileSystems.getDefault().getPath(basicAuthConf));
        String base64Data = Base64.getEncoder().encodeToString(data);

        // base64 format
        assertEquals(AuthenticationProviderBasic.readData("data:;base64," + base64Data), data);
        assertEquals(AuthenticationProviderBasic.readData(base64Data), data);

        // file format
        assertEquals(AuthenticationProviderBasic.readData("file://" + basicAuthConf), data);
        assertEquals(AuthenticationProviderBasic.readData(basicAuthConf), data);
    }
}
