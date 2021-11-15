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
package org.apache.pulsar.proxy.server;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class InvalidProxyConfigForAuthorizationTest {

    @Test
    void startupShouldFailWhenAuthorizationIsEnabledWithoutAuthentication() throws Exception {
        ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setAuthorizationEnabled(true);
        proxyConfiguration.setAuthenticationEnabled(false);
        try (ProxyService proxyService = new ProxyService(proxyConfiguration,
                Mockito.mock(AuthenticationService.class))) {
            proxyService.start();
            fail("An exception should have been thrown");
        } catch (Exception e) {
            assertEquals(e.getClass(), IllegalStateException.class);
            assertEquals(e.getMessage(), "Invalid proxy configuration. Authentication must be "
                    + "enabled with authenticationEnabled=true when authorization is enabled with "
                    + "authorizationEnabled=true.");
        }
    }
}
