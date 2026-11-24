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
package org.apache.pulsar.websocket.proxy;

import org.apache.pulsar.websocket.service.WebSocketProxyConfiguration;
import org.testng.annotations.Test;

/**
 * Tests WebSocket proxy authentication when the websocket server connects to the broker
 * using admin token authentication instead of proxy role authentication.
 */
@Test(groups = "websocket")
public class NonProxyRoleTest extends ProxyRoleAuthTest {

    @Override
    protected WebSocketProxyConfiguration getProxyConfig() {
        WebSocketProxyConfiguration configuration = super.getProxyConfig();
        // Configure proxy's internal client to use ADMIN_TOKEN when connecting to broker
        configuration.setBrokerClientAuthenticationPlugin("org.apache.pulsar.client.impl.auth.AuthenticationToken");
        configuration.setBrokerClientAuthenticationParameters("token:" + ADMIN_TOKEN);
        return configuration;
    }
}