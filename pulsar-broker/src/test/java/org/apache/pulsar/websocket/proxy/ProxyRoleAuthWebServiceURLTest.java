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

/**
 * Same test with ProxyRoleAuthTest but using REST API as the internal client.
 */
public class ProxyRoleAuthWebServiceURLTest extends ProxyRoleAuthTest {

    @Override
    protected WebSocketProxyConfiguration getProxyConfig() {
        // Create WebSocket proxy configuration with authentication and authorization enabled
        WebSocketProxyConfiguration proxyConfig = super.getProxyConfig();
        proxyConfig.setServiceUrl(pulsar.getWebServiceAddress());
        proxyConfig.setServiceUrlTls(pulsar.getWebServiceAddressTls());
        proxyConfig.setBrokerServiceUrl(null);
        proxyConfig.setBrokerServiceUrlTls(null);
        return  proxyConfig;
    }
}
