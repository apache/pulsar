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
package org.apache.pulsar.proxy.server;

import org.apache.pulsar.broker.tls.TlsFactorySupport;

/**
 * The BROKER_CLIENT auth-material fold gate (PIP-478 stage 3c): re-runs {@link AuthedAdminProxyHandlerTest}
 * with the <em>new</em> PIP-478 broker-client TLS path selected (a non-blank {@code brokerClientTlsFactoryClassName}
 * makes {@code TlsFactorySupport.selectPath} choose {@code NEW}). The proxy's
 * {@code brokerClientAuthenticationPlugin=AuthenticationTls} certificate must be folded over the (blank)
 * {@code brokerClient*FilePath} policy, or the proxy would present no / the wrong identity to the broker and
 * forwarded-principal authorization would fail (the flip-CI blocker). The superclass keeps exercising the
 * legacy path unchanged.
 */
public class AuthedAdminProxyHandlerNewTlsPathTest extends AuthedAdminProxyHandlerTest {

    @Override
    protected void customizeProxyConfiguration(ProxyConfiguration proxyConfig) {
        // Select the built-in default PIP-478 factory for the broker-client (BROKER_CLIENT) purpose.
        proxyConfig.setBrokerClientTlsFactoryClassName(TlsFactorySupport.DEFAULT_FACTORY);
    }
}
