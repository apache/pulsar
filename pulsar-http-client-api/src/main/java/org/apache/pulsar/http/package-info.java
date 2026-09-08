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

/**
 * The framework-managed HTTP client SPI (PIP-478).
 *
 * <p>Authentication plugins that need HTTP (for example OAuth2's token endpoint or Athenz's ZTS) obtain
 * a {@link org.apache.pulsar.http.PulsarHttpClient} from the framework via
 * {@code AuthenticationInitContext.httpClientFactory()} rather than constructing their own. The
 * framework owns the lifecycle (Netty event loop, timer, DNS cache, TLS material refresh integration)
 * and selects each instance's TLS material by {@link org.apache.pulsar.tls.TlsPurpose}; plugins
 * describe what they need through a {@link org.apache.pulsar.http.PulsarHttpClientConfig}.
 *
 * <p>The HTTP backend is framework-owned (built on AsyncHttpClient) and deliberately not pluggable. The
 * SPI is hosted in the focused {@code pulsar-http-client-api} module so it can later serve other HTTP
 * needs inside Pulsar (e.g. broker-side JWKS fetching) without importing a client artifact. The module
 * depends on {@code pulsar-tls-factory-api} for the {@link org.apache.pulsar.tls.TlsPurpose} value type.
 */
package org.apache.pulsar.http;
