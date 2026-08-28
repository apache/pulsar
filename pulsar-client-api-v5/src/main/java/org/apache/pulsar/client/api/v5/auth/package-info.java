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
 * Pluggable authentication (and end-to-end encryption) types for the v5 client (PIP-478).
 *
 * <p>{@link org.apache.pulsar.client.api.v5.auth.Authentication} carries only lifecycle
 * ({@code configure} / {@code initializeAsync} / {@code capability} / {@code close}); the credential work
 * lives on opt-in <em>capability</em> interfaces the framework discovers <em>only</em> through
 * {@link org.apache.pulsar.client.api.v5.auth.Authentication#capability(Class)} — never
 * {@code instanceof}. The capabilities are segregated on two axes (transport × style):
 *
 * <table border="1">
 *   <caption>The capability matrix</caption>
 *   <tr><th></th><th>Pulsar binary protocol</th><th>HTTP</th></tr>
 *   <tr><td><b>Single-pass</b></td>
 *       <td>{@link org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider}</td>
 *       <td>{@link org.apache.pulsar.client.api.v5.auth.HttpAuthHeadersProvider}</td></tr>
 *   <tr><td><b>Multi-round challenge/response</b></td>
 *       <td>{@link org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler}</td>
 *       <td>{@link org.apache.pulsar.client.api.v5.auth.HttpAuthChallengeHandler}</td></tr>
 * </table>
 *
 * <p>The convenience composite {@link org.apache.pulsar.client.api.v5.auth.SinglePassAuthentication}
 * bundles the common single-pass-over-both-transports combination. All capability methods are
 * asynchronous, so credential acquisition never blocks the Netty event loop.
 *
 * <p><b>Two configuration paths.</b>
 * <ul>
 *   <li><em>Programmatic</em> — build and configure an instance yourself, then pass it to the builder's
 *       {@code authentication(Authentication)}. {@link org.apache.pulsar.client.api.v5.auth.Authentication#configure}
 *       is NOT called (the instance is presumed already configured). The convenience factories in
 *       {@link org.apache.pulsar.client.api.v5.auth.AuthenticationFactory} return such instances.</li>
 *   <li><em>Reflective by class name</em> — {@code authentication(authPluginClassName, authParams)}: the
 *       framework loads the class, constructs it no-arg, and calls
 *       {@link org.apache.pulsar.client.api.v5.auth.Authentication#configure} with the parsed params. A
 *       class implementing only this v5 SPI works over this path exactly like a built-in.</li>
 * </ul>
 *
 * <p><b>Binary routing rules (normative).</b> For every plugin, {@code ClientCnx} routes as:
 * <ol>
 *   <li><b>Initial connect</b> &rarr; {@link org.apache.pulsar.client.api.v5.auth.BinaryAuthDataProvider};
 *       this capability is required for the binary transport — a plugin that does not expose it fails at
 *       client build with an {@code UnsupportedAuthenticationException}.</li>
 *   <li><b>The refresh sentinel</b> ({@code AuthData.REFRESH_AUTH_DATA_BYTES}) &rarr; the current
 *       credential is re-produced through {@code BinaryAuthDataProvider} on a <em>fresh</em> exchange; the
 *       sentinel is filtered out and never reaches the challenge handler.</li>
 *   <li><b>Any other challenge</b> &rarr;
 *       {@link org.apache.pulsar.client.api.v5.auth.BinaryAuthChallengeHandler}; a plugin that receives a
 *       challenge without exposing the handler fails the connection with an
 *       {@code AuthenticationException}.</li>
 * </ol>
 *
 * <p>The package also provides end-to-end encryption support via
 * {@link org.apache.pulsar.client.api.v5.auth.PublicKeyProvider} (producer side) and
 * {@link org.apache.pulsar.client.api.v5.auth.PrivateKeyProvider} (consumer side); for local
 * PEM-file-backed setups use {@link org.apache.pulsar.client.api.v5.auth.PemFileKeyProvider}.
 */
package org.apache.pulsar.client.api.v5.auth;
