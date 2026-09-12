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
 * v5-native implementation of the built-in SASL authentication plugin, on <em>both</em> transports
 * (PIP-478).
 *
 * <p>{@link org.apache.pulsar.client.impl.auth.v5.SaslAuthenticationV5} implements the binary capabilities
 * ({@code BinaryAuthDataProvider} / {@code BinaryAuthChallengeHandler}) and the HTTP ones
 * ({@code HttpAuthHeadersProvider} / {@code HttpAuthChallengeHandler}). The v4
 * {@code AuthenticationSasl} class in the parent package is a shim that keeps its verbatim synchronous
 * surface for third-party callers, and routes both transports here: the binary path through the shared
 * {@code V5BinaryAuthenticationDriver}, and the SASL-over-HTTP {@code 401}→resubmit→{@code 200} loop
 * through {@code HttpAuthenticationDriver} rather than the deprecated
 * {@code authenticationStage(...)} hook.
 */
package org.apache.pulsar.client.impl.auth.v5;
