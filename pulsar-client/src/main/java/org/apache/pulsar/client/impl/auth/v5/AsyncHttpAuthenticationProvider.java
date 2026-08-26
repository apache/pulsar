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
package org.apache.pulsar.client.impl.auth.v5;

import java.util.Optional;

/**
 * Internal bridge a v4 {@code org.apache.pulsar.client.api.Authentication} plugin implements to expose its
 * v5-native HTTP multi-round authentication to the framework's HTTP client APIs (PIP-478).
 *
 * <p>This mirrors, on the HTTP transport, what
 * {@code org.apache.pulsar.client.api.internal.AsyncAuthenticationDriver} does on the binary transport:
 * the HTTP callers ({@code HttpClient} for topic lookup, {@code BaseResource}/{@code ComponentResource}
 * for the admin client) test a plugin for this marker and, when present, route the SASL-style
 * {@code 401}→resubmit→{@code 200} exchange through the shared {@link HttpAuthenticationDriver} instead of
 * the deprecated v4 {@code authenticationStage(...)} hook. A third-party v4 plugin that does not implement
 * this marker keeps working unchanged on the v4 hook.
 *
 * <p>The {@code instanceof} test used to detect this marker is on the <em>bridge</em>, not on a capability;
 * the driver still discovers what the returned v5 body supports only via {@code capability(...)} lookup,
 * never {@code instanceof}.
 */
public interface AsyncHttpAuthenticationProvider {

    /**
     * @return the framework HTTP auth driver bound to this plugin's v5-native body (capabilities + a
     *         default transport), or {@link Optional#empty()} if this plugin does not support v5-native
     *         HTTP multi-round authentication
     */
    Optional<HttpAuthenticationDriver> httpAuthenticationDriver();
}
