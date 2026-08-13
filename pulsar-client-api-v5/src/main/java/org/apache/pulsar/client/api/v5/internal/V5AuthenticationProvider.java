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
package org.apache.pulsar.client.api.v5.internal;

import org.apache.pulsar.client.api.v5.auth.Authentication;

/**
 * Implemented by a legacy v4 {@code org.apache.pulsar.client.api.Authentication} that owns a v5-native
 * body, so the client can drive the v5 body directly instead of bridging the v4 plugin (PIP-478).
 *
 * <p>The client drives the v5 authentication model natively: it resolves exactly one v5
 * {@link Authentication} per client and drives every transport from it. A v4 plugin therefore has to be
 * turned into a v5 one, and there are two ways to do that. The built-in shims that already <em>have</em> a
 * v5-native body — token, basic and OAuth2, where the v4 class is a thin compatibility surface over it —
 * hand that body over through this interface. Everything else, including the built-in Athenz and SASL
 * plugins, whose credential machinery has no v5-native body today, is wrapped by
 * {@code LegacyV4AuthenticationAdapter}, which re-expresses the v4 surface as v5 capabilities and
 * off-loads every v4 call to the client's blocking executor.
 *
 * <p>Handing over the native body is not merely tidier: the generic bridge exists to make an
 * <em>unknown</em> plugin safe, and it pays for that with an executor hop per call and a start-time probe
 * of the v4 plugin's capabilities. A built-in whose body is already asynchronous and non-blocking needs
 * neither.
 *
 * <p>The body is requested once per client, during client construction, and must be cheap to produce —
 * credential acquisition belongs in the body's own asynchronous methods, not here. Implementations must
 * return a body that reads any mutable configuration (a rotated token, for instance) at call time rather
 * than capturing it, since the same body serves the whole client lifetime.
 *
 * <p>The {@code .internal.} subpackage signals "stable internal" — application code should not implement
 * this interface; it is observed by the framework.
 */
public interface V5AuthenticationProvider {

    /**
     * The v5-native authentication body this v4 plugin delegates to.
     *
     * @return the v5 authentication to drive; never {@code null}
     */
    Authentication v5Authentication();
}
