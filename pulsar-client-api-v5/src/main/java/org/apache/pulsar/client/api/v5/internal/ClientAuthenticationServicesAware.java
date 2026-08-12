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

/**
 * Implemented by an authentication driver that wants the framework's real client services late-bound
 * into it after the {@code PulsarClient} is constructed (PIP-478).
 *
 * <p>The client calls {@link #bindClientAuthenticationServices} exactly once, on
 * {@code conf.getAuthentication()}, <em>before</em> {@code Authentication.start()} — so the bound
 * services (real scheduler, bounded blocking executor, HTTP client factory, client instance id) are
 * visible to the plugin's {@code initializeAsync(...)} and to every capability call thereafter. A driver
 * that is never bound (a plain v4 plugin, or one used outside a client) simply keeps its unbound
 * behaviour: credential work runs inline on the caller thread rather than being off-loaded.
 *
 * <p>Both the built-in v4 auth shims (which drive a v5-native body) and the v5&hairsp;&rarr;&hairsp;v4
 * bridge ({@code V5ToV4AuthenticationAdapter}, exposing a genuinely v5-native plugin) implement this so
 * that the same late-binding path serves the v4-client and v5-client cases uniformly.
 *
 * <p>The {@code .internal.} subpackage signals "stable internal" — application code should not implement
 * this interface; it is observed and driven by the framework.
 */
public interface ClientAuthenticationServicesAware {

    /**
     * Bind the framework's client services into this driver. Called once, before the plugin is started.
     *
     * @param services the framework-owned client services
     */
    void bindClientAuthenticationServices(ClientAuthenticationServices services);
}
