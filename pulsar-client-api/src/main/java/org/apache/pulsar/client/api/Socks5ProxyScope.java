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
package org.apache.pulsar.client.api;

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Selector that controls which kinds of connections should be routed through the configured
 * SOCKS5 proxy when a proxy address is set on a Pulsar client or admin builder.
 *
 * <p>Historically, the SOCKS5 proxy configured on {@code PulsarClient} only affected the Pulsar
 * binary protocol connections to brokers, and the HTTP lookup / failover clients inside
 * {@code PulsarClient} silently ignored it. The scope selector makes this behavior explicit and
 * also allows users to route HTTP traffic (HTTP lookups, failover HTTP clients, and
 * {@code PulsarAdmin} REST calls) through the same SOCKS5 proxy.
 *
 * <p>For {@code PulsarClient}, the default is {@link #BINARY_ONLY} so existing behavior is
 * preserved. For {@code PulsarAdmin}, the default is {@link #HTTP_ONLY} because admin traffic is
 * HTTP by nature.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum Socks5ProxyScope {

    /**
     * Apply the SOCKS5 proxy only to Pulsar binary protocol connections to brokers. HTTP
     * lookups and HTTP-based failover connections ignore the proxy. This matches the pre-existing
     * behavior of {@code PulsarClient} and is the default for client builders.
     */
    BINARY_ONLY,

    /**
     * Apply the SOCKS5 proxy only to HTTP traffic, i.e. HTTP/HTTPS lookups, failover HTTP
     * clients, and REST calls issued by {@code PulsarAdmin}. Pulsar binary protocol connections
     * to brokers do not use the proxy.
     */
    HTTP_ONLY,

    /**
     * Apply the SOCKS5 proxy to both Pulsar binary protocol connections and HTTP traffic.
     */
    BOTH;

    /**
     * @return {@code true} if this scope routes Pulsar binary protocol connections through SOCKS5.
     */
    public boolean appliesToBinary() {
        return this == BINARY_ONLY || this == BOTH;
    }

    /**
     * @return {@code true} if this scope routes HTTP/HTTPS traffic through SOCKS5.
     */
    public boolean appliesToHttp() {
        return this == HTTP_ONLY || this == BOTH;
    }
}
