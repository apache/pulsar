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
package org.apache.pulsar.tls;

import java.util.Objects;

/**
 * Identifies <em>why</em> TLS is requested and in what role (PIP-478).
 *
 * <p>A {@code TlsPurpose} is a <strong>simple named key</strong>, not a type hierarchy: a
 * {@link Role} (client or server) and an open {@link #name()}. A {@link PulsarTlsFactory} serves
 * distinct TLS material per purpose; the well-known purposes are exposed as constants, and components
 * may mint additional open-named purposes with {@link #client(String)} / {@link #server(String)}.
 *
 * <p><b>Terminal resolution.</b> When nothing is configured for a purpose, resolution ends: for the
 * {@link Role#CLIENT} role it resolves to the <em>system default</em> (the OS trust store, no client
 * certificate); for the {@link Role#SERVER} role it is a configuration error. In particular the OAuth2 /
 * identity-provider purpose ({@link #CLIENT_OAUTH2}) resolves to the system default rather than reusing
 * Pulsar-cluster TLS material, since the identity provider is a different trust domain.
 *
 * <p>The value is immutable. Instances are safe to use as map keys — {@link #equals(Object)} /
 * {@link #hashCode()} are defined over the role and name.
 */
public final class TlsPurpose {

    /** Whether the purpose describes an outbound (client) or inbound (server) TLS endpoint. */
    public enum Role {
        /** Outbound TLS: the local endpoint acts as a TLS client. */
        CLIENT,
        /** Inbound TLS: the local endpoint acts as a TLS server. */
        SERVER
    }

    // Well-known CLIENT purposes.

    /** Pulsar-cluster traffic: binary protocol, HTTP topic lookup, and the admin client. */
    public static final TlsPurpose CLIENT_DEFAULT = new TlsPurpose(Role.CLIENT, "default");

    /**
     * OAuth2 / identity-provider calls. An unconfigured {@code CLIENT_OAUTH2} resolves to the system
     * default rather than to the cluster material, because the identity provider is a different trust
     * domain and its TLS material must not be shared with Pulsar-cluster connections.
     */
    public static final TlsPurpose CLIENT_OAUTH2 = new TlsPurpose(Role.CLIENT, "oauth2");

    /**
     * A server component's own outbound Pulsar-client traffic — geo-replication, proxy&rarr;broker,
     * websocket&rarr;broker, and functions-worker&rarr;broker connections. A distinct trust domain
     * from both the server listeners and any application client (configured through the dedicated
     * {@code brokerClient*} keys on the server side).
     */
    public static final TlsPurpose BROKER_CLIENT = new TlsPurpose(Role.CLIENT, "broker-client");

    // Well-known SERVER purposes.

    /** The broker's binary protocol listener(s). */
    public static final TlsPurpose BROKER = new TlsPurpose(Role.SERVER, "broker");

    /** The proxy's binary protocol front-end. */
    public static final TlsPurpose PROXY = new TlsPurpose(Role.SERVER, "proxy");

    /** A component's Jetty web service (broker / proxy / functions-worker). */
    public static final TlsPurpose WEB = new TlsPurpose(Role.SERVER, "web");

    private final Role role;
    private final String name;

    private TlsPurpose(Role role, String name) {
        this.role = Objects.requireNonNull(role, "role must not be null");
        this.name = Objects.requireNonNull(name, "name must not be null");
    }

    /**
     * @return whether this purpose is a client-role (outbound) or server-role (inbound) endpoint
     */
    public Role role() {
        return role;
    }

    /**
     * @return the well-known or plugin-minted name, e.g. {@code "default"}, {@code "oauth2"},
     *         {@code "broker-client"}, {@code "broker"}
     */
    public String name() {
        return name;
    }

    /**
     * Mint a client-role purpose. An unconfigured client purpose resolves to the system default (OS
     * trust store, no client certificate).
     *
     * @param name the open purpose name
     * @return a new client-role {@link TlsPurpose}
     */
    public static TlsPurpose client(String name) {
        return new TlsPurpose(Role.CLIENT, name);
    }

    /**
     * Mint a server-role purpose. An unconfigured server purpose is a configuration error.
     *
     * @param name the open purpose name
     * @return a new server-role {@link TlsPurpose}
     */
    public static TlsPurpose server(String name) {
        return new TlsPurpose(Role.SERVER, name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TlsPurpose that)) {
            return false;
        }
        return role == that.role && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(role, name);
    }

    @Override
    public String toString() {
        return "TlsPurpose{" + role + ' ' + name + '}';
    }
}
