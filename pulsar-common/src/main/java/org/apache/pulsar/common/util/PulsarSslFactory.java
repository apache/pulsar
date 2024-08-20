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
package org.apache.pulsar.common.util;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

/**
 * Factory for generating SSL Context and SSL Engine using {@link PulsarSslConfiguration}.
 */
public interface PulsarSslFactory extends AutoCloseable {

    /**
     * Initializes the PulsarSslFactory.
     * @param config {@link PulsarSslConfiguration} object required for initialization
     */
    void initialize(PulsarSslConfiguration config);

    /**
     * Creates a Client {@link SSLEngine} utilizing {@link ByteBufAllocator} object, the peer hostname, peer port and
     * {@link PulsarSslConfiguration} object provided during initialization.
     *
     * @param buf The ByteBufAllocator required for netty connections. This can be passed as {@code null} if utilized
     *            for web connections.
     * @param peerHost the name of the peer host
     * @param peerPort the port number of the peer
     * @return {@link SSLEngine}
     */
    SSLEngine createClientSslEngine(ByteBufAllocator buf, String peerHost, int peerPort);

    /**
     * Creates a Server {@link SSLEngine} utilizing the {@link ByteBufAllocator} object and
     * {@link PulsarSslConfiguration} object provided during initialization.
     *
     * @param buf The ByteBufAllocator required for netty connections. This can be passed as {@code null} if utilized
     *            for web connections.
     * @return {@link SSLEngine}
     */
    SSLEngine createServerSslEngine(ByteBufAllocator buf);

    /**
     * Returns a boolean value indicating {@link SSLContext} or {@link SslContext} should be refreshed.
     *
     * @return {@code true} if {@link SSLContext} or {@link SslContext} should be refreshed.
     */
    boolean needsUpdate();

    /**
     * Update the internal {@link SSLContext} or {@link SslContext}.
     * @throws Exception if there are any issues generating the new {@link SSLContext} or {@link SslContext}
     */
    default void update() throws Exception {
        if (this.needsUpdate()) {
            this.createInternalSslContext();
        }
    }

    /**
     * Creates the following:
     * 1. {@link SslContext} if netty connections are being created for Non-Keystore based TLS configurations.
     * 2. {@link SSLContext} if netty connections are being created for Keystore based TLS configurations. It will
     *    also create it for all web connections irrespective of it being Keystore or Non-Keystore based TLS
     *    configurations.
     *
     * @throws Exception if there are any issues creating the new {@link SSLContext} or {@link SslContext}
     */
    void createInternalSslContext() throws Exception;

    /**
     * Get the internally stored {@link SSLContext}. It will be used in the following scenarios:
     * 1. Netty connection creations for keystore based TLS configurations
     * 2. All Web connections
     *
     * @return {@link SSLContext}
     * @throws RuntimeException if the {@link SSLContext} object has not yet been initialized.
     */
    SSLContext getInternalSslContext();

    /**
     * Get the internally stored {@link SslContext}. It will be used to create Netty Connections for non-keystore based
     * tls configurations.
     *
     * @return {@link SslContext}
     * @throws RuntimeException if the {@link SslContext} object has not yet been initialized.
     */
    SslContext getInternalNettySslContext();

}