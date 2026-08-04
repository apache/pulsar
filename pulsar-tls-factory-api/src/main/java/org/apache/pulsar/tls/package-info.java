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
 * The purpose-driven TLS SPI that replaces PIP-337's SSL factory (PIP-478).
 *
 * <p>A {@link org.apache.pulsar.tls.PulsarTlsFactory} answers requests for fully configured TLS
 * objects ({@code io.netty.handler.ssl.SslContext}, Jetty's {@code SslContextFactory.Server}, or
 * {@code javax.net.ssl.SSLContext}) per {@link org.apache.pulsar.tls.TlsPurpose}, delivering
 * rebuilt instances through reload callbacks on rotation. A factory that supplies only the
 * {@code SSLContext} fallback may additionally supply a {@code javax.net.ssl.SSLParameters} companion —
 * the optional engine-policy baseline (protocols, cipher suites, client-auth mode, endpoint
 * identification) consulted on the synthesis path with a deterministic merge order documented on
 * {@link org.apache.pulsar.tls.PulsarTlsFactory}. How the factory sources key material and builds
 * the objects is entirely factory-internal — nothing material-shaped appears in the SPI and key material
 * never crosses a Pulsar API. The single user-facing configuration value is
 * {@link org.apache.pulsar.tls.TlsPolicy}.
 *
 * <p>This SPI is hosted in the focused, dependency-light {@code pulsar-tls-factory-api} module so both
 * the v5 client builder and the server-side components (and the sibling broker-side PIP) can consume
 * the same SPI without dragging in heavyweight dependencies. The default {@code PulsarTlsFactory}
 * implementation ({@code FileBasedTlsFactory} under {@code org.apache.pulsar.common.tls.impl}) and the
 * JDK-only hostname-verification helpers ship in {@code pulsar-common}, not in this module.
 */
package org.apache.pulsar.tls;
