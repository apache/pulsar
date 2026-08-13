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
 * JDK-only TLS helper classes.
 *
 * <p>This package hosts small TLS support classes ({@link org.apache.pulsar.common.tls.InetAddressUtils},
 * {@link org.apache.pulsar.common.tls.NoopHostnameVerifier}, and friends) used by the client and server TLS
 * stacks. They carry no dependency beyond the JDK and slog.
 *
 * <p>Hostname verification is delegated to the JDK/provider standard endpoint identification
 * ({@code endpointIdentificationAlgorithm = "HTTPS"}). Pulsar's own CN-based hostname verifier was removed in
 * 5.0 (PIP-478), so verification is whatever the configured provider implements. Note that the standard
 * algorithm is not SAN-only: per RFC 6125 the JDK and OpenSSL engines consult the CN when a certificate
 * carries no {@code dNSName} SAN, and ignore it once any is present. Conscrypt does not fall back to the CN;
 * pinning it is what makes verification strictly SAN-based.
 *
 * <p>The purpose-driven TLS SPI ({@code PulsarTlsFactory} and companions) lives in its own focused module
 * under {@link org.apache.pulsar.tls}; the default {@code FileBasedTlsFactory} implementation lives in the
 * sibling {@code org.apache.pulsar.common.tls.impl} package.
 */
package org.apache.pulsar.common.tls;
