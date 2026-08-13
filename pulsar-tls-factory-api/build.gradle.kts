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

// PIP-478: focused, dependency-light API module hosting the purpose-driven TLS factory SPI
// (org.apache.pulsar.tls). Published so that server-side modules (pulsar-common, broker-common,
// proxy, ...) and the v5 client may depend on it in later stages — the published-module dependency
// guard only allows published-on-published deps.
plugins {
    id("pulsar.public-java-library-conventions")
}

dependencies {
    // TlsFactoryInitContext exposes OpenTelemetry on the SPI surface, so it is an api dependency:
    // an external factory implementation depending only on this module must resolve OpenTelemetry.
    // The Netty/Jetty well-known TLS classes appear only as documented Class<T> values referenced in
    // plain-text javadoc, so no netty/jetty dependency is needed.
    api(libs.opentelemetry.api)
}
