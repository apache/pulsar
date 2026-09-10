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

// PIP-478: focused, dependency-light API module hosting the framework-managed HTTP client SPI
// (org.apache.pulsar.http). Published so that server-side modules and the v5 client may depend on it
// in later stages — the published-module dependency guard only allows published-on-published deps.
plugins {
    id("pulsar.public-java-library-conventions")
}

dependencies {
    // PulsarHttpClientConfig selects TLS material by TlsPurpose, which is part of this SPI's surface;
    // exposed as `api` so consumers of PulsarHttpClientConfig see the TLS factory SPI.
    api(project(":pulsar-tls-factory-api"))
}
