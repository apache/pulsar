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

import org.gradle.api.Project

/** API/SPI publication set for Java clients, Functions and plugins, including their published dependencies. */
object PulsarApiSpiPublication {
    val projects: Set<String> = setOf(
        ":",
        ":buildtools",
        ":managed-ledger",
        ":pulsar-bom",
        ":pulsar-broker",
        ":pulsar-broker-auth-sasl",
        ":pulsar-broker-common",
        ":pulsar-cli-utils",
        ":pulsar-client-admin-api",
        ":pulsar-client-admin-original",
        ":pulsar-client-admin-shaded",
        ":pulsar-client-all",
        ":pulsar-client-api",
        ":pulsar-client-api-v5",
        ":pulsar-client-auth-sasl",
        ":pulsar-client-fastutil-minimized",
        ":pulsar-client-messagecrypto-bc",
        ":pulsar-client-original",
        ":pulsar-client-shaded",
        ":pulsar-client-v5",
        ":pulsar-client-v5-all",
        ":pulsar-client-v5-shaded",
        ":pulsar-common",
        ":pulsar-config-validation",
        ":pulsar-dependencies",
        ":pulsar-docs-tools",
        ":pulsar-functions:pulsar-functions-api",
        ":pulsar-functions:pulsar-functions-instance",
        ":pulsar-functions:pulsar-functions-proto",
        ":pulsar-functions:pulsar-functions-runtime",
        ":pulsar-functions:pulsar-functions-secrets",
        ":pulsar-functions:pulsar-functions-utils",
        ":pulsar-functions:pulsar-functions-worker",
        ":pulsar-http-client-api",
        ":pulsar-io:pulsar-io-core",
        ":pulsar-metadata",
        ":pulsar-opentelemetry",
        ":pulsar-package-management:pulsar-package-core",
        ":pulsar-package-management:pulsar-package-filesystem-storage",
        ":pulsar-proxy",
        ":pulsar-tls-factory-api",
        ":pulsar-transaction:pulsar-transaction-common",
        ":pulsar-transaction:pulsar-transaction-coordinator",
        ":pulsar-websocket",
        ":testmocks",
    )

    fun isEnabled(project: Project): Boolean =
        project.providers.gradleProperty("publishApiAndSpiOnly").getOrElse("false").toBoolean()

    fun includes(project: Project): Boolean = !isEnabled(project) || project.path in projects
}
