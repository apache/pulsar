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

// Pulsar BOM (Bill of Materials)
// Users import this to align all Pulsar module versions:
//   implementation(enforcedPlatform("org.apache.pulsar:pulsar-bom:X.Y.Z"))

plugins {
    `java-platform`
    id("pulsar.publish-conventions")
}

// Allow the platform to depend on other projects
javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        fun selectedApi(dependency: ProjectDependency) {
            if (!PulsarApiSpiPublication.isEnabled(project) ||
                dependency.path in PulsarApiSpiPublication.projects) {
                add("api", dependency)
            }
        }

        // Client API
        selectedApi(project(":pulsar-client-api"))
        selectedApi(project(":pulsar-client-admin-api"))
        selectedApi(project(":pulsar-client-api-v5"))

        // Focused SPI modules (PIP-478): TLS factory SPI + HTTP client SPI
        selectedApi(project(":pulsar-tls-factory-api"))
        selectedApi(project(":pulsar-http-client-api"))

        // Shaded clients (the published artifacts users depend on)
        selectedApi(project(":pulsar-client-shaded"))
        selectedApi(project(":pulsar-client-admin-shaded"))
        selectedApi(project(":pulsar-client-all"))
        selectedApi(project(":pulsar-client-v5-shaded"))

        // Combined unshaded v4/v5 client and admin
        selectedApi(project(":pulsar-client-v5-all"))

        // Original (unshaded) clients
        selectedApi(project(":pulsar-client-v5"))
        selectedApi(project(":pulsar-client-original"))
        selectedApi(project(":pulsar-client-admin-original"))

        // Client auth
        selectedApi(project(":pulsar-client-auth-sasl"))
        selectedApi(project(":pulsar-client-messagecrypto-bc"))

        // Common
        selectedApi(project(":pulsar-common"))
        selectedApi(project(":pulsar-config-validation"))

        // Functions API
        selectedApi(project(":pulsar-functions:pulsar-functions-api"))

        // IO core
        selectedApi(project(":pulsar-io:pulsar-io-core"))
        selectedApi(project(":pulsar-io:pulsar-io-common"))

        // Broker
        selectedApi(project(":pulsar-broker"))
        selectedApi(project(":pulsar-broker-common"))
        selectedApi(project(":pulsar-broker-auth-oidc"))
        selectedApi(project(":pulsar-broker-auth-sasl"))

        // Other core modules
        selectedApi(project(":managed-ledger"))
        selectedApi(project(":pulsar-metadata"))
        selectedApi(project(":pulsar-proxy"))
        selectedApi(project(":pulsar-websocket"))
        selectedApi(project(":pulsar-testclient"))
        selectedApi(project(":pulsar-cli-utils"))
        selectedApi(project(":pulsar-client-tools"))
        selectedApi(project(":pulsar-client-tools-api"))
        selectedApi(project(":pulsar-opentelemetry"))
        selectedApi(project(":testmocks"))

        // Transaction
        selectedApi(project(":pulsar-transaction:pulsar-transaction-common"))
        selectedApi(project(":pulsar-transaction:pulsar-transaction-coordinator"))

        // Functions
        selectedApi(project(":pulsar-functions:pulsar-functions-instance"))
        selectedApi(project(":pulsar-functions:pulsar-functions-runtime"))
        selectedApi(project(":pulsar-functions:pulsar-functions-worker"))
        selectedApi(project(":pulsar-functions:pulsar-functions-local-runner-original"))
        selectedApi(project(":pulsar-functions:pulsar-functions-proto"))
        selectedApi(project(":pulsar-functions:pulsar-functions-secrets"))
        selectedApi(project(":pulsar-functions:pulsar-functions-utils"))

        // Athenz auth
        selectedApi(project(":pulsar-client-auth-athenz"))
        selectedApi(project(":pulsar-broker-auth-athenz"))

        // Functions
        selectedApi(project(":pulsar-functions:pulsar-functions-local-runner-shaded"))

        // Tiered storage
        selectedApi(project(":tiered-storage:tiered-storage-jcloud"))
        selectedApi(project(":tiered-storage:tiered-storage-file-system"))
    }
}
