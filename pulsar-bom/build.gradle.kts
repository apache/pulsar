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
        // Client API
        api(project(":pulsar-client-api"))
        api(project(":pulsar-client-admin-api"))

        // Shaded clients (the published artifacts users depend on)
        api(project(":pulsar-client-shaded"))
        api(project(":pulsar-client-admin-shaded"))
        api(project(":pulsar-client-all"))

        // Original (unshaded) clients
        api(project(":pulsar-client-original"))
        api(project(":pulsar-client-admin-original"))

        // Client auth
        api(project(":pulsar-client-auth-sasl"))
        api(project(":pulsar-client-messagecrypto-bc"))

        // Common
        api(project(":pulsar-common"))
        api(project(":pulsar-config-validation"))

        // Functions API
        api(project(":pulsar-functions:pulsar-functions-api"))

        // IO core
        api(project(":pulsar-io:pulsar-io-core"))
        api(project(":pulsar-io:pulsar-io-common"))

        // Broker
        api(project(":pulsar-broker"))
        api(project(":pulsar-broker-common"))
        api(project(":pulsar-broker-auth-oidc"))
        api(project(":pulsar-broker-auth-sasl"))

        // Other core modules
        api(project(":managed-ledger"))
        api(project(":pulsar-metadata"))
        api(project(":pulsar-proxy"))
        api(project(":pulsar-websocket"))
        api(project(":pulsar-testclient"))
        api(project(":pulsar-cli-utils"))
        api(project(":pulsar-client-tools"))
        api(project(":pulsar-client-tools-api"))
        api(project(":pulsar-opentelemetry"))
        api(project(":testmocks"))

        // Transaction
        api(project(":pulsar-transaction:pulsar-transaction-common"))
        api(project(":pulsar-transaction:pulsar-transaction-coordinator"))

        // Functions
        api(project(":pulsar-functions:pulsar-functions-instance"))
        api(project(":pulsar-functions:pulsar-functions-runtime"))
        api(project(":pulsar-functions:pulsar-functions-worker"))
        api(project(":pulsar-functions:pulsar-functions-local-runner-original"))
        api(project(":pulsar-functions:pulsar-functions-proto"))
        api(project(":pulsar-functions:pulsar-functions-secrets"))
        api(project(":pulsar-functions:pulsar-functions-utils"))

        // Bouncy Castle
        api(project(":bouncy-castle:bouncy-castle-bc"))
        api(project(":bouncy-castle:bcfips"))

        // Athenz auth
        api(project(":pulsar-client-auth-athenz"))
        api(project(":pulsar-broker-auth-athenz"))

        // Functions
        api(project(":pulsar-functions:pulsar-functions-local-runner-shaded"))

        // Tiered storage
        api(project(":tiered-storage:tiered-storage-jcloud"))
        api(project(":tiered-storage:tiered-storage-file-system"))
    }
}
