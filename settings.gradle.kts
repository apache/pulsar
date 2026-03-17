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

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("com.gradle.develocity") version "3.19"
}

develocity {
    server = "https://develocity.apache.org"
    buildScan {
        termsOfUseUrl = "https://gradle.com/help/legal-terms-of-use"
        termsOfUseAgree = "yes"
        publishing.onlyIf { false } // opt-in via --scan
    }
}

rootProject.name = "pulsar"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

// ──────────────────────────────────────────────────────────────
// Module includes
// ──────────────────────────────────────────────────────────────

// Core infrastructure
include(":buildtools")
include(":testmocks")

// Common libraries
include(":pulsar-common")
include(":pulsar-broker-common")
include(":pulsar-client-api")
include(":pulsar-client-admin-api")
include(":pulsar-cli-utils")
include(":pulsar-config-validation")

// Storage
include(":managed-ledger")
include(":pulsar-metadata")

// Broker
include(":pulsar-broker")

// Client
include(":pulsar-client")
project(":pulsar-client").projectDir = file("pulsar-client")
// The Maven artifactId is "pulsar-client-original" but the directory is "pulsar-client"

include(":pulsar-client-admin")
project(":pulsar-client-admin").projectDir = file("pulsar-client-admin")

// Shaded clients
include(":pulsar-client-shaded")
include(":pulsar-client-admin-shaded")
include(":pulsar-client-all")
include(":pulsar-client-dependencies-minimized")

// Client tools
include(":pulsar-client-tools-api")
include(":pulsar-client-tools")
include(":pulsar-client-tools-customcommand-example")
include(":pulsar-client-tools-test")

// Network services
include(":pulsar-websocket")
include(":pulsar-proxy")
include(":pulsar-testclient")
include(":pulsar-docs-tools")

// Auth
include(":pulsar-broker-auth-athenz")
include(":pulsar-client-auth-athenz")
include(":pulsar-broker-auth-oidc")
include(":pulsar-broker-auth-sasl")
include(":pulsar-client-auth-sasl")

// Observability
include(":pulsar-opentelemetry")
include(":structured-event-log")

// Crypto
include(":bouncy-castle:bc")
include(":bouncy-castle:bcfips")
include(":bouncy-castle:bcfips-include-test")
include(":pulsar-client-messagecrypto-bc")

// Transactions
include(":pulsar-transaction:common")
include(":pulsar-transaction:coordinator")

// Functions
include(":pulsar-functions:proto")
include(":pulsar-functions:api-java")
include(":pulsar-functions:java-examples")
include(":pulsar-functions:java-examples-builtin")
include(":pulsar-functions:utils")
include(":pulsar-functions:instance")
include(":pulsar-functions:runtime")
include(":pulsar-functions:runtime-all")
include(":pulsar-functions:worker")
include(":pulsar-functions:secrets")
include(":pulsar-functions:localrun")
include(":pulsar-functions:localrun-shaded")

// Connectors (IO)
include(":pulsar-io-core")
project(":pulsar-io-core").projectDir = file("pulsar-io/core")
include(":pulsar-io:common")
include(":pulsar-io:docs")
include(":pulsar-io:aws")
include(":pulsar-io:batch-discovery-triggerers")
include(":pulsar-io:batch-data-generator")
include(":pulsar-io:azure-data-explorer")
include(":pulsar-io:cassandra")
include(":pulsar-io:aerospike")
include(":pulsar-io:http")
include(":pulsar-io:kafka")
include(":pulsar-io:rabbitmq")
include(":pulsar-io:kinesis")
include(":pulsar-io:kinesis-kpl-shaded")
include(":pulsar-io:hdfs3")
include(":pulsar-io:jdbc-core")
project(":pulsar-io:jdbc-core").projectDir = file("pulsar-io/jdbc/core")
include(":pulsar-io:jdbc-clickhouse")
project(":pulsar-io:jdbc-clickhouse").projectDir = file("pulsar-io/jdbc/clickhouse")
include(":pulsar-io:jdbc-mariadb")
project(":pulsar-io:jdbc-mariadb").projectDir = file("pulsar-io/jdbc/mariadb")
include(":pulsar-io:jdbc-openmldb")
project(":pulsar-io:jdbc-openmldb").projectDir = file("pulsar-io/jdbc/openmldb")
include(":pulsar-io:jdbc-postgres")
project(":pulsar-io:jdbc-postgres").projectDir = file("pulsar-io/jdbc/postgres")
include(":pulsar-io:jdbc-sqlite")
project(":pulsar-io:jdbc-sqlite").projectDir = file("pulsar-io/jdbc/sqlite")
include(":pulsar-io:data-generator")
include(":pulsar-io:elastic-search")
include(":pulsar-io:kafka-connect-adaptor")
include(":pulsar-io:kafka-connect-adaptor-nar")
include(":pulsar-io:debezium-core")
project(":pulsar-io:debezium-core").projectDir = file("pulsar-io/debezium/core")
include(":pulsar-io:debezium-mongodb")
project(":pulsar-io:debezium-mongodb").projectDir = file("pulsar-io/debezium/mongodb")
include(":pulsar-io:debezium-mssql")
project(":pulsar-io:debezium-mssql").projectDir = file("pulsar-io/debezium/mssql")
include(":pulsar-io:debezium-mysql")
project(":pulsar-io:debezium-mysql").projectDir = file("pulsar-io/debezium/mysql")
include(":pulsar-io:debezium-oracle")
project(":pulsar-io:debezium-oracle").projectDir = file("pulsar-io/debezium/oracle")
include(":pulsar-io:debezium-postgres")
project(":pulsar-io:debezium-postgres").projectDir = file("pulsar-io/debezium/postgres")
include(":pulsar-io:canal")
include(":pulsar-io:file")
include(":pulsar-io:netty")
include(":pulsar-io:hbase")
include(":pulsar-io:mongo")
include(":pulsar-io:redis")
include(":pulsar-io:solr")
include(":pulsar-io:influxdb")
include(":pulsar-io:dynamodb")
include(":pulsar-io:nsq")
include(":pulsar-io:alluxio")
include(":pulsar-io:twitter")
include(":pulsar-io:flume")

// Shaded dependencies
include(":jetcd-core-shaded")
include(":jclouds-shaded")

// Package management
include(":pulsar-package-management:core")
include(":pulsar-package-management:bookkeeper-storage")
include(":pulsar-package-management:filesystem-storage")

// Tiered storage
include(":tiered-storage:jcloud")
include(":tiered-storage:file-system")

// Distribution
include(":distribution:server")
include(":distribution:shell")
include(":distribution:io")
include(":distribution:offloaders")

// BOM
include(":pulsar-bom")

// Misc
include(":jetty-upgrade")
include(":microbench")

// ──────────────────────────────────────────────────────────────
// Conditional modules (equivalent to Maven profiles)
// ──────────────────────────────────────────────────────────────

if (providers.gradleProperty("docker").isPresent) {
    include(":docker")
}

if (providers.gradleProperty("integrationTests").isPresent) {
    include(":tests")
}
