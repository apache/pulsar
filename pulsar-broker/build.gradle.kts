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

plugins {
    id("pulsar.java-conventions")
    id("pulsar.test-jar")
    id("com.google.protobuf")
    id("pulsar.lightproto")
}

description = "Pulsar Broker"

// Only process specific proto files with lightproto (others go to protoc)
ext["lightprotoIncludes"] = listOf(
    "${projectDir}/src/main/proto/TransactionPendingAck.proto",
    "${projectDir}/src/main/proto/ResourceUsage.proto",
    "${projectDir}/src/main/proto/DelayedMessageIndexBucketSegment.proto",
).joinToString(",")

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${libs.versions.protobuf3.get()}"
    }
// In Maven, TransactionPendingAck.proto, ResourceUsage.proto, and DelayedMessageIndexBucketSegment.proto
// are excluded from protoc and compiled by lightproto instead.
// The remaining proto files (SchemaRegistryFormat, SchemaStorageFormat, DelayedMessageIndexBucketMetadata)
// don't import PulsarApi.proto and compile standalone.
sourceSets {
    main {
        proto {
            exclude("**/TransactionPendingAck.proto")
            exclude("**/ResourceUsage.proto")
            exclude("**/DelayedMessageIndexBucketSegment.proto")
        }
    }
}

dependencies {
    // Pulsar modules
    api(project(":pulsar-broker-common"))
    api(project(":managed-ledger"))
    api(project(":pulsar-client"))
    api(project(":pulsar-client-admin"))
    api(project(":pulsar-websocket"))
    api(project(":pulsar-cli-utils"))
    api(project(":pulsar-transaction:common"))
    api(project(":pulsar-transaction:coordinator"))
    api(project(":pulsar-opentelemetry"))
    api(project(":pulsar-docs-tools"))
    api(project(":pulsar-package-management:core"))

    implementation(project(":pulsar-functions:worker"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(project(":pulsar-package-management:filesystem-storage"))

    // Commons
    implementation(libs.commons.codec)
    implementation(libs.commons.collections4)
    implementation(libs.commons.lang3)

    // Logging
    implementation(libs.slf4j.api)

    // Netty
    implementation(libs.netty.transport)

    // Protobuf
    implementation(libs.protobuf.java)

    // Collections
    implementation(libs.fastutil)

    // BookKeeper
    implementation(libs.curator.recipes) {
        exclude(group = "org.apache.zookeeper")
    }
    implementation(libs.bookkeeper.stream.storage.server) {
        exclude(group = "io.grpc", module = "grpc-all")
        exclude(group = "io.grpc", module = "grpc-okhttp")
        exclude(group = "com.squareup.okhttp", module = "okhttp")
        exclude(group = "com.squareup.okio", module = "okio")
        exclude(group = "org.codehaus.jackson", module = "jackson-mapper-asl")
        exclude(group = "org.inferred", module = "freebuilder")
    }
    implementation(libs.bookkeeper.tools.framework)

    // Jetty
    implementation(libs.jetty.server)
    implementation("org.eclipse.jetty:jetty-alpn-conscrypt-server:${libs.versions.jetty.get()}")
    implementation(libs.jetty.ee8.servlet)
    implementation("org.eclipse.jetty.ee8:jetty-ee8-servlets:${libs.versions.jetty.get()}")

    // Jersey
    implementation(libs.jersey.server)
    implementation("org.glassfish.jersey.containers:jersey-container-servlet-core:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.containers:jersey-container-servlet:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.media:jersey-media-json-jackson:${libs.versions.jersey.get()}")
    implementation("org.glassfish.jersey.inject:jersey-hk2:${libs.versions.jersey.get()}")

    // Metrics
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.servlet)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.prometheus.jmx.collector)

    // Jackson
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation(libs.jackson.module.jsonSchema)
    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${libs.versions.jackson.get()}")

    // Swagger
    compileOnly(libs.swagger.core)
    compileOnly(libs.swagger.annotations)

    // Security
    implementation(libs.conscrypt.openjdk.uber)
    implementation(libs.javax.servlet.api)

    // Misc
    implementation(libs.guava)
    implementation(libs.byte.buddy)
    implementation(libs.jspecify)
    implementation(libs.picocli)
    implementation(libs.hdrHistogram)
    implementation(libs.zt.zip)
    implementation(libs.reflections)
    implementation(libs.cron.utils) {
        exclude(group = "org.glassfish", module = "javax.el")
    }
    implementation(libs.gson)
    implementation(libs.java.semver)
    implementation(libs.avro)
    implementation(libs.hppc)
    implementation(libs.roaringbitmap)
    implementation(libs.oshi)
    implementation(libs.snappy.java)
    implementation(libs.dropwizard.metrics.core)
    implementation(libs.jcl.over.slf4j)
    implementation("jakarta.activation:jakarta.activation-api:${libs.versions.jakarta.activation.get()}")
    implementation("jakarta.xml.bind:jakarta.xml.bind-api:${libs.versions.jakarta.xml.bind.get()}")
    runtimeOnly("com.sun.activation:jakarta.activation:${libs.versions.jakarta.activation.get()}")

    // Caching
    implementation(libs.caffeine)

    // JWT
    implementation(libs.jjwt.impl)
    implementation(libs.jjwt.jackson)

    // Netty
    implementation(libs.netty.codec.haproxy)

    // Websocket
    implementation("org.eclipse.jetty.ee8.websocket:jetty-ee8-websocket-jetty-server:${libs.versions.jetty.get()}")

    // Jersey multipart
    implementation("org.glassfish.jersey.media:jersey-media-multipart:${libs.versions.jersey.get()}")

    // OTel
    implementation(libs.opentelemetry.sdk)
    implementation("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure:1.56.0")

    // Sketches
    implementation(libs.sketches.core)

    // Compression
    implementation(libs.commons.compress)

    // Test
    testImplementation(project(":pulsar-io:batch-discovery-triggerers"))
    testImplementation(project(":pulsar-functions:localrun"))
    testImplementation(project(":testmocks"))
    testImplementation(project(":buildtools"))
    testImplementation(project(path = ":pulsar-broker-common", configuration = "testArtifacts"))
    testImplementation(libs.wiremock)
    testImplementation(libs.consolecaptor)
    testImplementation(libs.oxia.testcontainers)
    testImplementation("org.eclipse.jetty.websocket:jetty-websocket-jetty-client:${libs.versions.jetty.get()}")
    testImplementation("org.eclipse.jetty.ee8:jetty-ee8-proxy:${libs.versions.jetty.get()}")
    testImplementation(libs.awaitility)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.restassured)
    testImplementation(libs.grpc.netty.shaded)
    testImplementation(libs.jetcd.test)
    testImplementation("org.glassfish.jersey.test-framework:jersey-test-framework-core:${libs.versions.jersey.get()}")
    testImplementation("org.glassfish.jersey.test-framework.providers:jersey-test-framework-provider-grizzly2:${libs.versions.jersey.get()}")
    testImplementation(libs.netty.codec.socks)
    testImplementation(libs.vertx.core)
    testImplementation(libs.okhttp3)
    testImplementation(libs.async.http.client)
    testImplementation(libs.bcprov.jdk18on)
    testImplementation(libs.system.lambda)
    testImplementation("org.springframework:spring-core:${libs.versions.spring.get()}")
    testImplementation(project(":pulsar-io-core"))
    testImplementation(project(":pulsar-functions:java-examples"))
    testImplementation(project(":pulsar-io:batch-data-generator"))
    testImplementation(project(":pulsar-io:data-generator"))
    testImplementation(project(path = ":pulsar-metadata", configuration = "testArtifacts"))
    testImplementation("org.apache.commons:commons-math3:3.6.1")
    testImplementation("org.apache.bookkeeper:bookkeeper-common:${libs.versions.bookkeeper.get()}:tests")
    testImplementation(project(path = ":managed-ledger", configuration = "testArtifacts"))
    testImplementation(project(path = ":pulsar-transaction:coordinator", configuration = "testArtifacts"))
    testImplementation(project(path = ":pulsar-package-management:core", configuration = "testArtifacts"))
}
}
