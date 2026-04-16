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
    id("pulsar.public-java-library-conventions")
    id("pulsar.test-certs-conventions")
    alias(libs.plugins.protobuf)
    alias(libs.plugins.lightproto)
}

dependencies {
    implementation(libs.slog)
    api(project(":managed-ledger"))
    api(project(":pulsar-broker-common"))
    implementation(project(":pulsar-client-original"))
    implementation(project(":pulsar-client-admin-original"))
    implementation(project(":pulsar-websocket"))
    implementation(project(":pulsar-cli-utils"))
    implementation(project(":pulsar-transaction:pulsar-transaction-common"))
    implementation(project(":pulsar-transaction:pulsar-transaction-coordinator"))
    implementation(project(":pulsar-opentelemetry"))
    implementation(project(":pulsar-client-messagecrypto-bc"))
    implementation(project(":pulsar-functions:pulsar-functions-worker"))
    implementation(project(":pulsar-docs-tools")) {
        exclude(group = "io.swagger")
    }
    implementation(project(":pulsar-package-management:pulsar-package-core"))
    implementation(project(":pulsar-package-management:pulsar-package-filesystem-storage"))

    implementation(libs.commons.codec)
    implementation(libs.commons.collections4)
    implementation(libs.commons.lang3)
    implementation(libs.netty.transport)
    implementation(libs.protobuf.java)
    implementation(libs.curator.recipes)
    implementation(libs.bookkeeper.stream.storage.server) {
        exclude(group = "org.apache.bookkeeper")
        exclude(group = "org.apache.distributedlog")
        exclude(group = "io.grpc")
    }
    implementation(libs.bookkeeper.tools.framework)
    implementation(libs.dropwizardmetrics.core)
    implementation(libs.snappy.java)
    implementation(libs.jetty.server)
    implementation(libs.jetty.alpn.conscrypt.server)
    implementation(libs.jetty.ee8.servlet)
    implementation(libs.jetty.ee8.servlets)
    implementation(libs.jersey.server)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.hk2)
    implementation(libs.jakarta.activation.api)
    implementation(libs.jackson.jaxrs.json.provider)
    implementation(libs.jackson.module.jsonSchema)
    implementation(libs.jcl.over.slf4j)
    implementation(libs.guava)
    implementation(libs.jspecify)
    implementation(libs.picocli)
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.hotspot)
    implementation(libs.simpleclient.caffeine)
    implementation(libs.hdrHistogram)
    implementation(libs.gson)
    implementation(libs.java.semver)
    implementation(libs.avro)
    implementation(libs.hppc)
    implementation(libs.roaringbitmap)
    implementation(libs.oshi.core)
    implementation(libs.jakarta.xml.bind.api)
    implementation(libs.jakarta.activation)
    implementation(libs.bookkeeper.server)
    implementation(libs.bookkeeper.circe.checksum)
    implementation(libs.caffeine)
    implementation(libs.sketches.core)
    implementation(libs.netty.codec.haproxy)
    implementation(libs.opentelemetry.sdk.extension.autoconfigure)
    implementation(libs.jetty.ee8.websocket.jetty.server)
    implementation(libs.jersey.media.multipart)
    implementation(libs.bookkeeper.stream.storage.java.client)
    implementation(libs.bookkeeper.stream.storage.service.api)
    implementation(libs.bookkeeper.stream.storage.service.impl)
    implementation(libs.jjwt.api)
    implementation(libs.jjwt.impl)
    implementation(project(":pulsar-functions:pulsar-functions-proto"))

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.swagger.core)
    compileOnly(libs.jsr305)

    testImplementation(project(":testmocks"))
    testImplementation(project(path = ":pulsar-broker-common", configuration = "testJar"))
    testImplementation(project(path = ":managed-ledger", configuration = "testJar"))
    testImplementation(project(path = ":pulsar-metadata", configuration = "testJar"))
    testImplementation(project(path = ":pulsar-package-management:pulsar-package-core", configuration = "testJar"))
    testImplementation(libs.bookkeeper.common) { artifact { classifier = "tests" } }
    testImplementation(libs.zookeeper) { artifact { classifier = "tests" } }
    testImplementation(project(":pulsar-functions:pulsar-functions-local-runner-original"))
    testImplementation(project(":pulsar-functions:pulsar-functions-api-examples"))
    testImplementation(project(":pulsar-io:pulsar-io-batch-discovery-triggerers"))
    testImplementation(libs.zt.zip)
    testImplementation(libs.asynchttpclient)
    testImplementation(libs.bcprov.ext.jdk18on)
    testImplementation(libs.commons.math3)
    testImplementation(libs.okhttp3)
    testImplementation(libs.spring.core)
    testImplementation(libs.vertx.core)
    testImplementation(libs.wiremock)
    testImplementation(libs.consolecaptor)
    testImplementation(libs.awaitility)
    testImplementation(libs.restassured)
    testImplementation(libs.jersey.test.framework.core)
    testImplementation(libs.jersey.test.framework.grizzly2)
    testImplementation(libs.jetty.ee8.proxy)
    testImplementation(libs.jetty.websocket.jetty.client)
    testImplementation(libs.opentelemetry.sdk.testing)
    testRuntimeOnly(libs.avro.protobuf) {
        exclude(group = "com.google.protobuf")
    }
}

// Ensure parent projects are configured before resolving cross-project task references.
// Required for --configure-on-demand: the Kotlin DSL needs parent ClassLoaderScopes to be locked.
evaluationDependsOn(":pulsar-io")
evaluationDependsOn(":pulsar-functions")

// NAR/JAR files needed by broker tests (mirrors Maven's maven-dependency-plugin config).
tasks.withType<Test> {
    dependsOn(
        ":pulsar-functions:pulsar-functions-api-examples:jar",
        ":pulsar-functions:pulsar-functions-api-examples-builtin:nar",
        ":pulsar-io:pulsar-io-data-generator:nar",
        ":pulsar-io:pulsar-io-batch-data-generator:nar",
    )
    doFirst {
        fun narPath(projectPath: String): String {
            val p = rootProject.project(projectPath)
            return p.tasks.named("nar").get().outputs.files.singleFile.absolutePath
        }
        fun jarPath(projectPath: String): String {
            val p = rootProject.project(projectPath)
            return p.tasks.named<Jar>("jar").get().archiveFile.get().asFile.absolutePath
        }
        val examplesJarPath = jarPath(":pulsar-functions:pulsar-functions-api-examples")
        systemProperty("pulsar-functions-api-examples.jar.path", examplesJarPath)
        systemProperty("pulsar-functions-api-examples.nar.path", narPath(":pulsar-functions:pulsar-functions-api-examples-builtin"))
        systemProperty("pulsar-io-data-generator.nar.path", narPath(":pulsar-io:pulsar-io-data-generator"))
        systemProperty("pulsar-io-batch-data-generator.nar.path", narPath(":pulsar-io:pulsar-io-batch-data-generator"))
        // A valid jar that is not a valid nar — used for invalid-nar tests
        systemProperty("pulsar-io-invalid.nar.path", examplesJarPath)
    }
}

protobuf {
    protoc {
        val protocVersion = providers.gradleProperty("protobufVersion").getOrElse(libs.versions.protobuf.get())
        artifact = "com.google.protobuf:protoc:$protocVersion"
    }
}

// All main proto files now use lightproto. Only test protos use standard protobuf.
sourceSets["main"].proto {
    exclude("TransactionPendingAck.proto")
    exclude("ResourceUsage.proto")
    exclude("DelayedMessageIndexBucketSegment.proto")
    exclude("SchemaRegistryFormat.proto")
    exclude("SchemaStorageFormat.proto")
    exclude("DelayedMessageIndexBucketMetadata.proto")
}

lightproto {
    // Test protos that need standard protobuf (GeneratedMessageV3), not lightproto
    excludes.addAll("ProtobufSchemaTest.proto", "DataRecord.proto")
    // TransactionPendingAck.proto imports PulsarApi.proto from pulsar-common
    extraProtoPaths.from(rootProject.layout.projectDirectory)
}
