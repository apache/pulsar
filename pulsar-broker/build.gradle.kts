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
    alias(libs.plugins.swagger)
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
        exclude(group = "io.swagger.core.v3")
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
    implementation(libs.jetty.ee10.servlet)
    implementation(libs.jetty.ee10.servlets)
    // ee8 + javax.servlet retained for the legacy AdditionalServlet javax.servlet path (PIP-472)
    implementation(libs.jetty.ee8.servlet)
    implementation(libs.javax.servlet.api)
    implementation(libs.jersey.server)
    implementation(libs.jersey.container.servlet.core)
    implementation(libs.jersey.container.servlet)
    implementation(libs.jersey.media.json.jackson)
    implementation(libs.jersey.hk2)
    implementation(libs.jakarta.activation.api)
    implementation(libs.jackson.jakarta.rs.json.provider)
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
    implementation(libs.angus.activation)
    implementation(libs.bookkeeper.server)
    implementation(libs.bookkeeper.circe.checksum)
    implementation(libs.caffeine)
    implementation(libs.datasketches.java)
    implementation(libs.netty.codec.haproxy)
    implementation(libs.opentelemetry.sdk.extension.autoconfigure)
    implementation(libs.jetty.ee10.websocket.jetty.server)
    implementation(libs.jersey.media.multipart)
    implementation(libs.bookkeeper.stream.storage.java.client)
    implementation(libs.bookkeeper.stream.storage.service.api)
    implementation(libs.bookkeeper.stream.storage.service.impl)
    implementation(libs.jjwt.api)
    implementation(libs.jjwt.impl)
    implementation(project(":pulsar-functions:pulsar-functions-proto"))

    compileOnly(libs.swagger.annotations)
    compileOnly(libs.jsr305)

    testImplementation(project(":testmocks"))
    testImplementation(project(path = ":pulsar-broker-common", configuration = "testJar"))
    testImplementation(project(path = ":managed-ledger", configuration = "testJar"))
    testImplementation(project(path = ":pulsar-metadata", configuration = "testJar"))
    testImplementation(project(path = ":pulsar-package-management:pulsar-package-core", configuration = "testJar"))
    testImplementation(libs.bookkeeper.common) { artifact { classifier = "tests" } }
    testImplementation(libs.zookeeper) { artifact { classifier = "tests" } }
    testImplementation(project(":pulsar-client-v5"))
    testImplementation(project(":pulsar-client-api-v5"))
    testImplementation(project(":pulsar-functions:pulsar-functions-local-runner-original"))
    testImplementation(project(":pulsar-functions:pulsar-functions-api-examples"))
    testImplementation(project(":pulsar-io:pulsar-io-batch-discovery-triggerers"))
    testImplementation(libs.zt.zip)
    testImplementation(libs.re2j)
    testImplementation(libs.asynchttpclient)
    testImplementation(libs.bcprov.jdk18on)
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
    testImplementation(libs.jetty.ee10.proxy)
    testImplementation(libs.jetty.websocket.jetty.client)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.oxia.testcontainers)
    testRuntimeOnly(libs.avro.protobuf) {
        exclude(group = "com.google.protobuf")
    }
}

// Ensure parent projects are configured before resolving cross-project task references.
// Required for --configure-on-demand: the Kotlin DSL needs parent ClassLoaderScopes to be locked.
evaluationDependsOn(":pulsar-io")
evaluationDependsOn(":pulsar-functions")

// NAR/JAR files needed by broker tests (mirrors Maven's maven-dependency-plugin config).
// Resolve through dependency configurations instead of cross-project task references.
val testNars by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
    attributes {
        attribute(org.gradle.api.artifacts.type.ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, "nar")
    }
}
val testExamplesJar by configurations.creating {
    isCanBeResolved = true
    isCanBeConsumed = false
}

dependencies {
    testNars(project(":pulsar-functions:pulsar-functions-api-examples-builtin"))
    testNars(project(":pulsar-io:pulsar-io-data-generator"))
    testNars(project(":pulsar-io:pulsar-io-batch-data-generator"))
    testExamplesJar(project(":pulsar-functions:pulsar-functions-api-examples"))
}

tasks.withType<Test> {
    dependsOn(testNars, testExamplesJar)
    val narFiles = testNars.incoming.artifacts.resolvedArtifacts
    val jarFiles = testExamplesJar.incoming.artifacts.resolvedArtifacts
    doFirst {
        val narMap = narFiles.get().associate {
            val id = it.id.componentIdentifier as org.gradle.api.artifacts.component.ProjectComponentIdentifier
            id.projectPath to it.file.absolutePath
        }
        val examplesJarPath = jarFiles.get().first().file.absolutePath
        systemProperty("pulsar-functions-api-examples.jar.path", examplesJarPath)
        systemProperty("pulsar-functions-api-examples.nar.path",
            narMap[":pulsar-functions:pulsar-functions-api-examples-builtin"]!!)
        systemProperty("pulsar-io-data-generator.nar.path",
            narMap[":pulsar-io:pulsar-io-data-generator"]!!)
        systemProperty("pulsar-io-batch-data-generator.nar.path",
            narMap[":pulsar-io:pulsar-io-batch-data-generator"]!!)
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

// ── OpenAPI (Swagger) REST API documentation ────────────────────────────────
// Mirrors the Maven build's `swagger` profile (kongchen swagger-maven-plugin, Swagger 1.x),
// ported to the official Swagger Core v3 gradle plugin. Run on demand:
//   ./gradlew :pulsar-broker:generateOpenApiSpecs   (outputs to pulsar-broker/build/openapi/)
// The plugin's default `swaggerDeps` resolver dependencies target javax.ws.rs; declaring
// our own dependencies on the configuration replaces them with the jakarta variants.
dependencies {
    // The component metadata rule in pulsar.java-conventions replaces
    // com.sun.activation:jakarta.activation with versionless jakarta.activation-api/
    // angus-activation deps, so swaggerDeps needs the platform to pin their versions.
    "swaggerDeps"(enforcedPlatform(project(":pulsar-dependencies")))
    "swaggerDeps"(libs.commons.lang3)
    "swaggerDeps"(libs.swagger.jaxrs2)
    "swaggerDeps"(libs.jakarta.ws.rs.api)
    "swaggerDeps"(libs.jakarta.servlet.api)
}

fun registerSwaggerTask(
    name: String,
    fileName: String,
    baseInfoFile: String,
    configure: io.swagger.v3.plugins.gradle.tasks.ResolveTask.() -> Unit,
) = tasks.register<io.swagger.v3.plugins.gradle.tasks.ResolveTask>(name) {
    group = "documentation"
    description = "Generates $fileName.json OpenAPI documentation"
    buildClasspath.setFrom(configurations["swaggerDeps"])
    classpath.setFrom(sourceSets["main"].runtimeClasspath)
    outputDir.set(layout.buildDirectory.dir("swagger/$name"))
    outputFileName.set(fileName)
    outputFormat.set(io.swagger.v3.plugins.gradle.tasks.ResolveTask.Format.JSON)
    openApiFile.set(file("src/main/openapi/$baseInfoFile"))
    prettyPrint.set(true)
    sortOutput.set(true)
    readAllResources.set(true)
    configure()
}

registerSwaggerTask("swaggerAdminV2", "swagger", "admin-v2.json") {
    resourceClasses.set(setOf(
        "org.apache.pulsar.broker.admin.v2.Bookies",
        "org.apache.pulsar.broker.admin.v2.BrokerStats",
        "org.apache.pulsar.broker.admin.v2.Brokers",
        "org.apache.pulsar.broker.admin.v2.Clusters",
        "org.apache.pulsar.broker.admin.v2.Functions",
        "org.apache.pulsar.broker.admin.v2.MetadataMigration",
        "org.apache.pulsar.broker.admin.v2.Namespaces",
        "org.apache.pulsar.broker.admin.v2.NonPersistentTopics",
        "org.apache.pulsar.broker.admin.v2.PersistentTopics",
        "org.apache.pulsar.broker.admin.v2.ResourceGroups",
        "org.apache.pulsar.broker.admin.v2.ResourceQuotas",
        "org.apache.pulsar.broker.admin.v2.ScalableTopics",
        "org.apache.pulsar.broker.admin.v2.SchemasResource",
        "org.apache.pulsar.broker.admin.v2.Segments",
        "org.apache.pulsar.broker.admin.v2.Tenants",
        "org.apache.pulsar.broker.admin.v2.Worker",
        "org.apache.pulsar.broker.admin.v2.WorkerStats",
    ))
}

registerSwaggerTask("swaggerLookup", "swaggerlookup", "lookup-v2.json") {
    resourcePackages.set(setOf("org.apache.pulsar.broker.lookup.v2"))
}

registerSwaggerTask("swaggerFunctions", "swaggerfunctions", "functions-v3.json") {
    resourceClasses.set(setOf("org.apache.pulsar.broker.admin.v3.Functions"))
}

registerSwaggerTask("swaggerTransactions", "swaggertransactions", "transactions-v3.json") {
    resourceClasses.set(setOf("org.apache.pulsar.broker.admin.v3.Transactions"))
}

registerSwaggerTask("swaggerSource", "swaggersource", "source-v3.json") {
    resourceClasses.set(setOf("org.apache.pulsar.broker.admin.v3.Sources"))
}

registerSwaggerTask("swaggerSink", "swaggersink", "sink-v3.json") {
    resourceClasses.set(setOf("org.apache.pulsar.broker.admin.v3.Sinks"))
}

registerSwaggerTask("swaggerPackages", "swaggerpackages", "packages-v3.json") {
    resourceClasses.set(setOf("org.apache.pulsar.broker.admin.v3.Packages"))
}

// Assemble the documentation set in the layout published on pulsar.apache.org (see e.g.
// pulsar-site static/swagger/<version>/): all files flat, plus v2/ and v3/ subdirectory copies
// grouped by REST API version.
tasks.register<Sync>("generateOpenApiSpecs") {
    group = "documentation"
    description = "Generates all OpenAPI REST API documentation files to build/openapi"
    into(layout.buildDirectory.dir("openapi"))
    val v2Tasks = listOf("swaggerAdminV2", "swaggerLookup")
    val v3Tasks = listOf("swaggerFunctions", "swaggerTransactions", "swaggerSource", "swaggerSink", "swaggerPackages")
    v2Tasks.forEach { t ->
        from(tasks.named(t))
        from(tasks.named(t)) { into("v2") }
    }
    v3Tasks.forEach { t ->
        from(tasks.named(t))
        from(tasks.named(t)) { into("v3") }
    }
}
