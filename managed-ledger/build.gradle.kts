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

import io.streamnative.lightproto.gradle.GenerateLightProtoTask

plugins {
    id("pulsar.public-java-library-conventions")
    alias(libs.plugins.lightproto)
}

// LightProto emits the publication getter from the schema but cannot declare an additional Java interface.
tasks.named<GenerateLightProtoTask>("generateLightProto") {
    val cursorSource = outputDirectory.file("org/apache/bookkeeper/mledger/proto/ManagedCursorInfo.java")
    doLast {
        val file = cursorSource.get().asFile
        val source = file.readText()
        val declaration = "public final class ManagedCursorInfo implements "
        check(declaration in source) { "Unrecognized LightProto declaration in ${file.name}" }
        file.writeText(source.replace(declaration, "import org.apache.pulsar.metadata.api.HasPublicationId;\n\n" +
                "${declaration}HasPublicationId, "))
    }
}

dependencies {
    api(project(":pulsar-common"))
    api(project(":pulsar-metadata"))
    implementation(project(":pulsar-opentelemetry"))
    api(libs.bookkeeper.server)
    api(libs.guava)
    implementation(libs.fastutil)
    implementation(libs.roaringbitmap)
    implementation(libs.jctools.core.jdk11)
    api(libs.slog)
    api(libs.prometheus.metrics.core)
    api(libs.commons.lang3)

    testImplementation(project(":testmocks"))
    testImplementation(libs.dropwizardmetrics.core)
    testImplementation(libs.snappy.java)
    testImplementation(libs.awaitility)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.opentelemetry.sdk.extension.autoconfigure)
    testImplementation(libs.zookeeper) { artifact { classifier = "tests" } }
}
