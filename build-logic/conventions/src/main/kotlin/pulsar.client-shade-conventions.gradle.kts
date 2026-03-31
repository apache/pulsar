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

// Convention plugin for Pulsar client shaded modules (pulsar-client-shaded,
// pulsar-client-admin-shaded, pulsar-client-all). Configures the shadow jar
// with the shared dependency includes, file excludes, relocations, and
// filesMatching blocks. Modules only need to define their project dependencies.

plugins {
    id("pulsar.shadow-conventions")
}

val shadePrefix = "org.apache.pulsar.shade"
extra["shadePrefix"] = shadePrefix

tasks.named<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar>("shadowJar") {

    // ---- Dependency includes ----
    dependencies {
        include(project(":pulsar-client-original"))
        include(project(":pulsar-client-admin-original"))
        include(project(":pulsar-common"))
        include(project(":pulsar-client-messagecrypto-bc"))
        include(dependency("com.fasterxml.jackson.*:.*"))
        include(dependency("com.google.*:.*"))
        include(dependency("com.google.auth:.*"))
        include(dependency("com.google.code.findbugs:.*"))
        include(dependency("com.google.code.gson:gson"))
        include(dependency("com.google.errorprone:.*"))
        include(dependency("com.google.guava:.*"))
        include(dependency("com.google.j2objc:.*"))
        include(dependency("com.google.re2j:re2j"))
        include(dependency("com.spotify:completable-futures"))
        include(dependency("com.squareup.*:.*"))
        include(dependency("com.sun.activation:jakarta.activation"))
        include(dependency("com.thoughtworks.paranamer:paranamer"))
        include(dependency("com.typesafe.netty:netty-reactive-streams"))
        include(dependency("com.yahoo.datasketches:.*"))
        include(dependency("commons-.*:.*"))
        include(dependency("io.airlift:.*"))
        include(dependency("io.grpc:.*"))
        include(dependency("io.netty.incubator:.*"))
        include(dependency("io.netty:.*"))
        include(dependency("io.opencensus:.*"))
        include(dependency("io.perfmark:.*"))
        include(dependency("io.prometheus:.*"))
        include(dependency("io.swagger:.*"))
        include(dependency("jakarta.activation:jakarta.activation-api"))
        include(dependency("jakarta.annotation:jakarta.annotation-api"))
        include(dependency("jakarta.ws.rs:jakarta.ws.rs-api"))
        include(dependency("jakarta.xml.bind:jakarta.xml.bind-api"))
        include(dependency("javax.ws.rs:.*"))
        include(dependency("javax.xml.bind:jaxb-api"))
        include(dependency("org.apache.avro:.*"))
        include(dependency("org.apache.bookkeeper:.*"))
        include(dependency("org.apache.commons:commons-compress"))
        include(dependency("org.apache.commons:commons-lang3"))
        include(dependency("org.asynchttpclient:.*"))
        include(dependency("org.checkerframework:.*"))
        include(dependency("org.eclipse.jetty:.*"))
        include(dependency("org.glassfish.hk2.*:.*"))
        include(dependency("org.glassfish.jersey.*:.*"))
        include(dependency("org.javassist:javassist"))
        include(dependency("org.jvnet.mimepull:.*"))
        include(dependency("org.objenesis:.*"))
        include(dependency("org.reactivestreams:reactive-streams"))
        include(dependency("org.tukaani:xz"))
        include(dependency("org.yaml:snakeyaml"))
        include(dependency("org.roaringbitmap:RoaringBitmap"))
        include(dependency("com.github.ben-manes.caffeine:.*"))
        exclude(dependency("com.fasterxml.jackson.core:jackson-annotations"))
        exclude(dependency("com.google.protobuf:protobuf-java"))
        exclude(dependency("io.netty:netty-transport-native-kqueue"))
    }

    // ---- File excludes ----
    // Exclude bouncycastle (signatures would break if shaded)
    exclude("org/bouncycastle/**")
    exclude("**/module-info.class")
    exclude("findbugsExclude.xml")
    exclude("META-INF/*-LICENSE")
    exclude("META-INF/*-NOTICE")
    exclude("META-INF/*.DSA")
    exclude("META-INF/*.RSA")
    exclude("META-INF/*.SF")
    exclude("META-INF/DEPENDENCIES*")
    exclude("META-INF/io.netty.versions.properties")
    exclude("META-INF/LICENSE*")
    exclude("META-INF/license/**")
    exclude("META-INF/maven/**")
    exclude("META-INF/native-image/**")
    exclude("META-INF/NOTICE*")
    exclude("META-INF/proguard/**")

    // ---- Relocations ----
    relocate("com.fasterxml.jackson", "$shadePrefix.com.fasterxml.jackson") {
        exclude("com.fasterxml.jackson.annotation.*")
    }
    relocate("com.google", "$shadePrefix.com.google") {
        exclude("com.google.protobuf.**")
    }
    relocate("org.apache.avro", "$shadePrefix.org.apache.avro") {
        exclude("org.apache.avro.reflect.AvroAlias")
        exclude("org.apache.avro.reflect.AvroDefault")
        exclude("org.apache.avro.reflect.AvroEncode")
        exclude("org.apache.avro.reflect.AvroIgnore")
        exclude("org.apache.avro.reflect.AvroMeta")
        exclude("org.apache.avro.reflect.AvroName")
        exclude("org.apache.avro.reflect.AvroSchema")
        exclude("org.apache.avro.reflect.Nullable")
        exclude("org.apache.avro.reflect.Stringable")
        exclude("org.apache.avro.reflect.Union")
    }
    relocate("org.apache.pulsar.policies", "$shadePrefix.org.apache.pulsar.policies") {
        exclude("org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport")
        exclude("org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats")
        exclude("org.apache.pulsar.policies.data.loadbalancer.ResourceUsage")
        exclude("org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData")
    }
    relocateWithPrefix(shadePrefix, "com.github.benmanes")
    relocateWithPrefix(shadePrefix, "com.spotify.futures")
    relocateWithPrefix(shadePrefix, "com.squareup")
    relocateWithPrefix(shadePrefix, "com.sun.activation")
    relocateWithPrefix(shadePrefix, "com.thoughtworks.paranamer")
    relocateWithPrefix(shadePrefix, "com.typesafe")
    relocateWithPrefix(shadePrefix, "com.yahoo")
    relocateWithPrefix(shadePrefix, "io.airlift")
    relocateWithPrefix(shadePrefix, "io.grpc")
    relocateWithPrefix(shadePrefix, "io.netty")
    relocateWithPrefix(shadePrefix, "io.opencensus")
    relocateWithPrefix(shadePrefix, "io.prometheus.client")
    relocateWithPrefix(shadePrefix, "io.swagger")
    relocateWithPrefix(shadePrefix, "javassist")
    relocateWithPrefix(shadePrefix, "javax.activation")
    relocateWithPrefix(shadePrefix, "javax.annotation")
    relocateWithPrefix(shadePrefix, "javax.inject")
    relocateWithPrefix(shadePrefix, "javax.ws")
    relocateWithPrefix(shadePrefix, "javax.xml.bind")
    relocateWithPrefix(shadePrefix, "jersey")
    relocateWithPrefix(shadePrefix, "okio")
    relocateWithPrefix(shadePrefix, "org.aopalliance")
    relocateWithPrefix(shadePrefix, "org.apache.bookkeeper")
    relocateWithPrefix(shadePrefix, "org.apache.commons")
    relocateWithPrefix(shadePrefix, "org.apache.pulsar.checksum")
    relocateWithPrefix(shadePrefix, "org.asynchttpclient")
    relocateWithPrefix(shadePrefix, "org.checkerframework")
    relocateWithPrefix(shadePrefix, "org.codehaus.jackson")
    relocateWithPrefix(shadePrefix, "org.eclipse.jetty")
    relocateWithPrefix(shadePrefix, "org.glassfish")
    relocateWithPrefix(shadePrefix, "org.jvnet")
    relocateWithPrefix(shadePrefix, "org.objenesis")
    relocateWithPrefix(shadePrefix, "org.reactivestreams")
    relocateWithPrefix(shadePrefix, "org.roaringbitmap")
    relocateWithPrefix(shadePrefix, "org.tukaani")
    relocateWithPrefix(shadePrefix, "org.yaml")
    // NOTE: Do NOT shade log4j, otherwise logging won't work

    // ---- File content transformations ----
    relocateAsyncHttpClientProperties(shadePrefix)
    // Relocate Netty native library filenames to avoid conflicts with unshaded Netty
    filesMatching("META-INF/native/**") {
        if (name.matches(Regex("netty.+\\.(so|jnilib|dll)"))) {
            path = path.replace(name, "org_apache_pulsar_shade_$name")
        }
    }
}
