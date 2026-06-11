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
    id("pulsar.nar-conventions")
}

// Force the legacy javax web stack for this module's tests. Hadoop's MiniDFSCluster is a fully
// javax-based component: it requires Jetty 9 classes (e.g., HandlerWrapper) removed in Jetty 12,
// and its NameNode web UI registers Jersey 2.x's ServletContainer (a javax.servlet.Servlet).
// The version catalog constraints from the jakarta migration (PIP-472) would otherwise upgrade
// Jetty to 12.x and Jersey to 3.x (jakarta), which MiniDFSCluster's Jetty 9 web app rejects with
// "org.glassfish.jersey.servlet.ServletContainer is not a javax.servlet.Servlet". These forces are
// scoped to test configurations only; production code uses Jetty 12 / Jersey 3 (jakarta).
configurations.matching { it.name.startsWith("test") }.all {
    resolutionStrategy.force(
        "org.eclipse.jetty:jetty-server:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-servlet:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-util:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-security:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-http:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-io:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-webapp:9.4.58.v20250814",
        "org.eclipse.jetty:jetty-xml:9.4.58.v20250814",
        // Jersey 2.x (javax) — the version Hadoop 3.4 declares; the migration would force these to 3.x.
        "org.glassfish.jersey.core:jersey-client:2.46",
        "org.glassfish.jersey.core:jersey-common:2.46",
        "org.glassfish.jersey.core:jersey-server:2.46",
        "org.glassfish.jersey.containers:jersey-container-servlet:2.46",
        "org.glassfish.jersey.containers:jersey-container-servlet-core:2.46",
        "org.glassfish.jersey.inject:jersey-hk2:2.46",
        // hk2 2.6.1 — the version Jersey 2.x depends on (the migration would force these to 3.x).
        "org.glassfish.hk2:hk2-api:2.6.1",
        "org.glassfish.hk2:hk2-locator:2.6.1",
        "org.glassfish.hk2:hk2-utils:2.6.1",
        "org.glassfish.hk2.external:aopalliance-repackaged:2.6.1",
        "org.glassfish.hk2.external:jakarta.inject:2.6.1",
    )
}

dependencies {
    implementation(libs.slog)
    compileOnly(project(":managed-ledger"))
    compileOnly(libs.bookkeeper.server)
    compileOnly(libs.netty.buffer)
    implementation(libs.hadoop.common) {
        exclude(group = "log4j", module = "log4j")
        exclude(group = "org.slf4j")
        exclude(group = "dnsjava", module = "dnsjava")
        exclude(group = "org.bouncycastle", module = "bcprov-jdk15on")
        exclude(group = "io.netty")
    }
    implementation(libs.bcprov.jdk18on)
    implementation(libs.hadoop.hdfs.client) {
        exclude(group = "org.apache.avro", module = "avro")
        exclude(group = "org.mortbay.jetty", module = "jetty")
        exclude(group = "com.sun.jersey", module = "jersey-core")
        exclude(group = "com.sun.jersey", module = "jersey-server")
        exclude(group = "javax.servlet", module = "servlet-api")
        exclude(group = "dnsjava", module = "dnsjava")
        exclude(group = "org.bouncycastle", module = "bcprov-jdk15on")
    }
    implementation(libs.avro)
    implementation(libs.json.smart)
    implementation(libs.protobuf.java)
    // Hadoop references the legacy javax.xml.bind API at runtime; it is no longer on the classpath
    // after the jakarta migration (PIP-472), so restore it for this module's Hadoop dependency.
    runtimeOnly(libs.jaxb.api)

    testImplementation(project(":managed-ledger"))
    testImplementation(project(":testmocks"))
    testImplementation(libs.bookkeeper.server)
    testImplementation(libs.netty.buffer)
    testImplementation(libs.hadoop.minicluster) {
        exclude(group = "io.netty", module = "netty-all")
        exclude(group = "org.bouncycastle")
        exclude(group = "org.slf4j")
        exclude(group = "dnsjava", module = "dnsjava")
    }
    testImplementation(libs.simpleclient)
    testImplementation(libs.bcpkix.jdk18on)
    testImplementation(libs.netty.codec.http)
    // Hadoop's MiniDFSCluster embeds a Jetty 9 web UI that needs the legacy javax.ws.rs,
    // javax.annotation (Jersey 2.x's @Priority) and javax.validation APIs, which are no longer on
    // the classpath after the jakarta migration (PIP-472). Only the tests need them.
    testRuntimeOnly(libs.javax.ws.rs.api)
    testRuntimeOnly("javax.annotation:javax.annotation-api:1.3.2")
    testRuntimeOnly("javax.validation:validation-api:2.0.1.Final")
    // Jetty 9 dependencies needed by Hadoop MiniDFSCluster (version forced above).
    testImplementation("org.eclipse.jetty:jetty-server:9.4.58.v20250814")
    testImplementation("org.eclipse.jetty:jetty-servlet:9.4.58.v20250814")
    testImplementation("org.eclipse.jetty:jetty-util:9.4.58.v20250814")
}
