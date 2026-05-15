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

// Force Jetty 9 for this module. Hadoop MiniDFSCluster requires Jetty 9 classes
// (e.g., HandlerWrapper) that were removed in Jetty 12. The version catalog constraints
// would otherwise upgrade Jetty to 12.x.
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
    // Jetty 9 dependencies needed by Hadoop MiniDFSCluster (version forced above).
    testImplementation("org.eclipse.jetty:jetty-server:9.4.58.v20250814")
    testImplementation("org.eclipse.jetty:jetty-servlet:9.4.58.v20250814")
    testImplementation("org.eclipse.jetty:jetty-util:9.4.58.v20250814")
}
