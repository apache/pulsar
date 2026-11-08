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
}

dependencies {
    implementation(libs.slog)
    implementation(libs.zookeeper)
    // Prometheus Java client 1.x: the registry and snapshot model, the metric implementations,
    // the JVM instrumentation and the text exposition format writer.
    implementation(libs.prometheus.metrics.model)
    implementation(libs.prometheus.metrics.core)
    implementation(libs.prometheus.metrics.instrumentation.jvm)
    implementation(libs.prometheus.metrics.exposition.textformats)
    // Transitional bridge for #27045: conf/log4j2.yaml still configures the legacy
    // simpleclient_log4j2 Prometheus appender, which registers into the 0.x
    // CollectorRegistry.defaultRegistry. Read that registry until the appender is migrated.
    implementation(libs.simpleclient)
    implementation(libs.simpleclient.common)
    implementation(libs.jetty.server)
    // javax.servlet and Jetty ee8 are still what the patched ZooKeeper AdminServer uses here;
    // the javax.servlet API is provided transitively by jetty-ee8-servlet.
    implementation(libs.jetty.ee8.servlet)
}
