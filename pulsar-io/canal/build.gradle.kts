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
    id("pulsar.nar")
}

description = "Pulsar IO :: Canal"

dependencies {
    implementation(project(":pulsar-io:common"))
    implementation(project(":pulsar-io-core"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.dataformat.yaml)
    implementation("com.alibaba:fastjson:1.2.83")
    implementation("org.springframework:spring-core:${libs.versions.spring.get()}")
    implementation("org.springframework:spring-aop:${libs.versions.spring.get()}")
    implementation("org.springframework:spring-context:${libs.versions.spring.get()}")
    implementation("org.springframework:spring-jdbc:${libs.versions.spring.get()}")
    implementation("org.springframework:spring-orm:${libs.versions.spring.get()}")
    implementation("com.alibaba.otter:canal.protocol:1.1.5") {
        exclude(group = "ch.qos.logback")
    }
    implementation("com.alibaba.otter:canal.client:1.1.5") {
        exclude(group = "ch.qos.logback")
        exclude(group = "org.springframework")
    }
    implementation(libs.log4j.core)

    testImplementation(project(":buildtools"))
}
