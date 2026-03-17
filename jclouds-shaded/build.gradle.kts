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
    id("pulsar.shade-conventions")
}

description = "JClouds Shaded"

dependencies {
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)
    implementation("org.apache.jclouds:jclouds-blobstore:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds:jclouds-allblobstore:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.api:s3:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.provider:aws-s3:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.api:openstack-swift:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.provider:google-cloud-storage:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.api:filesystem:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.provider:azureblob:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.driver:jclouds-apachehc:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.driver:jclouds-okhttp:${libs.versions.jclouds.get()}")
    implementation("org.apache.jclouds.driver:jclouds-slf4j:${libs.versions.jclouds.get()}")
    implementation(libs.guice)
    implementation(libs.guice.assistedinject)
    implementation(libs.gson)
}

tasks.shadowJar {
    relocate("com.google", "org.apache.pulsar.jcloud.shade.com.google")
    relocate("com.jamesmurty.utils", "org.apache.pulsar.jcloud.shade.com.jamesmurty.utils")
    relocate("aopalliance", "org.apache.pulsar.jcloud.shade.aopalliance")
    relocate("net.iharder", "org.apache.pulsar.jcloud.shade.net.iharder")
    relocate("com.google.errorprone", "org.apache.pulsar.jcloud.shade.com.google.errorprone")
    relocate("jakarta", "org.apache.pulsar.jcloud.shade.jakarta")
    relocate("org.aopalliance", "org.apache.pulsar.jcloud.shade.org.aopalliance")
}
