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
    `java-platform`
    `maven-publish`
}

description = "Apache Pulsar BOM"

group = "org.apache.pulsar"
version = rootProject.version

javaPlatform {
    allowDependencies()
}

dependencies {
    constraints {
        api(project(":pulsar-client-api"))
        api(project(":pulsar-client-admin-api"))
        api(project(":pulsar-client-shaded"))
        api(project(":pulsar-client-admin-shaded"))
        api(project(":pulsar-client-all"))
        api(project(":pulsar-functions:api-java"))
        api(project(":pulsar-io-core"))
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["javaPlatform"])
            pom {
                name.set("Apache Pulsar BOM")
                description.set("Bill of Materials for Apache Pulsar")
                url.set("https://github.com/apache/pulsar")
                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
            }
        }
    }
}
