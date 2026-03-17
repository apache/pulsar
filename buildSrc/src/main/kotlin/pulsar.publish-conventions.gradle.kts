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
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set(project.name)
                description.set("Apache Pulsar - ${project.name}")
                url.set("https://github.com/apache/pulsar")

                licenses {
                    license {
                        name.set("Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        organization.set("Apache Software Foundation")
                        organizationUrl.set("https://www.apache.org/")
                    }
                }

                scm {
                    url.set("https://github.com/apache/pulsar")
                    connection.set("scm:git:https://github.com/apache/pulsar.git")
                    developerConnection.set("scm:git:ssh://git@github.com:apache/pulsar.git")
                }
            }
        }
    }

    repositories {
        maven {
            name = "apache"
            val releasesRepoUrl = uri("https://repository.apache.org/service/local/staging/deploy/maven2")
            val snapshotsRepoUrl = uri("https://repository.apache.org/content/repositories/snapshots")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
            credentials {
                username = findProperty("apache.username") as? String
                password = findProperty("apache.password") as? String
            }
        }
    }
}

signing {
    val signingKey = findProperty("signing.gnupg.keyName") as? String
    if (signingKey != null) {
        useGpgCmd()
        sign(publishing.publications["maven"])
    }
}
