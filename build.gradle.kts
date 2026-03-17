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
    base
}

group = "org.apache.pulsar"
version = "4.2.0-SNAPSHOT"
description = "Pulsar is a distributed pub-sub messaging platform with a very flexible messaging model and an intuitive client API."

// ──────────────────────────────────────────────────────────────
// Shared configuration for all subprojects
// ──────────────────────────────────────────────────────────────
subprojects {
    repositories {
        mavenCentral()
        maven { url = uri("https://packages.confluent.io/maven/") }
    }

    group = rootProject.group
    version = rootProject.version
}

// ──────────────────────────────────────────────────────────────
// Aggregated tasks
// ──────────────────────────────────────────────────────────────
tasks.register("rat") {
    description = "Runs Apache RAT license header checks"
    group = "verification"
    // TODO: configure Apache RAT Gradle plugin or exec task
}

tasks.register("checkstyle") {
    description = "Runs Checkstyle checks across all modules"
    group = "verification"
    // TODO: aggregate checkstyle tasks from subprojects
}
