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

description = "Pulsar IO Connectors Distribution"

// Collect all NAR files from IO connector modules
val narFiles by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
}

// TODO: Add NAR artifact dependencies from each connector module
// Once NAR plugin publishes artifacts properly, these can be collected here

tasks.register<Copy>("collectNars") {
    description = "Collects all connector NAR files into a single directory"
    group = "distribution"
    from(narFiles)
    into(layout.buildDirectory.dir("connectors"))
    include("*.nar")
}
