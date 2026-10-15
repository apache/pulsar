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

// Jar of just the fastutil classes reachable from the Pulsar server side. It replaces the
// full fastutil jar in the server distribution so the docker image / tarball ships only the
// classes actually used. The roots cover both the broker and (since it is bundled in the
// server distribution) the unrelocated pulsar-client-original, so this is a superset of
// :pulsar-client-fastutil-minimized. See pulsar.minimized-dependencies-conventions.

plugins {
    id("pulsar.minimized-dependencies-conventions")
}

dependencies {
    // Reachability roots: every Pulsar project that uses fastutil and ends up in the server
    // distribution. minimize() keeps the union of fastutil classes reachable from these.
    api(project(":pulsar-broker"))
    api(project(":pulsar-client-original"))
}

minimizedJar {
    minimizedDependencies.set(listOf("it.unimi.dsi:fastutil"))
    // The reachable set (broker + client usage) is ~818 classes; fail the build if it grows
    // past this (bump it when new fastutil usage legitimately enlarges the set).
    maxRetainedClasses.set(850)
}
