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
package org.apache.pulsar.tests.performance.report;

import java.util.ArrayList;
import java.util.List;

/**
 * The Docker engine that runs a run's containers, as {@code docker info} describes it. On Linux the engine runs on
 * the host, but on macOS and Windows it runs in Docker Desktop's virtual machine, whose CPUs and memory are those
 * that the broker, the bookies and the clients get, rather than the host's.
 *
 * @param os the operating system that the engine runs on, such as {@code Docker Desktop} or {@code Ubuntu 24.04 LTS}
 */
public record DockerEngine(String version, int cpus, long memoryBytes, String os, String kernel, String architecture) {
    /**
     * The engine in one line, such as "Docker 28.4.0, 16 CPUs, 31 GiB, Ubuntu 24.04 LTS (kernel
     * 6.8.0-45-generic, x86_64)".
     */
    public String summary() {
        List<String> parts = new ArrayList<>();
        if (!version.isEmpty()) {
            parts.add("Docker " + version);
        }
        if (cpus > 0) {
            parts.add(cpus + (cpus == 1 ? " CPU" : " CPUs"));
        }
        if (memoryBytes > 0) {
            parts.add(HostDetails.gibibytes(memoryBytes));
        }
        List<String> platform = new ArrayList<>();
        if (!kernel.isEmpty()) {
            platform.add("kernel " + kernel);
        }
        if (!architecture.isEmpty()) {
            platform.add(architecture);
        }
        String system = platform.isEmpty() ? os : (os + " (" + String.join(", ", platform) + ")").trim();
        if (!system.isEmpty()) {
            parts.add(system);
        }
        return String.join(", ", parts);
    }
}
