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
package org.apache.pulsar.tests.integration.containers;

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * ScyllaDB Container.
 *
 * ScyllaDB is a drop-in replacement for Apache Cassandra that uses the same
 * CQL protocol and is compatible with Cassandra drivers. This container
 * demonstrates ScyllaDB's compatibility by reusing the existing Cassandra
 * sink connector without any modifications.
 */
@Slf4j
public class ScyllaDBContainer<SelfT extends ChaosContainer<SelfT>> extends ChaosContainer<SelfT> {

    public static final String NAME = "scylladb";
    public static final int PORT = 9042;

    public ScyllaDBContainer(String clusterName) {
        super(clusterName, "scylladb/scylla:2025.1.4");
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
            .withExposedPorts(PORT)
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(clusterName + "-" + NAME);
            })
            .withCommand(
                "--smp", "1",                    // Single CPU core for container efficiency
                "--memory", "1G",                // Memory limit
                "--overprovisioned", "1",        // Container mode flag
                "--api-address", "0.0.0.0"       // Enable API access
                // Note: --reactor-backend=epoll is needed for macOS multi-node clusters
                // but single-node containers work without it
            )
            .waitingFor(new HostPortWaitStrategy());
    }

    public int getScyllaDBPort() {
        return getMappedPort(PORT);
    }
}
