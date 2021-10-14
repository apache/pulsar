/**
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


import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class DebeziumMsSqlContainer extends ChaosContainer<DebeziumMsSqlContainer> {

    // This password needs to include at least 8 characters of at least three of these four categories:
    // uppercase letters, lowercase letters, numbers and non-alphanumeric symbols
    public static final String SA_PASSWORD = "p@ssw0rD";
    public static final String NAME = "debezium-mssql";
    static final Integer[] PORTS = { 1433 };

    // https://hub.docker.com/_/microsoft-mssql-server
    // EULA: https://go.microsoft.com/fwlink/?linkid=857698
    // "You may install and use copies of the software on any device,
    // including third party shared devices, to design, develop, test and demonstrate your programs.
    // You may not use the software on a device or server in a production environment."
    private static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2019-CU12-ubuntu-20.04";

    public DebeziumMsSqlContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    public String getContainerName() {
        return clusterName;
    }

    @Override
    protected void configure() {
        super.configure();
        // leaving default MSSQL_PID (aka Developer edition)
        this.withNetworkAliases(NAME)
            .withExposedPorts(PORTS)
            .withEnv("ACCEPT_EULA", "Y")
            .withEnv("SA_PASSWORD", SA_PASSWORD)
            .withEnv("MSSQL_SA_PASSWORD", SA_PASSWORD)
            .withEnv("MSSQL_AGENT_ENABLED", "true")
            .withStartupTimeout(Duration.of(300, ChronoUnit.SECONDS))
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(getContainerName());
            })
            // wait strategy to address problem with MS SQL responding to the connection
            // before service starts up completely
            // https://github.com/microsoft/mssql-docker/issues/625#issuecomment-882025521
            .waitingFor(Wait.forLogMessage(".*The tempdb database has .*", 2));
    }

}
