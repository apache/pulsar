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

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class DebeziumOracleDbContainer extends ChaosContainer<DebeziumOracleDbContainer> {

    public static final String NAME = "debezium-oracledb-12c";
    static final Integer[] PORTS = { 1521 };

    // https://github.com/MaksymBilenko/docker-oracle-12c
    // Apache 2.0 license.
    // Newer versions don't have LigMiner in XE (Standard) Edition and require Enterprise.
    // Debezium 1.5 didn't work with 11g out of the box
    // and it is not tested with 11.g according to https://debezium.io/releases/1.5/
    private static final String IMAGE_NAME = "quay.io/maksymbilenko/oracle-12c:master";

    public DebeziumOracleDbContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    public String getContainerName() {
        return clusterName;
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
            .withExposedPorts(PORTS)
            .withEnv("DBCA_TOTAL_MEMORY", "2048")
            .withEnv("WEB_CONSOLE", "false")
            .withStartupTimeout(Duration.of(300, ChronoUnit.SECONDS))
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(getContainerName());
            })
            .waitingFor(new HostPortWaitStrategy());
    }

}
