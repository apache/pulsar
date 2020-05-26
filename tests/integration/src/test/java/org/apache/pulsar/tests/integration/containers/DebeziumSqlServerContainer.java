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

public class DebeziumSqlServerContainer extends ChaosContainer<DebeziumSqlServerContainer> {

    public static final String NAME = "debezium-sqlserver-example";
    static final Integer[] PORTS = { 1433 };

    private static final String IMAGE_NAME = "microsoft/mssql-server-linux:2017-CU9-GDR2";

    public DebeziumSqlServerContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
        this.withEnv("ACCEPT_EULA", "y");
        this.withEnv("MSSQL_PID", "Standard");
        this.withEnv("SA_PASSWORD", "Password!");
        this.withEnv("MSSQL_AGENT_ENABLED", "true");

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
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(getContainerName());
            })
            .waitingFor(new HostPortWaitStrategy());
    }

}
