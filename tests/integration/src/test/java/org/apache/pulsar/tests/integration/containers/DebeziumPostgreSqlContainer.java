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

public class DebeziumPostgreSqlContainer extends ChaosContainer<DebeziumPostgreSqlContainer> {

    public static final String NAME = "debezium-postgresql-example";
    static final Integer[] PORTS = { 5432 };

    private static final String IMAGE_NAME = "debezium/example-postgres:0.10";

    public DebeziumPostgreSqlContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
        this.withEnv("POSTGRES_USER", "postgres");
        this.withEnv("POSTGRES_PASSWORD", "postgres");
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
