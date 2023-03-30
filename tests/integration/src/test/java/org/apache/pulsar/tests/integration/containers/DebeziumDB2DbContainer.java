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


import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

public class DebeziumDB2DbContainer extends ChaosContainer<DebeziumDB2DbContainer> {

    public static final String NAME = "debezium-db2";
    static final Integer[] PORTS = { 50000 };

    //
    private static final String IMAGE_NAME = "apachepulsar/debezium-db2-test-image:latest";

    public DebeziumDB2DbContainer(String clusterName) {
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
            .withEnv("LICENSE", "accept")
            .withEnv("DB2INSTANCE", "db2inst1")
            .withEnv("DB2INST1_PASSWORD", "admin")
            .withEnv("DBNAME", "mydb2")
            .withEnv("BLU", "false")
            .withEnv("ENABLE_ORACLE_COMPATIBILITY", "false")
            .withEnv("UPDATEAVAIL", "NO")
            .withEnv("TO_CREATE_SAMPLEDB", "false")
            .withEnv("REPODB", "false")
            .withEnv("IS_OSXFS", "false")
            .withEnv("PERSISTENT_HOME", "true")
            .withEnv("HADR_ENABLED", "false")
            .withPrivilegedMode(true)
            .withStartupTimeout(Duration.of(300, ChronoUnit.SECONDS))
            .withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(NAME);
                createContainerCmd.withName(getContainerName());
            })
            .waitingFor(new HostPortWaitStrategy());
    }

}
