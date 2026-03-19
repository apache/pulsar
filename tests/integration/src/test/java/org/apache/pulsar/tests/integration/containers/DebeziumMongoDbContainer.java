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

import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

public class DebeziumMongoDbContainer extends ChaosContainer<DebeziumMongoDbContainer> {

    public static final String NAME = "debezium-mongodb-example";

    public static final Integer[] PORTS = { 27017 };
    private static final String IMAGE_NAME =
            "quay.io/debezium/example-mongodb:" + System.getProperty("debezium.version", "3.4.2.Final");

    public DebeziumMongoDbContainer(String clusterName) {
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
                .withEnv("MONGODB_USER", "debezium")
                .withEnv("MONGODB_PASSWORD", "dbz")
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(getContainerName());
                })
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(new HostPortWaitStrategy())
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete.*", 1))
                );
    }
}
