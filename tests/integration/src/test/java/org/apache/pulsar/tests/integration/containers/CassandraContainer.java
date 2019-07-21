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

import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;

/**
 * Cassandra Container.
 */
@Slf4j
public class CassandraContainer<SelfT extends ChaosContainer<SelfT>> extends ChaosContainer<SelfT> {

    public static final String NAME = "cassandra";
    public static final int PORT = 9042;

    public CassandraContainer(String clusterName) {
        super(clusterName, "cassandra:3");
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
            .waitingFor(new HostPortWaitStrategy());
    }

    public int getCassandraPort() {
        return getMappedPort(PORT);
    }
}
