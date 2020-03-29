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

public class RabbitMQContainer extends ChaosContainer<RabbitMQContainer> {
    public static final String NAME = "RabbitMQ";
    public static final Integer[] PORTS = { 5672 };
    private static final String IMAGE_NAME = "rabbitmq:3.8-management";

    private String networkAlias;

    public RabbitMQContainer(String clusterName, String networkAlias) {
        super(clusterName, IMAGE_NAME);
        this.networkAlias = networkAlias;
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(networkAlias)
                .withExposedPorts(PORTS)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(clusterName + "-" + NAME);
                })
                .waitingFor(new HostPortWaitStrategy());
    }
}
