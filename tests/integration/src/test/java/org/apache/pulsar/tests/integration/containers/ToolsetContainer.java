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

/**
 * A pulsar container that runs nothing.
 */
public class ToolsetContainer extends ChaosContainer<ToolsetContainer> {

    public static final String NAME = "toolset";

    public ToolsetContainer(String clusterName, String imageName) {
        super(clusterName, imageName);
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + NAME;
    }

    @Override
    protected void configure() {
        super.configure();
        this.withNetworkAliases(NAME)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(NAME);
                    createContainerCmd.withName(getContainerName());
                    createContainerCmd.withEntrypoint("sleep", "infinity");
                })
                .waitingFor(new HostPortWaitStrategy());
    }
}
