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

import org.apache.pulsar.tests.integration.utils.DockerUtils;

/**
 * A pulsar container that runs bookkeeper.
 */
public class BrokerContainer extends PulsarContainer<BrokerContainer> {

    public static final String NAME = "pulsar-broker";

    public BrokerContainer(String clusterName, String hostName) {
        super(
            clusterName, hostName, hostName, "bin/run-broker.sh", BROKER_PORT, BROKER_HTTP_PORT);
        tailContainerLog();
    }

    @Override
    protected void afterStart() {
        DockerUtils.runCommandAsyncWithLogging(this.dockerClient, this.getContainerId(),
                "tail", "-f", "/var/log/pulsar/broker.log");
    }
}
