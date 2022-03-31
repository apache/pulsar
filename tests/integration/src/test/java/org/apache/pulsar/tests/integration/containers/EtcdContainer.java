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

/**
 * Etcd container.
 */
public class EtcdContainer extends ChaosContainer<EtcdContainer> {

    public static final String NAME = "etcd";
    public static final int PORT = 2379;
    public static final int INTERNAL_PORT = 2380;
    public static final Integer[] PORTS = {PORT, INTERNAL_PORT};
    private static final String IMAGE_NAME = "quay.io/coreos/etcd:v3.5.2";

    private String networkAlias;

    public EtcdContainer(String clusterName, String networkAlias) {
        super(clusterName, IMAGE_NAME);
        this.networkAlias = networkAlias;
    }

    @Override
    protected void configure() {
        super.configure();

        String[] command = new String[]{
                "/usr/local/bin/etcd",
                "--name", String.format("%s0", clusterName),
                "--initial-advertise-peer-urls", String.format("http://%s:%d", clusterName, INTERNAL_PORT),
                "--listen-peer-urls", String.format("http://0.0.0.0:%d", INTERNAL_PORT),
                "--advertise-client-urls", String.format("http://%s:%d", clusterName, PORT),
                "--listen-client-urls", String.format("http://0.0.0.0:%d", PORT),
                "--initial-cluster", String.format("%s0=http://%s:%d", clusterName, clusterName, INTERNAL_PORT),
        };

        this.withNetworkAliases(networkAlias)
                .withExposedPorts(PORTS)
                .withCommand(command)
                .withCreateContainerCmdModifier(createContainerCmd -> {
                    createContainerCmd.withHostName(clusterName);
                    createContainerCmd.withName(clusterName + "-" + NAME);
                })
                .waitingFor(new HostPortWaitStrategy());
    }
}
