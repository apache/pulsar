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
 * A pulsar container that runs the presto worker
 */
public class PrestoWorkerContainer extends PulsarContainer<PrestoWorkerContainer> {

    public static final String NAME = "presto-worker";
    public static final int PRESTO_HTTP_PORT = 8081;

    public PrestoWorkerContainer(String clusterName, String hostname) {
        super(
                clusterName,
                hostname,
                hostname,
                "bin/run-presto-worker.sh",
                -1,
                PRESTO_HTTP_PORT,
                "/v1/info/state");
        tailContainerLog();
    }

    @Override
    protected void afterStart() {
        DockerUtils.runCommandAsyncWithLogging(this.dockerClient, this.getContainerId(),
                "tail", "-f", "/pulsar/lib/presto/var/log/launcher.log");
        DockerUtils.runCommandAsyncWithLogging(this.dockerClient, this.getContainerId(),
                "tail", "-f", "/var/log/pulsar/presto_worker.log");
        DockerUtils.runCommandAsyncWithLogging(this.dockerClient, this.getContainerId(),
                "tail", "-f", "/pulsar/lib/presto/var/log/server.log");
    }

    @Override
    protected void beforeStop() {
        super.beforeStop();
        if (null != getContainerId()) {
            DockerUtils.dumpContainerDirToTargetCompressed(
                    getDockerClient(),
                    getContainerId(),
                    "/pulsar/lib/presto/var/log"
            );
        }
    }

    public String getUrl() {
        return String.format("%s:%s",  getContainerIpAddress(), getMappedPort(PrestoWorkerContainer.PRESTO_HTTP_PORT));
    }
}
