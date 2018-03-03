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
package org.apache.pulsar.tests;

import org.arquillian.cube.docker.impl.docker.DockerClientExecutor;
import org.arquillian.cube.impl.model.CubeId;
import org.arquillian.cube.spi.beforeStop.BeforeStopAction;

public class PulsarLogsToTargetDirStopAction implements BeforeStopAction {
    private DockerClientExecutor dockerClientExecutor;
    private CubeId containerID;

    public void setDockerClientExecutor(DockerClientExecutor executor) {
        this.dockerClientExecutor = executor;
    }

    public void setContainerID(CubeId containerID) {
        this.containerID = containerID;
    }

    @Override
    public void doBeforeStop() {
        DockerUtils.dumpContainerLogToTarget(dockerClientExecutor.getDockerClient(), containerID.getId());
        DockerUtils.dumpContainerLogDirToTarget(dockerClientExecutor.getDockerClient(),
                                                containerID.getId(), "/var/log/pulsar");
    }
}
