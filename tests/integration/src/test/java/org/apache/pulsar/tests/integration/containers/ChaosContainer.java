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

import com.github.dockerjava.api.DockerClient;

import java.util.Base64;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.GenericContainer;

/**
 * A base container provides chaos capability.
 */
@Slf4j
public class ChaosContainer<SelfT extends ChaosContainer<SelfT>> extends GenericContainer<SelfT> {

    protected final String clusterName;

    protected ChaosContainer(String clusterName, String image) {
        super(image);
        this.clusterName = clusterName;
    }

    @Override
    protected void configure() {
        super.configure();
        addEnv("MALLOC_ARENA_MAX", "1");
    }

    protected void beforeStop() {
        if (null == getContainerId()) {
            return;
        }

        // dump the container log
        DockerUtils.dumpContainerLogToTarget(
            getDockerClient(),
            getContainerId()
        );
    }

    @Override
    public void stop() {
        beforeStop();
        super.stop();
    }

    protected void tailContainerLog() {
        withLogConsumer(item -> log.info(item.getUtf8String()));
    }

    public void putFile(String path, byte[] contents) throws Exception {
        String base64contents = Base64.getEncoder().encodeToString(contents);
        String cmd = String.format("echo %s | base64 -d > %s", base64contents, path);
        execCmd("bash", "-c", cmd);
    }

    public ContainerExecResult execCmd(String... commands) throws Exception {
        DockerClient client = this.getDockerClient();
        String dockerId = this.getContainerId();
        return DockerUtils.runCommand(client, dockerId, commands);
    }

    public CompletableFuture<ContainerExecResult> execCmdAsync(String... commands) throws Exception {
        DockerClient client = this.getDockerClient();
        String dockerId = this.getContainerId();
        return DockerUtils.runCommandAsync(client, dockerId, commands);
    }

    public ContainerExecResult execCmdAsUser(String userId, String... commands) throws Exception {
        DockerClient client = this.getDockerClient();
        String dockerId = this.getContainerId();
        return DockerUtils.runCommandAsUser(userId, client, dockerId, commands);
    }

    public CompletableFuture<ContainerExecResult> execCmdAsyncAsUser(String userId, String... commands) throws Exception {
        DockerClient client = this.getDockerClient();
        String dockerId = this.getContainerId();
        return DockerUtils.runCommandAsyncAsUser(userId, client, dockerId, commands);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ChaosContainer)) {
            return false;
        }

        ChaosContainer another = (ChaosContainer) o;
        return clusterName.equals(another.clusterName)
            && super.equals(another);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(
            clusterName);
    }

}
