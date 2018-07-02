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
package org.apache.pulsar.tests.containers;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
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

    public void tailContainerLog() {
        CompletableFuture.runAsync(() -> {
            while (null == containerId) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }

            LogContainerCmd logContainerCmd = this.dockerClient.logContainerCmd(containerId);
            logContainerCmd.withStdOut(true).withStdErr(true).withFollowStream(true);
            logContainerCmd.exec(new LogContainerResultCallback() {
                @Override
                public void onNext(Frame item) {
                    log.info(new String(item.getPayload(), UTF_8));
                }
            });
        });
    }

    public String getContainerLog() {
        StringBuilder sb = new StringBuilder();

        LogContainerCmd logContainerCmd = this.dockerClient.logContainerCmd(containerId);
        logContainerCmd.withStdOut(true).withStdErr(true);
        try {
            logContainerCmd.exec(new LogContainerResultCallback() {
                @Override
                public void onNext(Frame item) {
                    sb.append(new String(item.getPayload(), UTF_8));
                }
            }).awaitCompletion();
        } catch (InterruptedException e) {

        }
        return sb.toString();
    }

    public ExecResult execCmd(String... cmd) throws Exception {
        String cmdString = StringUtils.join(cmd, " ");

        log.info("DOCKER.exec({}:{}): Executing ...", containerId, cmdString);

        ExecResult result = execInContainer(cmd);

        log.info("Docker.exec({}:{}): Done", containerId, cmdString);
        log.info("Docker.exec({}:{}): Stdout -\n{}", containerId, cmdString, result.getStdout());
        log.info("Docker.exec({}:{}): Stderr -\n{}", containerId, cmdString, result.getStderr());

        return result;
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
