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

package org.apache.pulsar.tests.integration.oxia;

import java.time.Duration;
import org.apache.pulsar.tests.integration.containers.ChaosContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class OxiaContainer extends ChaosContainer<OxiaContainer> {

    public static final String NAME = "oxia";

    public static final int OXIA_PORT = 6648;
    public static final int METRICS_PORT = 8080;
    private static final int DEFAULT_SHARDS = 1;

    private static final String DEFAULT_IMAGE_NAME = "streamnative/oxia:main";

    public OxiaContainer(String clusterName) {
        this(clusterName, DEFAULT_IMAGE_NAME, DEFAULT_SHARDS);
    }

    @SuppressWarnings("resource")
    OxiaContainer(String clusterName, String imageName, int shards) {
        super(clusterName, imageName);
        if (shards <= 0) {
            throw new IllegalArgumentException("shards must be greater than zero");
        }
        addExposedPorts(OXIA_PORT, METRICS_PORT);
        this.withCreateContainerCmdModifier(createContainerCmd -> {
            createContainerCmd.withHostName("oxia");
            createContainerCmd.withName(getContainerName());
        });
        setCommand("oxia", "standalone",
                "--shards=" + shards,
                "--wal-sync-data=false");
        waitingFor(
                Wait.forHttp("/metrics")
                        .forPort(METRICS_PORT)
                        .forStatusCode(200)
                        .withStartupTimeout(Duration.ofSeconds(30)));

        PulsarContainer.configureLeaveContainerRunning(this);
    }

    public String getServiceAddress() {
        return OxiaContainer.NAME + ":" + OXIA_PORT;
    }

    @Override
    public String getContainerName() {
        return clusterName + "-oxia";
    }
}
