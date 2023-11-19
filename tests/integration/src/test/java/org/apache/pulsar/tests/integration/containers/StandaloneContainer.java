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

import static java.time.temporal.ChronoUnit.SECONDS;

import java.time.Duration;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

/**
 * A pulsar container that runs standalone.
 */
public class StandaloneContainer extends PulsarContainer<StandaloneContainer> {

    public static final String NAME = "standalone";

    public StandaloneContainer(String clusterName) {
        this(clusterName, DEFAULT_IMAGE_NAME);
    }

    public StandaloneContainer(String clusterName, String pulsarImageName) {
        this(clusterName, pulsarImageName, false);
    }

    public StandaloneContainer(String clusterName, String pulsarImageName, boolean enableTls) {
        super(clusterName,
                NAME,
                NAME + "-cluster",
                pulsarImageName.endsWith("latest") ? "bin/run-standalone.sh" : "bin/pulsar",
                BROKER_PORT,
                enableTls ? BROKER_PORT_TLS : 0,
                BROKER_HTTP_PORT,
                enableTls ? BROKER_HTTPS_PORT : 0,
                "",
                pulsarImageName);
        if (pulsarImageName.endsWith("latest")) {
            tailContainerLog();
        }
    }



    @Override
    protected void configure() {
        super.configure();
        if (!getDockerImageName().endsWith("latest")) {
            setCommand("standalone");
        }
        addEnv("PULSAR_MEM", "-Xms128M -Xmx1g -XX:MaxDirectMemorySize=1g");
    }

    @Override
    protected void beforeStart() {
        // update the wait strategy until public/default namespace is created
        this.waitStrategy = new HttpWaitStrategy()
                .forPort(BROKER_HTTP_PORT)
                .forStatusCode(200)
                .forPath("/admin/v2/namespaces/public/default")
                .withStartupTimeout(Duration.of(300, SECONDS));
    }
}
