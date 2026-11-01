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
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;

/**
 * A pulsar container that runs bookkeeper.
 */
public class BrokerContainer extends PulsarContainer<BrokerContainer> {

    public static final String NAME = "pulsar-broker";

    public BrokerContainer(String clusterName, String hostName) {
        this(clusterName, hostName, false);
    }

    public BrokerContainer(String clusterName, String hostName, boolean enableTls) {
        this(clusterName, hostName, enableTls, DEFAULT_IMAGE_NAME);
    }

    public BrokerContainer(String clusterName, String hostName, boolean enableTls, String imageName) {
        super(clusterName, hostName, hostName, "bin/run-broker.sh", BROKER_PORT,
                enableTls ? BROKER_PORT_TLS : 0, BROKER_HTTP_PORT,
                enableTls ? BROKER_HTTPS_PORT : 0, DEFAULT_HTTP_PATH, imageName);
        tailContainerLog();
    }

    public String getHostName() {
        return super.hostname;
    }

    @Override
    protected void beforeStart() {
        // A broker opens its ports before it registers itself and is ready to serve, so also wait for it to be ready:
        // for its log to say so or, should it not log at INFO level, for its ready probe, which works without
        // authentication only
        waitStrategy = new WaitAllStrategy()
                .withStrategy(waitStrategy)
                .withStrategy(Wait.forSuccessfulCommand("until grep -qi 'messaging service is ready' "
                        + "/var/log/pulsar/broker.log || curl -sf -o /dev/null "
                        + "http://localhost:" + BROKER_HTTP_PORT + "/admin/v2/brokers/ready; do sleep 0.1; done"))
                .withStartupTimeout(Duration.of(300, SECONDS));
    }

    @Override
    protected void afterStart() {
        DockerUtils.runCommandAsyncWithLogging(this.dockerClient, this.getContainerId(),
                "tail", "-f", "/var/log/pulsar/broker.log");
    }
}
