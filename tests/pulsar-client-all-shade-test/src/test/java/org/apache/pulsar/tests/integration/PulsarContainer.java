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
package org.apache.pulsar.tests.integration;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public class PulsarContainer extends GenericContainer<PulsarContainer> {

    public static final int PULSAR_PORT = 6650;
    public static final int BROKER_HTTP_PORT = 8080;
    public static final String DEFAULT_IMAGE_NAME = System.getenv().getOrDefault("PULSAR_TEST_IMAGE_NAME",
            "apachepulsar/pulsar-test-latest-version:latest");

    public PulsarContainer() {
        this(DEFAULT_IMAGE_NAME);
    }

    public PulsarContainer(final String pulsarVersion) {
        super(pulsarVersion);
        withExposedPorts(BROKER_HTTP_PORT, PULSAR_PORT);
        withCommand("/pulsar/bin/pulsar standalone");
        waitingFor(new HttpWaitStrategy()
                .forPort(BROKER_HTTP_PORT)
                .forStatusCode(200)
                .forPath("/admin/v2/namespaces/public/default")
                .withStartupTimeout(Duration.of(300, SECONDS)));
    }

    public String getPlainTextPulsarBrokerUrl() {
        return String.format("pulsar://%s:%s", this.getContainerIpAddress(), this.getMappedPort(PULSAR_PORT));
    }

    public String getPulsarAdminUrl() {
        return String.format("http://%s:%s", this.getContainerIpAddress(), this.getMappedPort(BROKER_HTTP_PORT));
    }

}
