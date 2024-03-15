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

import java.time.Duration;
import org.apache.http.HttpStatus;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.MountableFile;

public class OpenTelemetryCollectorContainer extends ChaosContainer<OpenTelemetryCollectorContainer> {

    private static final String IMAGE_NAME = "otel/opentelemetry-collector-contrib:latest";
    private static final String NAME = "otel-collector";

    public static final int PROMETHEUS_EXPORTER_PORT = 8889;
    private static final int OTLP_RECEIVER_PORT = 4317;
    private static final int ZPAGES_PORT = 55679;

    public OpenTelemetryCollectorContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    protected void configure() {
        super.configure();

        this.withCopyFileToContainer(
                MountableFile.forClasspathResource("containers/otel-collector-config.yaml", 0644),
                "/etc/otel-collector-config.yaml")
            .withCommand("--config=/etc/otel-collector-config.yaml")
            .withExposedPorts(OTLP_RECEIVER_PORT, PROMETHEUS_EXPORTER_PORT, ZPAGES_PORT)
            .waitingFor(new HttpWaitStrategy()
                    .forPath("/debug/servicez")
                    .forPort(ZPAGES_PORT)
                    .forStatusCode(HttpStatus.SC_OK)
                    .withStartupTimeout(Duration.ofSeconds(300)));
    }

    @Override
    public String getContainerName() {
        return clusterName + "-" + NAME;
    }

    public String getOtlpEndpoint() {
        return String.format("http://%s:%d", NAME, OTLP_RECEIVER_PORT);
    }
}
