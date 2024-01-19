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

import org.testcontainers.utility.MountableFile;

public class PrometheusContainer extends ChaosContainer<PrometheusContainer> {

    private static final String IMAGE_NAME = "prom/prometheus:latest";
    private static final String NAME = "prometheus";
    private static final int PROMETHEUS_PORT = 9090;

    public PrometheusContainer(String clusterName) {
        super(clusterName, IMAGE_NAME);
    }

    @Override
    protected void configure() {
        super.configure();

        this.withNetworkAliases(NAME)
            .withCopyToContainer(
                MountableFile.forClasspathResource("containers/prometheus.yml"),
                "/etc/prometheus/prometheus.yml")
            .withExposedPorts(PROMETHEUS_PORT);
    }
}
