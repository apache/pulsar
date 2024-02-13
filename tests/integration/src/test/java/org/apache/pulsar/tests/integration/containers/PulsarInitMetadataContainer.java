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

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/**
 * Initialize the Pulsar metadata
 */
@Slf4j
public class PulsarInitMetadataContainer extends GenericContainer<PulsarInitMetadataContainer> {

    public static final String NAME = "init-metadata";

    private final String clusterName;
    private final String metadataStoreUrl;
    private final String configurationMetadataStoreUrl;
    private final String brokerHostname;

    public PulsarInitMetadataContainer(Network network,
                                       String clusterName,
                                       String metadataStoreUrl,
                                       String configurationMetadataStoreUrl,
                                       String brokerHostname) {
        this.clusterName = clusterName;
        this.metadataStoreUrl = metadataStoreUrl;
        this.configurationMetadataStoreUrl = configurationMetadataStoreUrl;
        this.brokerHostname = brokerHostname;
        setDockerImageName(PulsarContainer.DEFAULT_IMAGE_NAME);
        withNetwork(network);

        setCommand("sleep 1000000");
    }


    public void initialize() throws Exception {
        start();
        ExecResult res = this.execInContainer(
                "/pulsar/bin/pulsar", "initialize-cluster-metadata",
                "--cluster", clusterName,
                "--metadata-store", metadataStoreUrl,
                "--configuration-metadata-store", configurationMetadataStoreUrl,
                "--web-service-url", "http://" + brokerHostname + ":8080/",
                "--broker-service-url", "pulsar://" + brokerHostname + ":6650/"
        );

        if (res.getExitCode() == 0) {
            log.info("Successfully initialized cluster");
        } else {
            log.warn("Failed to initialize Pulsar cluster. exit code: " + res.getExitCode());
            log.warn("STDOUT: " + res.getStdout());
            log.warn("STDERR: " + res.getStderr());
            throw new IOException("Failed to initialized Pulsar Cluster");
        }
    }
}
