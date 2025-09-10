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
package org.apache.pulsar.tests.integration.functions;

import static org.testng.Assert.assertEquals;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.state.PulsarMetadataStateStoreProviderImpl;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.StandaloneContainer;
import org.apache.pulsar.tests.integration.docker.ContainerExecResult;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testcontainers.containers.Network;

@Slf4j
public class PulsarMetadataStateStoreTest extends PulsarStateTest {
    protected PulsarMetadataStateStoreTest() {
        super(PulsarMetadataStateStoreProviderImpl.class.getName());
    }

    public void setUpCluster() throws Exception {
        incrementSetupNumber();
        network = Network.newNetwork();
        String clusterName = PulsarClusterTestBase.randomName(8);
        container = new StandaloneContainer(clusterName, PulsarContainer.DEFAULT_IMAGE_NAME)
                .withNetwork(network)
                .withNetworkAliases(StandaloneContainer.NAME + "-" + clusterName)
                .withEnv("PULSAR_STANDALONE_USE_ZOOKEEPER", "true")
                .withEnv("PF_stateStorageProviderImplementation", PulsarMetadataStateStoreProviderImpl.class.getName())
                .withEnv("PF_stateStorageServiceUrl", "zk:localhost:2181");
        container.start();
        log.info("Pulsar cluster {} is up running:", clusterName);
        log.info("\tBinary Service Url : {}", container.getPlainTextServiceUrl());
        log.info("\tHttp Service Url : {}", container.getHttpServiceUrl());

        // add cluster to public tenant
        ContainerExecResult result = container.execCmd(
                "/pulsar/bin/pulsar-admin", "namespaces", "policies", "public/default");
        assertEquals(0, result.getExitCode());
        log.info("public/default namespace policies are {}", result.getStdout());
    }


}

