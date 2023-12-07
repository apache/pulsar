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
package org.apache.pulsar.tests.integration.topologies;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.testcontainers.containers.Network;

@Slf4j
public class PulsarGeoCluster {

    @Getter
    private final PulsarClusterSpec[] clusterSpecs;

    @Getter
    private final CSContainer[] csContainers;

    @Getter
    private final PulsarCluster[] clusters;

    /**
     * Pulsar Cluster Spec
     *
     * @param specs each pulsar cluster spec.
     * @return the built a pulsar cluster with geo replication
     */
    public static PulsarGeoCluster forSpec(PulsarClusterSpec... specs) {
        return new PulsarGeoCluster(specs);
    }

    public PulsarGeoCluster(PulsarClusterSpec... clusterSpecs) {
        this.clusterSpecs = clusterSpecs;
        this.clusters = new PulsarCluster[clusterSpecs.length];
        this.csContainers = new CSContainer[clusterSpecs.length];

        Network network = Network.newNetwork();
        for (int i = 0; i < this.clusters.length; i++) {
            String csName = "configuration-store-" + i;
            CSContainer csContainer = new CSContainer(csName, csName)
                    .withNetwork(network)
                    .withNetworkAliases(CSContainer.NAME);
            csContainers[i] = csContainer;
            clusters[i] = PulsarCluster.forSpec(this.clusterSpecs[i], csContainer);
        }
    }

    public void start() throws Exception {
        // start the configuration store
        for (CSContainer csContainer : csContainers) {
            csContainer.start();
            log.info("Successfully started configuration store container for cluster {}.", csContainer.getClusterName());
        }

        for (PulsarCluster cluster : clusters) {
            cluster.start();
            log.info("Successfully started all components for cluster {}.", cluster.getClusterName());
        }
    }

    public void stop() throws Exception {
        for (PulsarCluster cluster : clusters) {
            cluster.stop();
            log.info("Successfully stopped all components for cluster {}.", cluster.getClusterName());
        }
        // stop the configuration store
        for (CSContainer csContainer : csContainers) {
            csContainer.stop();
            log.info("Successfully stopped configuration store container for cluster {}.",
                    csContainer.getClusterName());
        }
    }

}
