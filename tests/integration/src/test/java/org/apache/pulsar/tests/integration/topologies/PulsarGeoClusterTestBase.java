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
package org.apache.pulsar.tests.integration.topologies;

import static java.util.stream.Collectors.joining;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PulsarGeoClusterTestBase extends PulsarTestBase {

    @Override
    protected final void setup() throws Exception {
        setupCluster();
    }

    @Override
    protected final void cleanup() throws Exception {
        tearDownCluster();
    }

    protected void setupCluster() throws Exception {
        this.setupCluster("");
    }

    @Getter
    private PulsarGeoCluster geoCluster;

    public void setupCluster(String namePrefix) throws Exception {
        PulsarClusterSpec.PulsarClusterSpecBuilder[] specBuilders = new PulsarClusterSpec.PulsarClusterSpecBuilder[2];
        for (int i = 0; i < 2; i++) {
            String clusterName = Stream.of(this.getClass().getSimpleName(), namePrefix, String.valueOf(i),
                            randomName(5))
                    .filter(s -> s != null && !s.isEmpty())
                    .collect(joining("-"));
            specBuilders[i] = PulsarClusterSpec.builder().clusterName(clusterName);
        }
        specBuilders = beforeSetupCluster(specBuilders);
        PulsarClusterSpec[] specs = new PulsarClusterSpec[2];
        for (int i = 0; i < specBuilders.length; i++) {
            specs[i] = specBuilders[i].build();
        }
        setupCluster0(specs);
    }

    protected PulsarClusterSpec.PulsarClusterSpecBuilder[] beforeSetupCluster (
            PulsarClusterSpec.PulsarClusterSpecBuilder... specBuilder) {
        return specBuilder;
    }

    protected void setupCluster0(PulsarClusterSpec... specs) throws Exception {
        incrementSetupNumber();
        log.info("Setting up geo cluster with {} local clusters}", specs.length);

        this.geoCluster = PulsarGeoCluster.forSpec(specs);

        beforeStartCluster();

        this.geoCluster.start();

        log.info("Geo Cluster is setup!");
    }

    protected void beforeStartCluster() throws Exception {
        // no-op
    }

    public void tearDownCluster() throws Exception {
        markCurrentSetupNumberCleaned();
        if (null != this.geoCluster) {
            this.geoCluster.stop();
        }
    }
}
