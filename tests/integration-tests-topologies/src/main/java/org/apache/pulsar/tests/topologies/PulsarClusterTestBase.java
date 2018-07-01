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
package org.apache.pulsar.tests.topologies;

import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public class PulsarClusterTestBase {

    protected static PulsarCluster pulsarCluster;

    @BeforeClass
    public static void setupCluster() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 8; i++) {
            sb.append((char) (ThreadLocalRandom.current().nextInt(26) + 'a'));
        }
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .clusterName(sb.toString())
            .build();

        setupCluster(spec);
    }

    protected static void setupCluster(PulsarClusterSpec spec) throws Exception {
        log.info("Setting up cluster {} with {} bookies, {} brokers",
            spec.clusterName(), spec.numBookies(), spec.numBrokers());

        pulsarCluster = PulsarCluster.forSpec(spec);
        pulsarCluster.start();

        log.info("Cluster {} is setup", spec.clusterName());
    }

    @AfterClass
    public static void teardownCluster() {
        if (null != pulsarCluster) {
            pulsarCluster.stop();
        }
    }

}
