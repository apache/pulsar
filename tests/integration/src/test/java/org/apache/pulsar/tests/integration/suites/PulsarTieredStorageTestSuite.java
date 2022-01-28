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
package org.apache.pulsar.tests.integration.suites;

import static java.util.stream.Collectors.joining;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

@Slf4j
public abstract class PulsarTieredStorageTestSuite extends PulsarClusterTestBase {

    protected static final int ENTRIES_PER_LEDGER = 1024;

    @BeforeClass(alwaysRun = true)
    @Override
    public final void setupCluster() throws Exception {
        final String clusterName = Stream.of(this.getClass().getSimpleName(), randomName(5))
                .filter(s -> s != null && !s.isEmpty())
                .collect(joining("-"));

        PulsarClusterSpec spec = PulsarClusterSpec.builder()
            .numBookies(2)
            .numBrokers(1)
            .clusterName(clusterName)
            .build();

        setupCluster(spec);
    }

    @AfterClass(alwaysRun = true)
    @Override
    public final void tearDownCluster() throws Exception {
        super.tearDownCluster();
    }

    protected abstract Map<String, String> getEnv();

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            getEnv().forEach(brokerContainer::withEnv);
        }
    }
}
