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
package org.apache.pulsar.tests.integration.transaction;

import lombok.CustomLog;
import org.apache.pulsar.tests.integration.oxia.OxiaContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;

/**
 * Runs the full {@link V5ScalableTopicTransactionTest} suite against a cluster whose metadata store
 * is a containerized Oxia instead of ZooKeeper. Oxia is the backend PIP-473 targets natively — its
 * {@code scanByIndex} / {@code subscribeSequence} / partition-key / sequence-delta primitives back the
 * {@code /txn} layout directly, rather than the scan-and-filter fallback ZooKeeper uses — so this is
 * the closest the test suite gets to the production transaction path.
 */
@CustomLog
public class V5ScalableTopicTransactionOnOxiaTest extends V5ScalableTopicTransactionTest {

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName, PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.enableOxia(true);
        return specBuilder;
    }

    @Override
    protected String configurationStoreConnectionString() {
        return "oxia://" + OxiaContainer.NAME + ":" + OxiaContainer.OXIA_PORT;
    }
}
