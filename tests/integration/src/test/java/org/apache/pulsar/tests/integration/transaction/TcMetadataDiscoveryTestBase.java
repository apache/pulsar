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
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;

/**
 * Base for the metadata-store transaction-coordinator discovery tests. Brings up a multi-broker
 * cluster with the scalable-topics transaction coordinator enabled, so leadership of the TC
 * partitions is established via the metadata-store election and clients discover coordinators via
 * the {@code CommandWatchTcAssignments} stream rather than the assign-topic lookup.
 *
 * <p>Scope note: only the transaction-coordinator <em>control plane</em> is enabled here. Producing
 * / acking data inside a transaction additionally requires the scalable-topic transaction buffer
 * and pending-ack providers (and {@code segment://} topics), which land with the default flip. These
 * tests therefore exercise the transaction <em>lifecycle</em> (newTransaction / commit / abort) over
 * the discovered connections — which is exactly the surface the new client discovery path drives.
 */
@CustomLog
public abstract class TcMetadataDiscoveryTestBase extends PulsarTestSuite {

    /** Number of TC partitions; small so leadership spreads predictably across the brokers. */
    protected static final int TC_PARALLELISM = 4;

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            // transactionCoordinatorEnabled is present in broker.conf, so the bare env var name
            // overrides it. The two scalable-topics settings are NOT in broker.conf, so they must
            // use the PULSAR_PREFIX_ prefix to be appended as new config keys by
            // apply-config-from-env.py — otherwise a bare name is silently ignored.
            brokerContainer.withEnv("transactionCoordinatorEnabled", "true");
            brokerContainer.withEnv("PULSAR_PREFIX_transactionCoordinatorScalableTopicsEnabled", "true");
            brokerContainer.withEnv("PULSAR_PREFIX_transactionCoordinatorScalableTopicsParallelism",
                    Integer.toString(TC_PARALLELISM));
        }
    }

    @Override
    public void setupCluster() throws Exception {
        super.setupCluster();
        // The assign-topic partitioned metadata is still created so the legacy ownership-based
        // fallback in handleClientConnect remains valid during the deprecation window.
        BrokerContainer brokerContainer = pulsarCluster.getBrokers().iterator().next();
        brokerContainer.execCmd(
                "/pulsar/bin/pulsar", "initialize-transaction-coordinator-metadata",
                "-cs", configurationStoreConnectionString(),
                "-c", pulsarCluster.getClusterName(),
                "--initial-num-transaction-coordinators", Integer.toString(TC_PARALLELISM));
    }

    /**
     * Configuration-store connection string used to initialize the transaction-coordinator metadata.
     * Defaults to the ZooKeeper container; an Oxia-backed subclass overrides this with the Oxia URL.
     */
    protected String configurationStoreConnectionString() {
        return ZKContainer.NAME;
    }
}
