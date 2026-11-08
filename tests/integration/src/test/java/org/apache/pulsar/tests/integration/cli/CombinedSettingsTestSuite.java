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
package org.apache.pulsar.tests.integration.cli;

import java.util.Map;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;

/**
 * The cluster that the CLI test classes which need broker or bookie settings of their own share
 * ({@link #isClusterShared()}). It has the settings of all of them: each is there for one class and makes no
 * difference to the others, so that the classes start one cluster rather than one each.
 */
public abstract class CombinedSettingsTestSuite extends PulsarTestSuite {

    // AnalyzeBacklogTest: a scan of a subscription's backlog reads at most this many entries, so that a backlog of a
    // few dozen entries takes several scans
    public static final int SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES = 10;

    @Override
    protected final boolean isClusterShared() {
        return true;
    }

    @Override
    protected final PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName, PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        return specBuilder
                .brokerEnvs(Map.of(
                        "PULSAR_PREFIX_subscriptionBacklogScanMaxEntries",
                        String.valueOf(SUBSCRIPTION_BACKLOG_SCAN_MAX_ENTRIES),
                        // PackagesCliTest
                        "enablePackagesManagement", "true",
                        "packagesManagementLedgerRootPath", "/ledgers",
                        // HealthCheckTest: with ZooKeeper paused, a check fails once the broker gives up loading its
                        // health check topic: after 5 s rather than 60 s, so that ZooKeeper resumes before any
                        // session in the cluster expires
                        "PULSAR_PREFIX_topicLoadTimeoutSeconds", "5",
                        // HealthCheckTest: with the bookies paused, a check fails once the broker's client gives up
                        // creating the check's producer, whose schema write times out: after about 10 s rather than
                        // 30 s. The client fails the producer on the first error after its operation timeout, and
                        // the write's timeout makes that error come then rather than at the client's next check of
                        // its requests' timeouts, which can be up to an operation timeout later.
                        "PULSAR_PREFIX_brokerClient_operationTimeoutMs", "10000",
                        "PULSAR_PREFIX_bookkeeperClientTimeoutInSeconds", "10"))
                // HealthCheckTest: a bookie that stays paused for longer than its ZooKeeper session registers again
                // when it resumes, but its AutoRecovery daemon stops on the expired session and shuts the bookie down
                .bookkeeperEnvs(Map.of("autoRecoveryDaemonEnabled", "false"));
    }
}
