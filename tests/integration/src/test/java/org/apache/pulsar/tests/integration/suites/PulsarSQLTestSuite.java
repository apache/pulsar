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

import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;

@Slf4j
public abstract class PulsarSQLTestSuite extends PulsarTestSuite {

    public static final int ENTRIES_PER_LEDGER = 100;
    public static final String OFFLOAD_DRIVER = "aws-s3";
    public static final String BUCKET = "pulsar-integtest";
    public static final String ENDPOINT = "http://" + S3Container.NAME + ":9090";

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName, PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {
        specBuilder.queryLastMessage(true);
        specBuilder.clusterName("pulsar-sql-test");
        specBuilder.numBrokers(1);
        return super.beforeSetupCluster(clusterName, specBuilder);
    }

    @Override
    protected void beforeStartCluster() throws Exception {
        Map<String, String> envMap = new HashMap<>();
        envMap.put("managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
        envMap.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        envMap.put("managedLedgerOffloadDriver", OFFLOAD_DRIVER);
        envMap.put("s3ManagedLedgerOffloadBucket", BUCKET);
        envMap.put("s3ManagedLedgerOffloadServiceEndpoint", ENDPOINT);

        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            brokerContainer.withEnv(envMap);
        }
    }

}
