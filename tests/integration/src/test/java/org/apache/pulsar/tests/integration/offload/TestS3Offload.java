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
package org.apache.pulsar.tests.integration.offload;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class TestS3Offload extends TestBaseOffload {

    private S3Container s3Container;

    @BeforeClass
    public void setupS3() {
        s3Container = new S3Container(
                pulsarCluster.getClusterName(),
                S3Container.NAME)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(S3Container.NAME);
        s3Container.start();
    }

    @AfterClass
    public void teardownS3() {
        if (null != s3Container) {
            s3Container.stop();
        }
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaCLI(String serviceUrl, String adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeViaCLI(serviceUrl, adminUrl);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaThreshold(String serviceUrl, String adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeViaThreshold(serviceUrl, adminUrl);
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeDeletionLag(String serviceUrl, String adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeDeletionLag(serviceUrl, adminUrl);

    }


    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "s3");
        result.put("s3ManagedLedgerOffloadBucket", "pulsar-integtest");
        result.put("s3ManagedLedgerOffloadServiceEndpoint", "http://" + S3Container.NAME + ":9090");

        return result;
    }


}
