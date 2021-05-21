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

import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.tests.integration.containers.S3Container;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class TestUniversalConfigurations extends TestBaseOffload {

    private S3Container s3Container;

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();

        log.info("s3 container init");
        s3Container = new S3Container(
                pulsarCluster.getClusterName(),
                S3Container.NAME)
                .withNetwork(pulsarCluster.getNetwork())
                .withNetworkAliases(S3Container.NAME);
        s3Container.start();
        log.info("s3 container start finish.");
    }

    @AfterClass(alwaysRun = true)
    public void teardownS3() {
        if (null != s3Container) {
            s3Container.stop();
        }
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaCLI(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeViaCLI(serviceUrl.get(), adminUrl.get());
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeViaThreshold(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeViaThreshold(serviceUrl.get(), adminUrl.get());
    }

    @Test(dataProvider =  "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeDeletionLag(Supplier<String> serviceUrl, Supplier<String> adminUrl) throws Exception {
        super.testPublishOffloadAndConsumeDeletionLag(serviceUrl.get(), adminUrl.get());

    }

    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(ENTRIES_PER_LEDGER));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "aws-s3");
        result.put("managedLedgerOffloadBucket", "pulsar-integtest");
        result.put("managedLedgerOffloadServiceEndpoint", "http://" + S3Container.NAME + ":9090");

        return result;
    }

}
