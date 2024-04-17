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
package org.apache.pulsar.tests.integration.backwardscompatibility;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.offload.TestBaseOffload;
import org.testng.annotations.Test;

public class ClientOffloadTest3_0 extends TestBaseOffload {
    @Override
    protected Map<String, String> getEnv() {
        Map<String, String> result = new HashMap<>();
        result.put("managedLedgerMaxEntriesPerLedger", String.valueOf(getNumEntriesPerLedger()));
        result.put("managedLedgerMinLedgerRolloverTimeMinutes", "0");
        result.put("managedLedgerOffloadDriver", "filesystem");
        result.put("fileSystemURI", "file:///tmp");

        return result;
    }

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        for (BrokerContainer brokerContainer : pulsarCluster.getBrokers()) {
            brokerContainer.setDockerImageName(PulsarContainer.PULSAR_3_0_IMAGE_NAME);
        }
    }

    @Test(dataProvider = "ServiceAndAdminUrls")
    public void testPublishOffloadAndConsumeDeletionLag(Supplier<String> serviceUrl, Supplier<String> adminUrl)
            throws Exception {
        super.testPublishOffloadAndConsumeDeletionLag(serviceUrl.get(), adminUrl.get());
    }
}
