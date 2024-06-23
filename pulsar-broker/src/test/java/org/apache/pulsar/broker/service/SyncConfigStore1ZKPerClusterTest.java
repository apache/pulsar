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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class SyncConfigStore1ZKPerClusterTest extends SyncConfigStore2ZKPerClusterTest {

    @Override
    @BeforeClass(alwaysRun = true, timeOut = 300000)
    public void setup() throws Exception {
        super.setup();
    }

    @Override
    @AfterClass(alwaysRun = true, timeOut = 300000)
    public void cleanup() throws Exception {
        super.cleanup();
    }

    @Override
    protected void tryInitBrokerConfigZK() {
        // Init nothing, the super class will use the local metadata ZK URL as config metadata ZK URL.
    }

    @Override
    protected void verifyMetadataStores() {
        Awaitility.await().untilAsserted(() -> {
            // Verify: config metadata store url is the same as local metadata store url.
            assertFalse(pulsar1.getConfig().isConfigurationStoreSeparated());
            assertFalse(pulsar2.getConfig().isConfigurationStoreSeparated());
            // Verify: Pulsar initialized itself to update the metadata synchronizer dynamically.
            assertTrue(pulsar1.hasConditionOfDynamicUpdateConf("configurationMetadataSyncEventTopic")
                    .getLeft());
            assertTrue(pulsar2.hasConditionOfDynamicUpdateConf("configurationMetadataSyncEventTopic")
                    .getLeft());
            assertTrue(pulsar1.hasConditionOfDynamicUpdateConf("metadataSyncEventTopic")
                    .getLeft());
            assertTrue(pulsar2.hasConditionOfDynamicUpdateConf("metadataSyncEventTopic")
                    .getLeft());
        });
    }

    @Test
    public void testDynamicEnableConfigurationMetadataSyncEventTopic() throws Exception {
        super.testDynamicEnableConfigurationMetadataSyncEventTopic();
    }
}
