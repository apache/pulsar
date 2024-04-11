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
package org.apache.pulsar.common.naming;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.junit.Assert;
import org.testng.annotations.Test;

@Test(groups = "broker-naming")
public class ServiceConfigurationCheckOnBrokerStartingTest extends MockedPulsarServiceBaseTest {

    private int deduplicationSnapshotFrequency = 120;

    @Override
    protected void setup() throws Exception {}

    protected void setupInternal() throws Exception {
        super.internalSetup();
    }

    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    protected void doInitConf() throws Exception {
        this.conf.setBrokerDeduplicationSnapshotFrequencyInSeconds(deduplicationSnapshotFrequency);
    }

    @Test
    public void testInvalidDeduplicationSnapshotFrequency() throws Exception {
        cleanup();
        // set a invalidate value.
        deduplicationSnapshotFrequency = 0;
        try {
            setupInternal();
            Assert.fail("the config brokerDeduplicationSnapshotFrequencyInSeconds is 0, the check should not pass");
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().contains("greater than 0"));
        }
    }
}
