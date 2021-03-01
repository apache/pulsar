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
package org.apache.pulsar.tests.integration.backwardscompatibility;

import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarStandaloneTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class PulsarStandaloneTestSuite2_5 extends PulsarStandaloneTestBase {

    @BeforeClass
    public void setUpCluster() throws Exception {
        super.startCluster(PulsarContainer.PULSAR_2_5_IMAGE_NAME);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownCluster() throws Exception {
        super.stopCluster();
    }

}
