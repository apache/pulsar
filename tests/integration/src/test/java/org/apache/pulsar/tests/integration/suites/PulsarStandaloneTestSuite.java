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

import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarStandaloneTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class PulsarStandaloneTestSuite extends PulsarStandaloneTestBase {
    private final String imageName;

    protected PulsarStandaloneTestSuite() {
        this(PulsarContainer.DEFAULT_IMAGE_NAME);
    }

    protected PulsarStandaloneTestSuite(String imageName) {
        this.imageName = imageName;
    }

    public void setUpCluster() throws Exception {
        incrementSetupNumber();
        super.startCluster(imageName);
    }

    public void tearDownCluster() throws Exception {
        markCurrentSetupNumberCleaned();
        super.stopCluster();
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected final void setup() throws Exception {
        setUpCluster();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected final void cleanup() throws Exception {
        tearDownCluster();
    }

}
