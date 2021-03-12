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
package org.apache.pulsar.tests;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests that TestRetrySupport doesn't call setup & cleanup when tests are successful
 */
public class TestRetrySupportSuccessTest extends TestRetrySupport {
    private int setupCallCount;
    private int cleanupCallCount;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        setupCallCount++;
        incrementSetupNumber();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        cleanupCallCount++;
        markCurrentSetupNumberCleaned();
    }

    @Test
    void shouldCallSetupOnce1() {
        Assert.assertEquals(setupCallCount, 1);
        Assert.assertEquals(cleanupCallCount, 0);
    }

    @Test
    void shouldCallSetupOnce2() {
        Assert.assertEquals(setupCallCount, 1);
        Assert.assertEquals(cleanupCallCount, 0);
    }

    @Test
    void shouldCallSetupOnce3() {
        Assert.assertEquals(setupCallCount, 1);
        Assert.assertEquals(cleanupCallCount, 0);
    }
}
