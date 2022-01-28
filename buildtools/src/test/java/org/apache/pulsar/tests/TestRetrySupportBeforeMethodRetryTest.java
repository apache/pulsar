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
import org.testng.IRetryAnalyzer;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestRetrySupportBeforeMethodRetryTest extends TestRetrySupport {
    public static class IllegalStateRetry implements IRetryAnalyzer {
        @Override
        public boolean retry(ITestResult result) {
            return result.getThrowable().getClass() == IllegalStateException.class;
        }
    }

    private int setupCallCount;
    private int cleanupCallCount;
    private int invocationCount;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        setupCallCount++;
        incrementSetupNumber();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        cleanupCallCount++;
        markCurrentSetupNumberCleaned();
    }

    @Test(retryAnalyzer = IllegalStateRetry.class)
    void shouldNotDoAnythingWhenThereIsBeforeAndAfterMethod(ITestContext testContext) {
        // get the number of times this method has been called
        invocationCount++;
        // setup method should have been called the same amount of times when TestRetrySupport
        // is doing the handling. setup method has @BeforeClass annotation so that TestNG calls it
        // only once
        Assert.assertEquals(setupCallCount, invocationCount);
        // cleanup method one less times
        Assert.assertEquals(cleanupCallCount, invocationCount - 1);
        // trigger a retry
        if (invocationCount < 5) {
            throw new IllegalStateException("Sample failure to trigger retry.");
        }
    }
}
