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

import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

/**
 * Adds support for resetting the internal state of the test
 * by calling "cleanup" and "setup" methods before running a test method
 * after a previous test method has failed.
 *
 * This is useful for making test retries to work on classes which use BeforeClass
 * and AfterClass methods to setup a test environment that is shared across all test methods in the test
 * class.
 */
public abstract class TestRetrySupport {
    private static final Logger LOG = LoggerFactory.getLogger(TestRetrySupport.class);
    private int currentSetupNumber;
    private int failedSetupNumber = -1;
    private int cleanedUpSetupNumber;

    @BeforeMethod(groups = { "setup", "flaky", "extra" })
    public final void stateCheck(Method method) throws Exception {
        // run cleanup and setup if the current setup number is the one where a failure happened
        // this is to cleanup state before retrying
        if (currentSetupNumber == failedSetupNumber
                && cleanedUpSetupNumber != failedSetupNumber) {
            LOG.info("Previous test run has failed before {}.{}, failedSetupNumber={}. Running cleanup and setup.",
                    method.getDeclaringClass().getSimpleName(), method.getName(), failedSetupNumber);
            try {
                cleanup();
            } catch (Exception e) {
                LOG.error("Cleanup failed, ignoring this.", e);
            }
            setup();
            LOG.info("State cleanup finished.");
            failedSetupNumber = -1;
        }
    }

    @AfterMethod(alwaysRun = true, groups = { "setup", "flaky", "extra" })
    public final void failureCheck(ITestResult testResult, Method method) {
        // track the setup number where the failure happened
        if (!testResult.isSuccess()) {
            LOG.info("Detected test failure in test {}.{}, currentSetupNumber={}",
                    method.getDeclaringClass().getSimpleName(), method.getName(),
                    currentSetupNumber);
            failedSetupNumber = currentSetupNumber;
        }
    }

    /**
     * This method should be called in the setup method of the concrete class.
     *
     * This increases an internal counter and resets the failure state which are used to determine
     * whether cleanup is needed before a test method is called.
     *
     */
    protected final void incrementSetupNumber() {
        currentSetupNumber++;
        failedSetupNumber = -1;
        LOG.debug("currentSetupNumber={}", currentSetupNumber);
    }

    protected final void markCurrentSetupNumberCleaned() {
        cleanedUpSetupNumber = currentSetupNumber;
        LOG.debug("cleanedUpSetupNumber={}", cleanedUpSetupNumber);
    }

    protected abstract void setup() throws Exception;

    protected abstract void cleanup() throws Exception;
}
