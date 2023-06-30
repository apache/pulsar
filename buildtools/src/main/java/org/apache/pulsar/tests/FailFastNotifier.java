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
package org.apache.pulsar.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.SkipException;

/**
 * Notifies TestNG core skipping remaining tests after first failure has appeared.
 *
 * Enabled when -DtestFailFast=true
 *
 * This is a workaround for https://issues.apache.org/jira/browse/SUREFIRE-1762 since
 * the bug makes the built-in fast-fast feature `-Dsurefire.skipAfterFailureCount=1` unusable.
 * Maven Surefire version 3.0.0-M5 contains the fix, but that version is unusable because of problems
 * with test output, https://issues.apache.org/jira/browse/SUREFIRE-1827.
 * It makes the Pulsar integration tests slow and to fail.
 *
 * This implementation is based on org.apache.maven.surefire.testng.utils.FailFastNotifier
 * implementation that is part of the Maven Surefire plugin.
 *
 */
public class FailFastNotifier
        implements IInvokedMethodListener, ITestListener {
    private static final Logger LOG = LoggerFactory.getLogger(FailFastNotifier.class);
    private static final String PROPERTY_NAME_TEST_FAIL_FAST = "testFailFast";
    private static final boolean FAIL_FAST_ENABLED = Boolean.parseBoolean(
            System.getProperty(PROPERTY_NAME_TEST_FAIL_FAST, "true"));

    private static final String PROPERTY_NAME_TEST_FAIL_FAST_FILE = "testFailFastFile";

    // A file that is used to communicate to other parallel forked test processes to terminate the build
    // so that fail fast mode works with multiple forked test processes
    private static final File FAIL_FAST_KILLSWITCH_FILE =
            System.getProperty(PROPERTY_NAME_TEST_FAIL_FAST_FILE) != null
                    && System.getProperty(PROPERTY_NAME_TEST_FAIL_FAST_FILE).trim().length() > 0
                    ? new File(System.getProperty(PROPERTY_NAME_TEST_FAIL_FAST_FILE).trim()) : null;

    static class FailFastEventsSingleton {
        private static final FailFastEventsSingleton INSTANCE = new FailFastEventsSingleton();

        private volatile ITestResult firstFailure;

        private FailFastEventsSingleton() {
        }

        public static FailFastEventsSingleton getInstance() {
            return INSTANCE;
        }

        public ITestResult getFirstFailure() {
            return firstFailure;
        }

        public void testFailed(ITestResult result) {
            if (this.firstFailure == null) {
                this.firstFailure = result;
                if (FAIL_FAST_KILLSWITCH_FILE != null && !FAIL_FAST_KILLSWITCH_FILE.exists()) {
                    try {
                        Files.createFile(FAIL_FAST_KILLSWITCH_FILE.toPath());
                    } catch (IOException e) {
                        LOG.warn("Unable to create fail fast kill switch file '"
                                + FAIL_FAST_KILLSWITCH_FILE.getAbsolutePath() + "'", e);
                    }
                }
            }
        }
    }

    static class FailFastSkipException extends SkipException {
        FailFastSkipException(String skipMessage) {
            super(skipMessage);
            reduceStackTrace();
        }
    }

    @Override
    public void onTestFailure(ITestResult result) {
        // Hide FailFastSkipExceptions and mark the test as skipped
        if (result.getThrowable() instanceof FailFastSkipException) {
            result.setThrowable(null);
            result.setStatus(ITestResult.SKIP);
        } else {
            FailFastNotifier.FailFastEventsSingleton.getInstance().testFailed(result);
        }
    }

    @Override
    public void beforeInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
        if (FAIL_FAST_ENABLED) {
            ITestResult firstFailure = FailFastEventsSingleton.getInstance().getFirstFailure();
            if (firstFailure != null) {
                ITestNGMethod iTestNGMethod = iInvokedMethod.getTestMethod();
                // condition that ensures that cleanup methods will be called in the test class where the
                // first exception happened
                if (iTestResult.getInstance() != firstFailure.getInstance()
                        || !(iTestNGMethod.isAfterMethodConfiguration()
                        || iTestNGMethod.isAfterClassConfiguration()
                        || iTestNGMethod.isAfterTestConfiguration())) {
                    throw new FailFastSkipException("Skipped after failure since testFailFast system property is set.");
                }
            } else if (FAIL_FAST_KILLSWITCH_FILE != null && FAIL_FAST_KILLSWITCH_FILE.exists()) {
                throw new FailFastSkipException("Skipped after failure since kill switch file exists.");
            }
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {

    }
}
