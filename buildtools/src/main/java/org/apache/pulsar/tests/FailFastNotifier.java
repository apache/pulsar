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

import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestListener;
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
    private static final boolean FAIL_FAST_ENABLED = Boolean.parseBoolean(
            System.getProperty("testFailFast", "true"));

    static class FailFastEventsSingleton {
        private static final FailFastEventsSingleton INSTANCE = new FailFastEventsSingleton();

        private volatile boolean skipAfterFailure;

        private FailFastEventsSingleton() {
        }

        public static FailFastEventsSingleton getInstance() {
            return INSTANCE;
        }

        public boolean isSkipAfterFailure() {
            return skipAfterFailure;
        }

        public void setSkipOnNextTest() {
            this.skipAfterFailure = true;
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
        FailFastNotifier.FailFastEventsSingleton.getInstance().setSkipOnNextTest();
        // Hide FailFastSkipExceptions and mark the test as skipped
        if (result.getThrowable() instanceof FailFastSkipException) {
            result.setThrowable(null);
            result.setStatus(ITestResult.SKIP);
        }
    }

    @Override
    public void beforeInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {
        if (FAIL_FAST_ENABLED && FailFastEventsSingleton.getInstance().isSkipAfterFailure()) {
            throw new FailFastSkipException("Skipped after failure since testFailFast system property is set.");
        }
    }

    @Override
    public void afterInvocation(IInvokedMethod iInvokedMethod, ITestResult iTestResult) {

    }
}
