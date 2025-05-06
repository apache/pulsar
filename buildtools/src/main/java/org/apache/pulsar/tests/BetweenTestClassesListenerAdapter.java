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

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IClass;
import org.testng.IClassListener;
import org.testng.IConfigurationListener;
import org.testng.ITestClass;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;

/**
 * TestNG listener adapter that detects when execution finishes in a test class, including AfterClass methods.
 * TestNG's IClassListener.onAfterClass method is called before AfterClass methods are executed,
 * which is why this solution is needed.
 */
abstract class BetweenTestClassesListenerAdapter implements IClassListener, IConfigurationListener {
    private static final Logger log = LoggerFactory.getLogger(BetweenTestClassesListenerAdapter.class);
    volatile Class<?> currentTestClass;
    volatile int remainingAfterClassMethodCount;

    @Override
    public final void onBeforeClass(ITestClass testClass) {
        // for parameterized tests for the same class, the onBeforeClass method is called for each instance
        // so we need to check if the test class is the same as for the previous call before resetting the counter
        if (testClass.getRealClass() != currentTestClass) {
            // find out how many parameterized instances of the test class are expected
            Object[] instances = testClass.getInstances(false);
            int instanceCount = instances != null && instances.length != 0 ? instances.length : 1;
            // expect invocations of all annotated and enabled after class methods
            int annotatedAfterClassMethodCount = (int) Arrays.stream(testClass.getAfterClassMethods())
                    .filter(ITestNGMethod::getEnabled)
                    .count();
            // additionally expect invocations of the "onAfterClass" listener method in this class
            int expectedMethodCountForEachInstance = 1 + annotatedAfterClassMethodCount;
            // multiple by the number of instances
            remainingAfterClassMethodCount = instanceCount * expectedMethodCountForEachInstance;
            currentTestClass = testClass.getRealClass();
        }
    }

    @Override
    public final void onAfterClass(ITestClass testClass) {
        handleAfterClassMethodCalled(testClass);
    }

    @Override
    public final void onConfigurationSuccess(ITestResult tr) {
        handleAfterClassConfigurationMethodCompletion(tr);
    }

    @Override
    public final void onConfigurationSkip(ITestResult tr) {
        handleAfterClassConfigurationMethodCompletion(tr);
    }

    @Override
    public final void onConfigurationFailure(ITestResult tr) {
        handleAfterClassConfigurationMethodCompletion(tr);
    }

    private void handleAfterClassConfigurationMethodCompletion(ITestResult tr) {
        if (tr.getMethod().isAfterClassConfiguration() && !tr.wasRetried()) {
            handleAfterClassMethodCalled(tr.getTestClass());
        }
    }

    private void handleAfterClassMethodCalled(IClass testClass) {
        if (currentTestClass != testClass.getRealClass()) {
            log.error("Unexpected test class: {}. Expected: {}", testClass.getRealClass(), currentTestClass);
            return;
        }
        remainingAfterClassMethodCount--;
        if (remainingAfterClassMethodCount == 0) {
            onBetweenTestClasses(testClass);
        } else if (remainingAfterClassMethodCount < 0) {
            // unexpected case, log it for easier debugging if this causes test failures
            log.error("Remaining after class method count is negative: {} for test class: {}",
                    remainingAfterClassMethodCount, testClass.getRealClass());
        }
    }

    /**
     * Call back hook for adding logic when test execution has completely finished for a test class.
     */
    protected abstract void onBetweenTestClasses(IClass testClass);
}
