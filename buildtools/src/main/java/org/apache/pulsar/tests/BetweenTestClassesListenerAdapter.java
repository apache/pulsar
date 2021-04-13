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

import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;

/**
 * TestNG listener adapter for detecting when execution finishes in previous
 * test class and starts in a new class.
 */
abstract class BetweenTestClassesListenerAdapter implements IClassListener, ITestListener {
    Class<?> lastTestClass;

    @Override
    public void onBeforeClass(ITestClass testClass) {
        checkIfTestClassChanged(testClass.getRealClass());
    }

    private void checkIfTestClassChanged(Class<?> testClazz) {
        if (lastTestClass != testClazz) {
            onBetweenTestClasses(lastTestClass, testClazz);
            lastTestClass = testClazz;
        }
    }

    @Override
    public void onFinish(ITestContext context) {
        if (lastTestClass != null) {
            onBetweenTestClasses(lastTestClass, null);
            lastTestClass = null;
        }
    }

    /**
     * Call back hook for adding logic when test execution moves from test class to another.
     *
     * @param endedTestClass the test class which has finished execution. null if the started test class is the first
     * @param startedTestClass the test class which has started execution. null if the ended test class is the last
     */
    protected abstract void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass);
}
