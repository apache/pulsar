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
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestClass;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestNGMethod;

/**
 * TestNG listener adapter that detects when execution finishes for a test class,
 * assuming that a single test class is run in each context.
 * This is the case when running tests with maven-surefire-plugin.
 */
abstract class BetweenTestClassesListenerAdapter implements ITestListener {
    private static final Logger log = LoggerFactory.getLogger(BetweenTestClassesListenerAdapter.class);

    @Override
    public final void onFinish(ITestContext context) {
        List<ITestClass> testClasses =
                Arrays.stream(context.getAllTestMethods()).map(ITestNGMethod::getTestClass).distinct()
                        .collect(Collectors.toList());
        onBetweenTestClasses(testClasses);
    }

    /**
     * Call back hook for adding logic when test execution has completely finished for one or many test classes.
     */
    protected abstract void onBetweenTestClasses(List<ITestClass> testClasses);
}
