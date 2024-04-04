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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.IConfigurationAnnotation;
import org.testng.annotations.ITestAnnotation;
import org.testng.annotations.ITestOrConfiguration;
import org.testng.internal.annotations.DisabledRetryAnalyzer;

public class AnnotationListener implements IAnnotationTransformer {

    private static final long DEFAULT_TEST_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final String OTHER_GROUP = "other";

    private static final String FLAKY_GROUP = "flaky";

    private static final String QUARANTINE_GROUP = "quarantine";

    public AnnotationListener() {
        System.out.println("Created annotation listener");
    }

    @Override
    public void transform(ITestAnnotation annotation,
                          Class testClass,
                          Constructor testConstructor,
                          Method testMethod) {
        if (annotation.getRetryAnalyzerClass() == null
                || annotation.getRetryAnalyzerClass() == DisabledRetryAnalyzer.class) {
            annotation.setRetryAnalyzer(RetryAnalyzer.class);
        }

        // Enforce default test timeout
        if (annotation.getTimeOut() == 0) {
            annotation.setTimeOut(DEFAULT_TEST_TIMEOUT_MILLIS);
        }

        replaceGroupsIfFlakyOrQuarantineGroupIsIncluded(annotation);
        addToOtherGroupIfNoGroupsSpecified(annotation);
    }

    // A test method will inherit the test groups from the class level and this solution ensures that a test method
    // added to the flaky or quarantine group will not be executed as part of other groups.
    private void replaceGroupsIfFlakyOrQuarantineGroupIsIncluded(ITestAnnotation annotation) {
        if (annotation.getGroups() != null && annotation.getGroups().length > 1) {
            for (String group : annotation.getGroups()) {
                if (group.equals(QUARANTINE_GROUP)) {
                    annotation.setGroups(new String[]{QUARANTINE_GROUP});
                    return;
                }
                if (group.equals(FLAKY_GROUP)) {
                    annotation.setGroups(new String[]{FLAKY_GROUP});
                    return;
                }
            }
        }
    }

    private void addToOtherGroupIfNoGroupsSpecified(ITestOrConfiguration annotation) {
        // Add test to "other" group if there's no specified group
        if (annotation.getGroups() == null || annotation.getGroups().length == 0) {
            annotation.setGroups(new String[]{OTHER_GROUP});
        }
    }

    @Override
    public void transform(IConfigurationAnnotation annotation, Class testClass, Constructor testConstructor,
                          Method testMethod) {
        // configuration methods such as BeforeMethod / BeforeClass methods should also be added to the "other" group
        // since BeforeMethod/BeforeClass methods get run only when the group matches or when there's "alwaysRun=true"
        addToOtherGroupIfNoGroupsSpecified(annotation);
    }
}
