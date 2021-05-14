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

        addToOtherGroupIfNoGroupsSpecified(annotation);
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
