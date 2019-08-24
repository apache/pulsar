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

import org.apache.commons.lang3.math.NumberUtils;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

@SuppressWarnings("rawtypes")
public class AnnotationListener implements IAnnotationTransformer {

    private static final long DEFAULT_TEST_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5);

    private static final long TEST_TIMEOUT_MILLIS_PROPERTY =
            Integer.parseInt(System.getProperty("testTimeOut", Long.toString(DEFAULT_TEST_TIMEOUT_MILLIS)));

     private static final long TEST_TIMEOUT_MILLIS_ENV =
            NumberUtils.toLong(System.getenv("TEST_TIMEOUT_ENV_MILLIS"), DEFAULT_TEST_TIMEOUT_MILLIS);

     private static final long TEST_TIMEOUT_MILLIS = Math.max(TEST_TIMEOUT_MILLIS_PROPERTY, TEST_TIMEOUT_MILLIS_ENV);

    public AnnotationListener() {
        System.out.println("Created annotation listener");
    }

    @Override
    public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
        annotation.setRetryAnalyzer(RetryAnalyzer.class);

        // Enforce default test timeout
        if (annotation.getTimeOut() == 0) {
            annotation.setTimeOut(TEST_TIMEOUT_MILLIS);
        }
    }
}
