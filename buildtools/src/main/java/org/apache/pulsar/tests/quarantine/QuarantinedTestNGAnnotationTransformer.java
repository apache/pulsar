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
package org.apache.pulsar.tests.quarantine;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IAnnotationTransformer;
import org.testng.annotations.ITestAnnotation;

/**
 * <p>TestNG {@link IAnnotationTransformer} implementation for integrating {@link Quarantined} annotation
 * to TestNG.
 *
 * <p>Flaky tests can be <em>quarantined</em> by adding a {@link Quarantined} annotation. This
 * means that the test won't get executed by default.
 *
 * <p>Quarantined tests can be run by setting {@code runQuarantinedTests} system property to true.
 * Another way to run Quarantined tests is to set {@code runOnlyQuarantinedTests} system property
 * to true. This will result in disabling all other tests than quarantined tests.
 *
 */
public class QuarantinedTestNGAnnotationTransformer implements IAnnotationTransformer {
    private static final Logger LOG = LoggerFactory.getLogger(QuarantinedTestNGAnnotationTransformer.class);

    private static final boolean DEFAULT_RUN_QUARANTINED_TESTS =
            Boolean.parseBoolean(System.getProperty("runQuarantinedTests", "false"));
    private static final boolean DEFAULT_RUN_ONLY_QUARANTINED_TESTS =
            Boolean.parseBoolean(System.getProperty("runOnlyQuarantinedTests", "false"));

    private final boolean runQuarantinedTests;
    private final boolean runOnlyQuarantinedTests;

    public QuarantinedTestNGAnnotationTransformer() {
        this(DEFAULT_RUN_QUARANTINED_TESTS, DEFAULT_RUN_ONLY_QUARANTINED_TESTS);
    }

    // used for unit testing this class
    QuarantinedTestNGAnnotationTransformer(boolean runQuarantinedTests, boolean runOnlyQuarantinedTests) {
        this.runQuarantinedTests = runQuarantinedTests;
        this.runOnlyQuarantinedTests = runOnlyQuarantinedTests;
    }

    @Override
    public void transform(ITestAnnotation annotation, Class testClass, Constructor testConstructor, Method testMethod) {
        if (!runQuarantinedTests || runOnlyQuarantinedTests) {
            Quarantined quarantined = (Quarantined) testClass.getAnnotation(Quarantined.class);
            if (quarantined != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Test class {} is annotated with {} and reason '{}'",
                            testClass.getName(), quarantined.annotationType().getName(), quarantined.value());
                }
            } else if (testMethod != null) {
                quarantined = testMethod.getAnnotation(Quarantined.class);
                if (quarantined != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Test method {} is annotated with {} and reason '{}'",
                                testMethod.getName(), quarantined.annotationType().getName(), quarantined.value());
                    }
                }
            } else if (testConstructor != null) {
                quarantined = (Quarantined) testConstructor.getAnnotation(Quarantined.class);
                if (quarantined != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Test constructor {} is annotated with {} and reason '{}'",
                                testConstructor.getName(), quarantined.annotationType().getName(), quarantined.value());
                    }
                }
            }
            if (runOnlyQuarantinedTests) {
                if (quarantined == null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Disabling test method {} since only Quarantined methods are to be executed.",
                                testMethod.getName());
                    }
                    annotation.setEnabled(false);
                }
            } else if (!runQuarantinedTests) {
                if (quarantined != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Disabling test method {} since it is Quarantined with reason '{}'.",
                                testMethod.getName(), quarantined.value());
                    }
                    annotation.setEnabled(false);
                }
            }
        }
    }
}
