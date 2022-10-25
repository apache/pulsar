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

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleanup Mockito's Thread Local state that leaks memory
 * Mockito.reset method should be called at the end of a test in the same thread where the methods were
 * mocked/stubbed. There are some tests which mock methods in the ForkJoinPool thread and these leak memory.
 * This listener doesn't support parallel execution at TestNG level. This is not thread safe.
 * Separate forks {@code testForkCount > 1} controlled with Maven Surefire is the recommended solution
 * for parallel test execution and that is fine.
 */
public class MockitoCleanupListener extends BetweenTestClassesListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(MockitoCleanupListener.class);
    private static final boolean MOCKITO_CLEANUP_ENABLED = Boolean.parseBoolean(
            System.getProperty("testMockitoCleanup", "true"));

    private static final String MOCKITO_CLEANUP_INFO =
            "Cleaning up Mockito's ThreadSafeMockingProgress.MOCKING_PROGRESS_PROVIDER thread local state.";

    @Override
    protected void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass) {
        if (MOCKITO_CLEANUP_ENABLED) {
            try {
                if (MockitoThreadLocalStateCleaner.INSTANCE.isEnabled()) {
                    LOG.info(MOCKITO_CLEANUP_INFO);
                    MockitoThreadLocalStateCleaner.INSTANCE.cleanup();
                }
            } finally {
                cleanupMockitoInline();
            }
        }
    }

    /**
     * Mockito-inline can leak mocked objects, we need to clean up the inline mocks after every test.
     * See <a href="https://javadoc.io/doc/org.mockito/mockito-core/latest/org/mockito/Mockito.html#47"}>
     * mockito docs</a>.
     */
    private void cleanupMockitoInline() {
        Mockito.framework().clearInlineMocks();
    }
}
