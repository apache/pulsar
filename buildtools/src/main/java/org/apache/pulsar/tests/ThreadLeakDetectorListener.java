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

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects new threads that have been created during the test execution.
 */
public class ThreadLeakDetectorListener extends BetweenTestClassesListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadLeakDetectorListener.class);
    private static final boolean
            THREAD_LEAK_DETECTOR_ENABLED = Boolean.valueOf(System.getProperty("testThreadLeakDetector",
            "true"));
    private Set<ThreadKey> capturedThreadKeys;

    @Override
    protected void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass) {
        LOG.info("Capturing identifiers of running threads.");
        capturedThreadKeys = compareThreads(capturedThreadKeys, endedTestClass);
    }

    private static Set<ThreadKey> compareThreads(Set<ThreadKey> previousThreadKeys, Class<?> endedTestClass) {
        Set<ThreadKey> threadKeys = Collections.unmodifiableSet(ThreadUtils.getAllThreads().stream()
                .map(ThreadKey::of)
                .collect(Collectors.<ThreadKey, Set<ThreadKey>>toCollection(LinkedHashSet::new)));

        if (endedTestClass != null && previousThreadKeys != null) {
            int newThreadsCounter = 0;
            LOG.info("Checking for new threads created by {}.", endedTestClass.getName());
            for (ThreadKey threadKey : threadKeys) {
                if (!previousThreadKeys.contains(threadKey)) {
                    newThreadsCounter++;
                    LOG.warn("Tests in class {} created thread id {} with name '{}'", endedTestClass.getSimpleName(),
                            threadKey.getThreadId(), threadKey.getThreadName());
                }
            }
            if (newThreadsCounter > 0) {
                LOG.warn("Summary: Tests in class {} created {} new threads", endedTestClass.getName(),
                        newThreadsCounter);
            }
        }

        return threadKeys;
    }

    /**
     * Unique key for a thread
     * Based on thread id and it's identity hash code
     *
     * Both thread id and identity hash code have chances of getting reused,
     * so this solution helps mitigate that issue.
     */
    private static class ThreadKey {
        private final long threadId;
        private final int threadIdentityHashCode;
        private final String threadName;

        private ThreadKey(long threadId, int threadIdentityHashCode, String threadName) {
            this.threadId = threadId;
            this.threadIdentityHashCode = threadIdentityHashCode;
            this.threadName = threadName;
        }

        static ThreadKey of(Thread thread) {
            return new ThreadKey(thread.getId(), System.identityHashCode(thread), thread.toString());
        }

        public long getThreadId() {
            return threadId;
        }

        public String getThreadName() {
            return threadName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ThreadKey threadKey = (ThreadKey) o;
            return threadId == threadKey.threadId && threadIdentityHashCode == threadKey.threadIdentityHashCode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(threadId, threadIdentityHashCode);
        }
    }
}
