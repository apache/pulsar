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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects new threads that have been created during the test execution. This is useful to detect thread leaks.
 * Will create files to the configured directory if new threads are detected and THREAD_LEAK_DETECTOR_WAIT_MILLIS
 * is set to a positive value. A recommended value is 10000 for THREAD_LEAK_DETECTOR_WAIT_MILLIS. This will ensure
 * that any asynchronous operations should have completed before the detector determines that it has found a leak.
 */
public class ThreadLeakDetectorListener extends BetweenTestClassesListenerAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ThreadLeakDetectorListener.class);
    private static final long WAIT_FOR_THREAD_TERMINATION_MILLIS =
            Long.parseLong(System.getenv().getOrDefault("THREAD_LEAK_DETECTOR_WAIT_MILLIS", "0"));
    private static final File DUMP_DIR =
            new File(System.getenv().getOrDefault("THREAD_LEAK_DETECTOR_DIR", "target/thread-leak-dumps"));
    private static final long THREAD_TERMINATION_POLL_INTERVAL =
            Long.parseLong(System.getenv().getOrDefault("THREAD_LEAK_DETECTOR_POLL_INTERVAL", "250"));
    private static final boolean COLLECT_THREADDUMP =
            Boolean.parseBoolean(System.getenv().getOrDefault("THREAD_LEAK_DETECTOR_COLLECT_THREADDUMP", "true"));

    private Set<ThreadKey> capturedThreadKeys;

    private static final Field THREAD_TARGET_FIELD;
    static {
        Field targetField = null;
        try {
            targetField = Thread.class.getDeclaredField("target");
            targetField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            // ignore this error. on Java 21, the field is not present
            // TODO: add support for extracting the Runnable target on Java 21
        }
        THREAD_TARGET_FIELD = targetField;
    }

    @Override
    protected void onBetweenTestClasses(Class<?> endedTestClass, Class<?> startedTestClass) {
        LOG.info("Capturing identifiers of running threads.");
        MutableBoolean differenceDetected = new MutableBoolean();
        Set<ThreadKey> currentThreadKeys =
                compareThreads(capturedThreadKeys, endedTestClass, WAIT_FOR_THREAD_TERMINATION_MILLIS <= 0,
                        differenceDetected, null);
        if (WAIT_FOR_THREAD_TERMINATION_MILLIS > 0 && endedTestClass != null && differenceDetected.booleanValue()) {
            LOG.info("Difference detected in active threads. Waiting up to {} ms for threads to terminate.",
                    WAIT_FOR_THREAD_TERMINATION_MILLIS);
            long endTime = System.currentTimeMillis() + WAIT_FOR_THREAD_TERMINATION_MILLIS;
            while (System.currentTimeMillis() < endTime) {
                try {
                    Thread.sleep(THREAD_TERMINATION_POLL_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                differenceDetected.setFalse();
                currentThreadKeys = compareThreads(capturedThreadKeys, endedTestClass, false, differenceDetected, null);
                if (!differenceDetected.booleanValue()) {
                    break;
                }
            }
            if (differenceDetected.booleanValue()) {
                String datetimePart =
                        DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS").format(ZonedDateTime.now());
                PrintWriter out = null;
                try {
                    if (!DUMP_DIR.exists()) {
                        DUMP_DIR.mkdirs();
                    }
                    File threadleakdumpFile =
                            new File(DUMP_DIR, "threadleak" + datetimePart + endedTestClass.getName() + ".txt");
                    out = new PrintWriter(threadleakdumpFile);
                } catch (IOException e) {
                    LOG.error("Cannot write thread leak dump", e);
                }
                currentThreadKeys = compareThreads(capturedThreadKeys, endedTestClass, true, null, out);
                if (out != null) {
                    out.close();
                }
                if (COLLECT_THREADDUMP) {
                    File threaddumpFile =
                            new File(DUMP_DIR, "threaddump" + datetimePart + endedTestClass.getName() + ".txt");
                    try {
                        Files.asCharSink(threaddumpFile, Charsets.UTF_8)
                                .write(ThreadDumpUtil.buildThreadDiagnosticString());
                    } catch (IOException e) {
                        LOG.error("Cannot write thread dump", e);
                    }
                }
            }
        }
        capturedThreadKeys = currentThreadKeys;
    }

    private static Set<ThreadKey> compareThreads(Set<ThreadKey> previousThreadKeys, Class<?> endedTestClass,
                                                 boolean logDifference, MutableBoolean differenceDetected,
                                                 PrintWriter out) {
        Set<ThreadKey> threadKeys = Collections.unmodifiableSet(ThreadUtils.getAllThreads().stream()
                .filter(thread -> !shouldSkipThread(thread))
                .map(ThreadKey::of)
                .collect(Collectors.<ThreadKey, Set<ThreadKey>>toCollection(LinkedHashSet::new)));

        if (endedTestClass != null && previousThreadKeys != null) {
            int newThreadsCounter = 0;
            for (ThreadKey threadKey : threadKeys) {
                if (!previousThreadKeys.contains(threadKey)) {
                    newThreadsCounter++;
                    if (differenceDetected != null) {
                        differenceDetected.setTrue();
                    }
                    if (logDifference || out != null) {
                        String message = String.format("Tests in class %s created thread id %d with name '%s'",
                                endedTestClass.getSimpleName(),
                                threadKey.getThreadId(), threadKey.getThreadName());
                        if (logDifference) {
                            LOG.warn(message);
                        }
                        if (out != null) {
                            out.println(message);
                        }
                    }
                }
            }
            if (newThreadsCounter > 0 && (logDifference || out != null)) {
                String message = String.format(
                        "Summary: Tests in class %s created %d new threads. There are now %d threads in total.",
                        endedTestClass.getName(), newThreadsCounter, threadKeys.size());
                if (logDifference) {
                    LOG.warn(message);
                }
                if (out != null) {
                    out.println(message);
                }
            }
        }

        return threadKeys;
    }

    private static boolean shouldSkipThread(Thread thread) {
        // skip ForkJoinPool threads
        if (thread instanceof ForkJoinWorkerThread) {
            return true;
        }
        // skip Testcontainers threads
        final ThreadGroup threadGroup = thread.getThreadGroup();
        if (threadGroup != null && "testcontainers".equals(threadGroup.getName())) {
            return true;
        }
        String threadName = thread.getName();
        if (threadName != null) {
            // skip ClientTestFixtures.SCHEDULER threads
            if (threadName.startsWith("ClientTestFixtures-SCHEDULER-")) {
                return true;
            }
            // skip JVM internal threads related to java.lang.Process
            if (threadName.equals("process reaper")) {
                return true;
            }
            // skip JVM internal thread used for CompletableFuture.delayedExecutor
            if (threadName.equals("CompletableFutureDelayScheduler")) {
                return true;
            }
            // skip threadpool created in dev.failsafe.internal.util.DelegatingScheduler
            if (threadName.equals("FailsafeDelayScheduler")) {
                return true;
            }
            // skip Okio Watchdog thread and interrupt it
            if (threadName.equals("Okio Watchdog")) {
                return true;
            }
            // skip OkHttp TaskRunner thread
            if (threadName.equals("OkHttp TaskRunner")) {
                return true;
            }
            // skip JNA background thread
            if (threadName.equals("JNA Cleaner")) {
                return true;
            }
            // skip org.glassfish.grizzly.http.server.DefaultSessionManager thread pool
            if (threadName.equals("Grizzly-HttpSession-Expirer")) {
                return true;
            }
            // Testcontainers AbstractWaitStrategy.EXECUTOR
            if (threadName.startsWith("testcontainers-wait-")) {
                return true;
            }
            // org.rnorth.ducttape.timeouts.Timeouts.EXECUTOR_SERVICE thread pool, used by Testcontainers
            if (threadName.startsWith("ducttape-")) {
                return true;
            }
        }
        Runnable target = extractRunnableTarget(thread);
        if (target != null) {
            String targetClassName = target.getClass().getName();
            // ignore threads that contain a Runnable class under org.testcontainers package
            if (targetClassName.startsWith("org.testcontainers.")) {
                return true;
            }
        }
        return false;
    }

    // use reflection to extract the Runnable target from a thread so that we can detect threads created by
    // Testcontainers based on the Runnable's class name.
    private static Runnable extractRunnableTarget(Thread thread) {
        if (THREAD_TARGET_FIELD == null) {
            return null;
        }
        Runnable target = null;
        try {
            target = (Runnable) THREAD_TARGET_FIELD.get(thread);
        } catch (IllegalAccessException e) {
            LOG.warn("Cannot access target field in Thread.class", e);
        }
        return target;
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
