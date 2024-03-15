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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.testng.IExecutionListener;

/**
 * A TestNG listener that traces test resource cleanup by creating a thread dump, heap histogram and heap dump
 * (when mode is 'full') before the TestNG JVM exits.
 * The heap dump could help detecting memory leaks in tests or the sources of resource leaks that cannot be
 * detected with the ThreadLeakDetectorListener.
 */
public class TraceTestResourceCleanupListener implements IExecutionListener {
    enum TraceTestResourceCleanupMode {
        OFF,
        ON,
        FULL // includes heap dump
    }

    private static final TraceTestResourceCleanupMode MODE =
            TraceTestResourceCleanupMode.valueOf(
                    System.getenv().getOrDefault("TRACE_TEST_RESOURCE_CLEANUP", "off").toUpperCase());
    private static final File DUMP_DIR = new File(
            System.getenv().getOrDefault("TRACE_TEST_RESOURCE_CLEANUP_DIR", "target/trace-test-resource-cleanup"));
    private static final long WAIT_BEFORE_DUMP_MILLIS =
            Long.parseLong(System.getenv().getOrDefault("TRACE_TEST_RESOURCE_CLEANUP_DELAY", "5000"));

    static {
        if (MODE != TraceTestResourceCleanupMode.OFF) {
            Runtime.getRuntime().addShutdownHook(new Thread(TraceTestResourceCleanupListener::createDumps));
        }
    }

    static void createDumps() {
        if (!DUMP_DIR.isDirectory()) {
            DUMP_DIR.mkdirs();
        }
        try {
            Thread.sleep(WAIT_BEFORE_DUMP_MILLIS);
        } catch (InterruptedException e) {
            // ignore
        }

        String datetimePart =
                DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS").format(ZonedDateTime.now());
        try {
            String threadDump = ThreadDumpUtil.buildThreadDiagnosticString();
            File threaddumpFile = new File(DUMP_DIR, "threaddump" + datetimePart + ".txt");
            Files.asCharSink(threaddumpFile, Charsets.UTF_8).write(threadDump);
        } catch (Throwable t) {
            System.err.println("Error dumping threads");
            t.printStackTrace(System.err);
        }

        try {
            String heapHistogram = HeapHistogramUtil.buildHeapHistogram();
            File heapHistogramFile = new File(DUMP_DIR, "heaphistogram" + datetimePart + ".txt");
            Files.asCharSink(heapHistogramFile, Charsets.UTF_8).write(heapHistogram);
        } catch (Throwable t) {
            System.err.println("Error dumping heap histogram");
            t.printStackTrace(System.err);
        }

        if (MODE == TraceTestResourceCleanupMode.FULL) {
            try {
                File heapdumpFile = new File(DUMP_DIR, "heapdump" + datetimePart + ".hprof");
                HeapDumpUtil.dumpHeap(heapdumpFile, true);
            } catch (Throwable t) {
                System.err.println("Error dumping heap");
                t.printStackTrace(System.err);
            }
        }
    }
}
