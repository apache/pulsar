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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.ResourceLeakDetector;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.logging.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A custom Netty leak detector used in Pulsar tests that dumps the detected leaks to a file in a directory that is
 * configured with the NETTY_LEAK_DUMP_DIR environment variable. This directory defaults to java.io.tmpdir.
 * The files will be named netty_leak_YYYYMMDD-HHMMSS.SSS.txt.
 */
public class ExtendedNettyLeakDetector<T> extends ResourceLeakDetector<T> {
    private static final Logger LOG = LoggerFactory.getLogger(ExtendedNettyLeakDetector.class);
    public static final String NETTY_CUSTOM_LEAK_DETECTOR_SYSTEM_PROPERTY_NAME = "io.netty.customResourceLeakDetector";

    private static final File DUMP_DIR =
            new File(System.getenv().getOrDefault("NETTY_LEAK_DUMP_DIR", System.getProperty("java.io.tmpdir")));
    public static final String EXIT_JVM_ON_LEAK_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".exitJvmOnLeak";
    public static final String SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".sleepAfterGCAndFinalizationMillis";
    private static final long SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS = Long.parseLong(System.getProperty(
            SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS_SYSTEM_PROPERTY_NAME, "10"));
    private static boolean exitJvmOnLeak = Boolean.valueOf(
            System.getProperty(EXIT_JVM_ON_LEAK_SYSTEM_PROPERTY_NAME, "false"));
    private static final boolean DEFAULT_EXIT_JVM_ON_LEAK = exitJvmOnLeak;
    public static final String EXIT_JVM_DELAY_MILLIS_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".exitJvmDelayMillis";
    private static final long EXIT_JVM_DELAY_MILLIS =
            Long.parseLong(System.getProperty(EXIT_JVM_DELAY_MILLIS_SYSTEM_PROPERTY_NAME, "1000"));
    public static final String USE_SHUTDOWN_HOOK_SYSTEM_PROPERTY_NAME =
            ExtendedNettyLeakDetector.class.getName() + ".useShutdownHook";
    private static boolean useShutdownHook = Boolean.valueOf(
            System.getProperty(USE_SHUTDOWN_HOOK_SYSTEM_PROPERTY_NAME, "false"));
    static {
        maybeRegisterShutdownHook();
    }

    private boolean exitThreadStarted;
    private static volatile String initialHint;

    /**
     * Triggers Netty leak detection.
     * Passing "-XX:+UnlockExperimentalVMOptions -XX:ReferencesPerThread=0" to the JVM will help to detect leaks
     * with a shorter latency. When ReferencesPerThread is set to 0, the JVM will use maximum parallelism for processing
     * reference objects. Netty's leak detection relies on WeakReferences and this setting will help to process them
     * faster.
     */
    public static void triggerLeakDetection() {
        if (!isEnabled()) {
            return;
        }
        // run System.gc() to trigger finalization of objects and detection of possible Netty leaks
        System.gc();
        System.runFinalization();
        try {
            // wait for WeakReference collection to complete
            Thread.sleep(SLEEP_AFTER_GC_AND_FINALIZATION_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        triggerLeakReporting();
    }

    private static void triggerLeakReporting() {
        // create a ByteBuf and release it to trigger leak detection
        // this calls ResourceLeakDetector's reportLeak method when paranoid is enabled.
        ByteBuf buffer = ByteBufAllocator.DEFAULT.directBuffer();
        // this triggers leak detection which forces tracking even when paranoid isn't enabled
        // check the source code of ResourceLeakDetector to see how it works or step through it in a debugger
        // This will call leak detector's trackForcibly method which will then call ResourceLeakDetector's reportLeak
        // trackForcibly gets called in io.netty.buffer.SimpleLeakAwareByteBuf.unwrappedDerived
        ByteBuf retainedSlice = buffer.retainedSlice();
        retainedSlice.release();
        buffer.release();
    }

    public ExtendedNettyLeakDetector(Class<?> resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    public ExtendedNettyLeakDetector(Class<?> resourceType, int samplingInterval) {
        super(resourceType, samplingInterval);
    }

    public ExtendedNettyLeakDetector(String resourceType, int samplingInterval, long maxActive) {
        super(resourceType, samplingInterval, maxActive);
    }

    /**
     * Set the initial hint to be used when reporting leaks.
     * This hint will be printed alongside the stack trace of the creation of the resource in the Netty leak report.
     *
     * @see ResourceLeakDetector#getInitialHint(String)
     * @param initialHint the initial hint
     */
    public static void setInitialHint(String initialHint) {
        ExtendedNettyLeakDetector.initialHint = initialHint;
    }

    @Override
    protected boolean needReport() {
        return true;
    }

    @Override
    protected Object getInitialHint(String resourceType) {
        String currentInitialHint = ExtendedNettyLeakDetector.initialHint;
        if (currentInitialHint != null) {
            return currentInitialHint;
        } else {
            return super.getInitialHint(resourceType);
        }
    }

    @Override
    protected void reportTracedLeak(String resourceType, String records) {
        super.reportTracedLeak(resourceType, records);
        dumpToFile(resourceType, records);
        maybeExitJVM();
    }

    @Override
    protected void reportUntracedLeak(String resourceType) {
        super.reportUntracedLeak(resourceType);
        dumpToFile(resourceType, null);
        maybeExitJVM();
    }

    private synchronized void maybeExitJVM() {
        if (exitThreadStarted) {
            return;
        }
        if (exitJvmOnLeak) {
            new Thread(() -> {
                LOG.error("Exiting JVM due to Netty resource leak. Check logs for more details. Dumped to {}",
                        DUMP_DIR.getAbsolutePath());
                System.err.println("Exiting JVM due to Netty resource leak. Check logs for more details. Dumped to "
                        + DUMP_DIR.getAbsolutePath());
                triggerLeakDetectionBeforeJVMExit();
                // shutdown log4j2 logging to prevent log truncation
                LogManager.shutdown();
                // flush log buffers
                System.err.flush();
                System.out.flush();
                // exit JVM immediately
                Runtime.getRuntime().halt(1);
            }).start();
            exitThreadStarted = true;
        }
    }

    private void dumpToFile(String resourceType, String records) {
        try {
            if (!DUMP_DIR.exists()) {
                DUMP_DIR.mkdirs();
            }
            String datetimePart =
                    DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss.SSS").format(ZonedDateTime.now());
            File nettyLeakDumpFile =
                    new File(DUMP_DIR, "netty_leak_" + datetimePart + ".txt");
            // Prefix the line to make it easier to show the errors in GitHub Actions as annotations
            String linePrefix = exitJvmOnLeak ? "::error::" : "::warning::";
            try (PrintWriter out = new PrintWriter(new FileWriter(nettyLeakDumpFile, true))) {
                out.print(linePrefix);
                if (records != null) {
                    out.println("Traced leak detected " + resourceType);
                    out.println(records);
                } else {
                    out.println("Untraced leak detected " + resourceType);
                }
                out.println();
                out.flush();
            }
        } catch (IOException e) {
            LOG.error("Cannot write thread leak dump", e);
        }
    }

    public static boolean isExtendedNettyLeakDetectorEnabled() {
        return ExtendedNettyLeakDetector.class.getName()
                .equals(System.getProperty(NETTY_CUSTOM_LEAK_DETECTOR_SYSTEM_PROPERTY_NAME));
    }

    /**
     * Disable exit on leak. This is useful when exitJvmOnLeak is enabled and there's a test that is expected
     * to leak resources. This method can be called before the test execution begins.
     * This will not disable the leak detection itself, only the exit on leak behavior.
     */
    public static void disableExitJVMOnLeak() {
        exitJvmOnLeak = false;
    }

    /**
     * Restore exit on leak to original value. This is used to re-enable exitJvmOnLeak feature after it was
     * disabled for the duration of a specific test using disableExitJVMOnLeak.
     */
    public static void restoreExitJVMOnLeak() {
        triggerLeakDetection();
        exitJvmOnLeak = DEFAULT_EXIT_JVM_ON_LEAK;
    }

    /**
     * Shutdown hook to trigger leak detection on JVM shutdown.
     * This is useful when using the leak detector in actual production code or in system tests which
     * don't use don't have a test listener that would be calling triggerLeakDetection before the JVM exits.
     */
    private static void maybeRegisterShutdownHook() {
        if (!exitJvmOnLeak && useShutdownHook) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (!isEnabled()) {
                    return;
                }
                triggerLeakDetectionBeforeJVMExit();
            }, ExtendedNettyLeakDetector.class.getSimpleName() + "ShutdownHook"));
        }
    }

    private static void triggerLeakDetectionBeforeJVMExit() {
        triggerLeakDetection();
        // wait for a while
        try {
            Thread.sleep(EXIT_JVM_DELAY_MILLIS);
        } catch (InterruptedException e) {
            // ignore
        }
        // trigger leak detection again to increase the chances of detecting leaks
        // this could be helpful if more objects were finalized asynchronously during the delay
        triggerLeakDetection();
    }
}
