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
package org.apache.pulsar.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import javax.management.JMException;
import javax.management.ObjectName;

/**
 * Adapted from Hadoop TimedOutTestsListener.
 *
 * https://raw.githubusercontent.com/apache/hadoop/master/hadoop-common-project/hadoop-common/src/test/java/org/apache/hadoop/test/TimedOutTestsListener.java
 */
public class ThreadDumpUtil {
    private static final String INDENT = "    ";

    public static String buildThreadDiagnosticString() {
        StringWriter sw = new StringWriter();
        PrintWriter output = new PrintWriter(sw);

        output.println(buildThreadDump());

        String deadlocksInfo = buildDeadlockInfo();
        if (deadlocksInfo != null) {
            output.println("====> DEADLOCKS DETECTED <====");
            output.println();
            output.println(deadlocksInfo);
        }

        return sw.toString();
    }

    static String buildThreadDump() {
        try {
            // first attempt to use jcmd to do the thread dump, similar output to jstack
            return callDiagnosticCommand("threadPrint", "-l");
        } catch (Exception ignore) {
        }

        // fallback to using JMX for creating the thread dump
        StringBuilder dump = new StringBuilder();

        dump.append(String.format("Timestamp: %s", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(LocalDateTime.now())));
        dump.append("\n\n");

        Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
        for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
            Thread thread = e.getKey();
            dump.append('\n');
            dump.append(String.format("\"%s\" %s prio=%d tid=%d %s%njava.lang.Thread.State: %s", thread.getName(),
                    (thread.isDaemon() ? "daemon" : ""), thread.getPriority(), thread.getId(),
                    Thread.State.WAITING.equals(thread.getState()) ? "in Object.wait()" : thread.getState().name(),
                    Thread.State.WAITING.equals(thread.getState()) ? "WAITING (on object monitor)"
                            : thread.getState()));
            for (StackTraceElement stackTraceElement : e.getValue()) {
                dump.append("\n        at ");
                dump.append(stackTraceElement);
            }
            dump.append("\n");
        }
        return dump.toString();
    }

    /**
     * Calls a diagnostic commands.
     * The available operations are similar to what the jcmd commandline tool has,
     * however the naming of the operations are different. The "help" operation can be used
     * to find out the available operations. For example, the jcmd command "Thread.print" maps
     * to "threadPrint" operation name.
     */
    static String callDiagnosticCommand(String operationName, String... args)
            throws JMException {
        return (String) ManagementFactory.getPlatformMBeanServer()
                .invoke(new ObjectName("com.sun.management:type=DiagnosticCommand"),
                        operationName, new Object[]{args}, new String[]{String[].class.getName()});
    }

    static String buildDeadlockInfo() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadBean.findDeadlockedThreads();
        if (threadIds != null && threadIds.length > 0) {
            StringWriter stringWriter = new StringWriter();
            PrintWriter out = new PrintWriter(stringWriter);

            ThreadInfo[] infos = threadBean.getThreadInfo(threadIds, true, true);
            for (ThreadInfo ti : infos) {
                printThreadInfo(ti, out);
                printLockInfo(ti.getLockedSynchronizers(), out);
                out.println();
            }

            out.close();
            return stringWriter.toString();
        } else {
            return null;
        }
    }

    private static void printThreadInfo(ThreadInfo ti, PrintWriter out) {
        // print thread information
        printThread(ti, out);

        // print stack trace with locks
        StackTraceElement[] stacktrace = ti.getStackTrace();
        MonitorInfo[] monitors = ti.getLockedMonitors();
        for (int i = 0; i < stacktrace.length; i++) {
            StackTraceElement ste = stacktrace[i];
            out.println(INDENT + "at " + ste.toString());
            for (MonitorInfo mi : monitors) {
                if (mi.getLockedStackDepth() == i) {
                    out.println(INDENT + "  - locked " + mi);
                }
            }
        }
        out.println();
    }

    private static void printThread(ThreadInfo ti, PrintWriter out) {
        out.println();
        out.print("\"" + ti.getThreadName() + "\"" + " Id=" + ti.getThreadId() + " in " + ti.getThreadState());
        if (ti.getLockName() != null) {
            out.print(" on lock=" + ti.getLockName());
        }
        if (ti.isSuspended()) {
            out.print(" (suspended)");
        }
        if (ti.isInNative()) {
            out.print(" (running in native)");
        }
        out.println();
        if (ti.getLockOwnerName() != null) {
            out.println(INDENT + " owned by " + ti.getLockOwnerName() + " Id=" + ti.getLockOwnerId());
        }
    }

    private static void printLockInfo(LockInfo[] locks, PrintWriter out) {
        out.println(INDENT + "Locked synchronizers: count = " + locks.length);
        for (LockInfo li : locks) {
            out.println(INDENT + "  - " + li);
        }
        out.println();
    }

}
