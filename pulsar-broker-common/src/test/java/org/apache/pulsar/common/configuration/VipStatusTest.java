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
package org.apache.pulsar.common.configuration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.EventListener;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.servlet.Filter;
import javax.servlet.FilterRegistration;
import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.descriptor.JspConfigDescriptor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.ThreadDumpUtil;
import org.eclipse.jetty.util.AttributesMap;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class VipStatusTest {

    public static final String ATTRIBUTE_STATUS_FILE_PATH = "statusFilePath";
    public static final String ATTRIBUTE_IS_READY_PROBE = "isReadyProbe";

    // log a full thread dump when a deadlock is detected in status check once every 10 minutes
    // to prevent excessive logging
    private static final long LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED = 10000L;
    private static volatile long lastCheckStatusTimestamp;
    private static volatile long lastPrintThreadDumpTimestamp;

    // Rate limit status checks to every 500ms to prevent DoS
    private static final long CHECK_STATUS_INTERVAL = 500L;
    private static volatile boolean lastCheckStatusResult;
    private static String CHECK_RESULT_OK = "OK";
    private static String CHECK_RESULT_NOT_OK = "NOT_OK";
    private int checkThrottlingCount;
    private int checkNoThrottlingCount;
    private int printDeadlockThreadDumpCount;

    private MockServletContext mockServletContext = new MockServletContext();
    @BeforeTest
    public void setup() throws IOException {
        String statusFilePath = "/tmp/status.html";
        File file = new File(statusFilePath);
        file.createNewFile();
        mockServletContext.setAttribute(ATTRIBUTE_STATUS_FILE_PATH, statusFilePath);
        Supplier<Boolean> isReadyProbe = () -> true;
        mockServletContext.setAttribute(ATTRIBUTE_IS_READY_PROBE, isReadyProbe);
    }

    @Test
    public void testVipStatusCheckStatus() throws InterruptedException {
        // No deadlocks
        testVipStatusCheckStatusWithoutDeadlock();
        // There is a deadlock
        testVipStatusCheckStatusWithDeadlock();
    }

    @AfterTest
    public void release() throws IOException {
        String statusFilePath = "/tmp/status.html";
        File file = new File(statusFilePath);
        file.deleteOnExit();
    }

    public void testVipStatusCheckStatusWithoutDeadlock() throws InterruptedException {
        //1.No DOS attacks
        for (int i = 0; i < 10; i++) {
            assertEquals(checkStatus(), CHECK_RESULT_OK);
            Thread.sleep(CHECK_STATUS_INTERVAL + 10);
        }
        assertTrue(checkNoThrottlingCount == 10);
        assertTrue(checkThrottlingCount == 0);
        checkNoThrottlingCount = 0;
        checkThrottlingCount = 0;

        //2.There are DOS attacks
        for (int i = 0; i < 10; i++) {
            assertEquals(checkStatus(), CHECK_RESULT_OK);
        }
        assertTrue(checkNoThrottlingCount >= 1);
        assertTrue(checkThrottlingCount >= 1);
        checkNoThrottlingCount = 0;
        checkThrottlingCount = 0;
    }

    public void testVipStatusCheckStatusWithDeadlock() throws InterruptedException {
        MockDeadlock.startDeadlock();
        //1.No DOS attacks
        for (int i = 0; i < 10; i++) {
            assertEquals(checkStatus(), CHECK_RESULT_NOT_OK);
            Thread.sleep(CHECK_STATUS_INTERVAL + 10);
        }
        assertTrue(checkNoThrottlingCount == 10);
        assertTrue(checkThrottlingCount == 0);
        checkNoThrottlingCount = 0;
        checkThrottlingCount = 0;

        //2.There are DOS attacks
        for (int i = 0; i < 10; i++) {
            assertEquals(checkStatus(), CHECK_RESULT_NOT_OK);
        }
        assertTrue(checkNoThrottlingCount >= 1);
        assertTrue(checkThrottlingCount >= 1);
        checkNoThrottlingCount = 0;
        checkThrottlingCount = 0;

        //3.print deadlock info
        for (int i = 0; i < 10; i++) {
            assertEquals(checkStatus(), CHECK_RESULT_NOT_OK);
            Thread.sleep(LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED / 2);
        }
        assertTrue(printDeadlockThreadDumpCount >= 5);
    }

    public String checkStatus() {
        // Locking classes to avoid deadlock detection in multi-thread concurrent requests.
        synchronized (VipStatus.class) {
            if (System.currentTimeMillis() - lastCheckStatusTimestamp < CHECK_STATUS_INTERVAL) {
                checkThrottlingCount ++;
                if (lastCheckStatusResult) {
                    return CHECK_RESULT_OK;
                } else {
                    return CHECK_RESULT_NOT_OK;
                }
            }
            lastCheckStatusTimestamp = System.currentTimeMillis();

            String statusFilePath = (String) mockServletContext.getAttribute(ATTRIBUTE_STATUS_FILE_PATH);
            @SuppressWarnings("unchecked")
            Supplier<Boolean> isReadyProbe = (Supplier<Boolean>) mockServletContext.getAttribute(ATTRIBUTE_IS_READY_PROBE);

            boolean isReady = isReadyProbe != null ? isReadyProbe.get() : true;

            if (statusFilePath != null) {
                File statusFile = new File(statusFilePath);
                if (isReady && statusFile.exists() && statusFile.isFile()) {
                    // check deadlock
                    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
                    long[] threadIds = threadBean.findDeadlockedThreads();
                    if (threadIds != null && threadIds.length > 0) {
                        ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, false,
                                false);
                        String threadNames = Arrays.stream(threadInfos)
                                .map(threadInfo -> threadInfo.getThreadName()
                                        + "(tid=" + threadInfo.getThreadId() + ")")
                                .collect(Collectors.joining(", "));
                        if (System.currentTimeMillis() - lastPrintThreadDumpTimestamp
                                > LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED) {
                            String diagnosticResult = ThreadDumpUtil.buildThreadDiagnosticString();
                            log.error("Deadlock detected, service may be unavailable, "
                                    + "thread stack details are as follows: {}.", diagnosticResult);
                            lastPrintThreadDumpTimestamp = System.currentTimeMillis();
                            printDeadlockThreadDumpCount ++;
                        } else {
                            log.error("Deadlocked threads detected. {}", threadNames);
                        }
                        lastCheckStatusResult = false;
                        checkNoThrottlingCount ++;
                        return CHECK_RESULT_NOT_OK;
                    } else {
                        checkNoThrottlingCount ++;
                        lastCheckStatusResult = true;
                        return CHECK_RESULT_OK;
                    }
                }
            }
            checkNoThrottlingCount ++;
            lastCheckStatusResult = false;
            log.warn("Failed to access \"status.html\". The service is not ready");
            return CHECK_RESULT_OK;
        }
    }

    public class MockServletContext extends AttributesMap implements ServletContext {

        @Override
        public String getContextPath() {
            return null;
        }

        @Override
        public ServletContext getContext(String s) {
            return null;
        }

        @Override
        public int getMajorVersion() {
            return 0;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public int getEffectiveMajorVersion() {
            return 0;
        }

        @Override
        public int getEffectiveMinorVersion() {
            return 0;
        }

        @Override
        public String getMimeType(String s) {
            return null;
        }

        @Override
        public Set<String> getResourcePaths(String s) {
            return null;
        }

        @Override
        public URL getResource(String s) throws MalformedURLException {
            return null;
        }

        @Override
        public InputStream getResourceAsStream(String s) {
            return null;
        }

        @Override
        public RequestDispatcher getRequestDispatcher(String s) {
            return null;
        }

        @Override
        public RequestDispatcher getNamedDispatcher(String s) {
            return null;
        }

        @Override
        public Servlet getServlet(String s) throws ServletException {
            return null;
        }

        @Override
        public Enumeration<Servlet> getServlets() {
            return null;
        }

        @Override
        public Enumeration<String> getServletNames() {
            return null;
        }

        @Override
        public void log(String s) {

        }

        @Override
        public void log(Exception e, String s) {

        }

        @Override
        public void log(String s, Throwable throwable) {

        }

        @Override
        public String getRealPath(String s) {
            return null;
        }

        @Override
        public String getServerInfo() {
            return null;
        }

        @Override
        public String getInitParameter(String s) {
            return null;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return null;
        }

        @Override
        public boolean setInitParameter(String s, String s1) {
            return false;
        }

        @Override
        public String getServletContextName() {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String s, String s1) {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String s, Servlet servlet) {
            return null;
        }

        @Override
        public ServletRegistration.Dynamic addServlet(String s, Class<? extends Servlet> aClass) {
            return null;
        }

        @Override
        public <T extends Servlet> T createServlet(Class<T> aClass) throws ServletException {
            return null;
        }

        @Override
        public ServletRegistration getServletRegistration(String s) {
            return null;
        }

        @Override
        public Map<String, ? extends ServletRegistration> getServletRegistrations() {
            return null;
        }

        @Override
        public FilterRegistration.Dynamic addFilter(String s, String s1) {
            return null;
        }

        @Override
        public FilterRegistration.Dynamic addFilter(String s, Filter filter) {
            return null;
        }

        @Override
        public FilterRegistration.Dynamic addFilter(String s, Class<? extends Filter> aClass) {
            return null;
        }

        @Override
        public <T extends Filter> T createFilter(Class<T> aClass) throws ServletException {
            return null;
        }

        @Override
        public FilterRegistration getFilterRegistration(String s) {
            return null;
        }

        @Override
        public Map<String, ? extends FilterRegistration> getFilterRegistrations() {
            return null;
        }

        @Override
        public SessionCookieConfig getSessionCookieConfig() {
            return null;
        }

        @Override
        public void setSessionTrackingModes(Set<SessionTrackingMode> set) {

        }

        @Override
        public Set<SessionTrackingMode> getDefaultSessionTrackingModes() {
            return null;
        }

        @Override
        public Set<SessionTrackingMode> getEffectiveSessionTrackingModes() {
            return null;
        }

        @Override
        public void addListener(String s) {

        }

        @Override
        public <T extends EventListener> void addListener(T t) {

        }

        @Override
        public void addListener(Class<? extends EventListener> aClass) {

        }

        @Override
        public <T extends EventListener> T createListener(Class<T> aClass) throws ServletException {
            return null;
        }

        @Override
        public JspConfigDescriptor getJspConfigDescriptor() {
            return null;
        }

        @Override
        public ClassLoader getClassLoader() {
            return null;
        }

        @Override
        public void declareRoles(String... strings) {

        }

        @Override
        public String getVirtualServerName() {
            return null;
        }
    }

    public class MockDeadlock {
        private static Object lockA = new Object();
        private static Object lockB = new Object();
        private static Thread t1 = new Thread(new ThreadOne());
        private static Thread t2 = new Thread(new ThreadTwo());

        @SneakyThrows
        public static void startDeadlock() {
            // 启动两个线程来模拟死锁
            t1.start();
            t2.start();
            Thread.sleep(CHECK_STATUS_INTERVAL);
        }

        private static class ThreadOne implements Runnable {
            @Override
            public void run() {
                synchronized (lockA) {
                    System.out.println("ThreadOne acquired lockA");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (lockB) {
                        System.out.println("ThreadOne acquired lockB");
                    }
                }
            }
        }

        private static class ThreadTwo implements Runnable {
            @Override
            public void run() {
                synchronized (lockB) {
                    System.out.println("ThreadTwo acquired lockB");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    synchronized (lockA) {
                        System.out.println("ThreadTwo acquired lockA");
                    }
                }
            }
        }
    }
}
