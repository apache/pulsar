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
import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

@Slf4j
public class VipStatusTest {

    public static final String ATTRIBUTE_STATUS_FILE_PATH = "statusFilePath";
    public static final String ATTRIBUTE_IS_READY_PROBE = "isReadyProbe";
    private static final long LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED = 10000L;
    // Rate limit status checks to every 500ms to prevent DoS
    private static final long CHECK_STATUS_INTERVAL = 500L;

    private MockServletContext mockServletContext = new MockServletContext();
    private VipStatus vipStatus;

    @BeforeTest
    public void setup() throws IOException {
        String statusFilePath = "/tmp/status.html";
        File file = new File(statusFilePath);
        file.createNewFile();

        mockServletContext.setAttribute(ATTRIBUTE_STATUS_FILE_PATH, statusFilePath);
        Supplier<Boolean> isReadyProbe = () -> true;
        mockServletContext.setAttribute(ATTRIBUTE_IS_READY_PROBE, isReadyProbe);
        vipStatus = new VipStatus(mockServletContext, LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED);
    }

    @Test
    public void testVipStatusCheckStatus() {
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

    public void testVipStatusCheckStatusWithoutDeadlock() {
        assertEquals(vipStatus.checkStatus(), "OK");
    }

    public void testVipStatusCheckStatusWithDeadlock() {
        MockDeadlock.startDeadlock();
        boolean asExpected = true;
        try {
            vipStatus.checkStatus();
            asExpected = false;
            System.out.println("Simulated deadlock, no deadlock detected, not as expected.");
        } catch (Exception wae) {
            System.out.println("Simulated deadlock and detected it, as expected.");
        }

        if (!asExpected) {
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }
    }

    public class MockDeadlock {
        private static Object lockA = new Object();
        private static Object lockB = new Object();
        private static Thread t1 = new Thread(new ThreadOne());
        private static Thread t2 = new Thread(new ThreadTwo());

        @SneakyThrows
        public static void startDeadlock() {
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