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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mock;
import org.mockito.Mockito;
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

    @Mock
    private ServletContext mockServletContext;
    private VipStatus vipStatus;

    @BeforeTest
    public void setup() throws IOException {
        String statusFilePath = "/tmp/status.html";
        File file = new File(statusFilePath);
        file.createNewFile();
        Supplier<Boolean> isReadyProbe = () -> true;

        mockServletContext = Mockito.mock(ServletContext.class);
        Mockito.when(mockServletContext.getAttribute(ATTRIBUTE_STATUS_FILE_PATH)).thenReturn(statusFilePath);
        Mockito.when(mockServletContext.getAttribute(ATTRIBUTE_IS_READY_PROBE)).thenReturn(isReadyProbe);

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
        } finally {
            MockDeadlock.executorService.shutdownNow();
        }

        if (!asExpected) {
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }
    }

    public class MockDeadlock {
        private static ExecutorService executorService = Executors.newCachedThreadPool();
        private static ReentrantLock lockA = new ReentrantLock();
        private static ReentrantLock lockB = new ReentrantLock();

        @SneakyThrows
        public static void startDeadlock() {
            executorService.execute(new ThreadOne());
            executorService.execute(new ThreadTwo());
            Thread.sleep(CHECK_STATUS_INTERVAL);
        }

        private static class ThreadOne implements Runnable {
            @Override
            public void run() {
                try {
                    lockA.lock();
                    System.out.println("ThreadOne acquired lockA");
                    Thread.sleep(100);
                    while (!lockB.tryLock(1, TimeUnit.SECONDS)) {
                        System.out.println("ThreadOne acquired lockB");
                    }
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                } finally {
                    lockA.unlock();
                }
            }
        }

        private static class ThreadTwo implements Runnable {
            @Override
            public void run() {
                try {
                    lockB.lock();
                    System.out.println("ThreadOne acquired lockB");
                    Thread.sleep(100);
                    while (!lockA.tryLock(1, TimeUnit.SECONDS)) {
                        System.out.println("ThreadOne acquired lockA");
                    }
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                } finally {
                    lockB.unlock();
                }
            }
        }
    }
}