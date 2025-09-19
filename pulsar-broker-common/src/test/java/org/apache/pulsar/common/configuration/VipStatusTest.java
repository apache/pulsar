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
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import javax.servlet.ServletContext;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Files;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
public class VipStatusTest {

    public static final String ATTRIBUTE_STATUS_FILE_PATH = "statusFilePath";
    public static final String ATTRIBUTE_IS_READY_PROBE = "isReadyProbe";
    private static final long LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED = 10000L;
    // Rate limit status checks to every 500ms to prevent DoS
    private static final long CHECK_STATUS_INTERVAL = 500L;

    private ServletContext mockServletContext;
    private VipStatus vipStatus;
    private File file;

    @BeforeMethod
    public void setup() throws IOException {
        file = Files.newTemporaryFile();
        Supplier<Boolean> isReadyProbe = () -> true;
        mockServletContext = Mockito.mock(ServletContext.class);
        Mockito.when(mockServletContext.getAttribute(ATTRIBUTE_STATUS_FILE_PATH)).thenReturn(file.getAbsolutePath());
        Mockito.when(mockServletContext.getAttribute(ATTRIBUTE_IS_READY_PROBE)).thenReturn(isReadyProbe);
        vipStatus = new VipStatus(mockServletContext, LOG_THREADDUMP_INTERVAL_WHEN_DEADLOCK_DETECTED);
        VipStatus.reset();
    }

    @Test
    public void testVipStatusCheckStatus() {
        // No deadlocks
        testVipStatusCheckStatusWithoutDeadlock();
        // There is a deadlock
        testVipStatusCheckStatusWithDeadlock();
    }

    @AfterMethod(alwaysRun = true)
    public void release() throws IOException {
        if (file != null) {
            file.delete();
            file = null;
        }
    }

    @Test
    public void testVipStatusCheckStatusWithoutDeadlock() {
        assertEquals(vipStatus.checkStatus(), "OK");
    }

    @Test
    public void testVipStatusCheckStatusWithDeadlock() {
        MockDeadlock mockDeadlock = new MockDeadlock();
        boolean asExpected = true;
        try {
            mockDeadlock.startDeadlock();
            vipStatus.checkStatus();
            asExpected = false;
            System.out.println("Simulated deadlock, no deadlock detected, not as expected.");
        } catch (Exception wae) {
            System.out.println("Simulated deadlock and detected it, as expected.");
        } finally {
            mockDeadlock.close();
        }

        if (!asExpected) {
            throw new WebApplicationException(Response.Status.SERVICE_UNAVAILABLE);
        }
    }

    static class MockDeadlock implements Closeable {
        private ExecutorService executorService = Executors.newCachedThreadPool();
        private ReentrantLock lockA = new ReentrantLock();
        private ReentrantLock lockB = new ReentrantLock();
        private Phaser phaser = new Phaser(2);

        @SneakyThrows
        public void startDeadlock() {
            executorService.execute(new TaskOne());
            executorService.execute(new TaskTwo());
            Thread.sleep(CHECK_STATUS_INTERVAL);
        }

        @Override
        public void close() {
            executorService.shutdownNow();
        }

        private class TaskOne implements Runnable {
            @Override
            public void run() {
                try {
                    lockA.lock();
                    System.out.println("ThreadOne acquired lockA");
                    phaser.arriveAndAwaitAdvance();
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

        private class TaskTwo implements Runnable {
            @Override
            public void run() {
                try {
                    lockB.lock();
                    System.out.println("ThreadOne acquired lockB");
                    phaser.arriveAndAwaitAdvance();
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
