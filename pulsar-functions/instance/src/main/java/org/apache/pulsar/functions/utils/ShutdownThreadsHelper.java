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

package org.apache.pulsar.functions.utils;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public enum ShutdownThreadsHelper {
    instance;

    private static final int SHUTDOWN_WAIT_MS = 3000;

    public static ShutdownThreadsHelper getInstance(){
        return instance;
    }

    public boolean shutdownThread(Thread thread){
        return shutdownThread(thread,SHUTDOWN_WAIT_MS);
    }

    public static boolean shutdownThread(Thread thread,
                                         long timeoutInMilliSeconds) {
        if (thread == null) {
            return true;
        }
        try {
            thread.interrupt();
            thread.join(timeoutInMilliSeconds);
            return true;
        } catch (InterruptedException ie) {
            log.warn("Interrupted while shutting down thread - " + thread.getName());
            Thread.currentThread().interrupt();
            return false;
        }
    }

    public static boolean shutdownExecutorService(ThreadPoolExecutor service) throws InterruptedException {
        return shutdownExecutorService(service, SHUTDOWN_WAIT_MS);
    }



    public static boolean shutdownExecutorService(ThreadPoolExecutor service, long timeoutInMs)
            throws InterruptedException {
        if (service == null) {
            return true;
        }
        service.shutdown();
        if (!service.awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS)) {
            service.shutdownNow();
            return service.awaitTermination(timeoutInMs, TimeUnit.MILLISECONDS);
        } else {
            return true;
        }
    }


}
