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
package org.apache.pulsar.common.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShutdownUtil {
    private static final Method log4j2ShutdownMethod;

    static {
        // use reflection to find org.apache.logging.log4j.LogManager.shutdown method
        Method shutdownMethod = null;
        try {
            shutdownMethod = Class.forName("org.apache.logging.log4j.LogManager")
                    .getMethod("shutdown");
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            // ignore when Log4j2 isn't found, log at debug level
            log.debug("Cannot find org.apache.logging.log4j.LogManager.shutdown method", e);
        }
        log4j2ShutdownMethod = shutdownMethod;
    }


    /**
     * Triggers an immediate forceful shutdown of the current process.
     *
     * @param status Termination status. By convention, a nonzero status code indicates abnormal termination.
     * @see Runtime#halt(int)
     */
    public static void triggerImmediateForcefulShutdown(int status) {
        try {
            if (status != 0) {
                log.warn("Triggering immediate shutdown of current process with status {}", status,
                        new Exception("Stacktrace for immediate shutdown"));
            }
            shutdownLogging();
        } finally {
            Runtime.getRuntime().halt(status);
        }
    }

    private static void shutdownLogging() {
        // flush log buffers and shutdown log4j2 logging to prevent log truncation
        if (log4j2ShutdownMethod != null) {
            try {
                // use reflection to call org.apache.logging.log4j.LogManager.shutdown()
                log4j2ShutdownMethod.invoke(null);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("Unable to call org.apache.logging.log4j.LogManager.shutdown using reflection.", e);
            }
        }
    }

    /**
     * Triggers an immediate forceful shutdown of the current process using 1 as the status code.
     *
     * @see Runtime#halt(int)
     */
    public static void triggerImmediateForcefulShutdown() {
        triggerImmediateForcefulShutdown(1);
    }
}
