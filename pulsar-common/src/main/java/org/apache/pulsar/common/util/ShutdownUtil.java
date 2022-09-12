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

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;

@Slf4j
public class ShutdownUtil {
    /**
     * Triggers an immediate forceful shutdown of the current process.
     *
     * @param status Termination status. By convention, a nonzero status code indicates abnormal termination.
     * @see Runtime#halt(int)
     */
    public static void triggerImmediateForcefulShutdown(int status) {
        try {
            log.warn("Triggering immediate shutdown of current process with status {}", status, new Exception());
            // flush log buffers and shutdown log4j2 logging to prevent log truncation
            LogManager.shutdown();
        } finally {
            Runtime.getRuntime().halt(status);
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
