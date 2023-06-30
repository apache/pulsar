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

import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

public class PortManager {

    private static final Set<Integer> PORTS = new HashSet<>();

    /**
     * Return a locked available port.
     *
     * @return locked available port.
     */
    public static synchronized int nextLockedFreePort() {
        int exceptionCount = 0;
        while (true) {
            try (ServerSocket ss = new ServerSocket(0)) {
                int port = ss.getLocalPort();
                if (!checkPortIfLocked(port)) {
                    PORTS.add(port);
                    return port;
                }
            } catch (Exception e) {
                exceptionCount++;
                if (exceptionCount > 100) {
                    throw new RuntimeException("Unable to allocate socket port", e);
                }
            }
        }
    }

    /**
     * Returns whether the port was released successfully.
     *
     * @return whether the release is successful.
     */
    public static synchronized boolean releaseLockedPort(int lockedPort) {
        return PORTS.remove(lockedPort);
    }

    /**
     * Check port if locked.
     *
     * @return whether the port is locked.
     */
    public static synchronized boolean checkPortIfLocked(int lockedPort) {
        return PORTS.contains(lockedPort);
    }
}
