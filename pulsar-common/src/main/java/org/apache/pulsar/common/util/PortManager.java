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

/**
 * Allocates ports for tests that need a known port number BEFORE binding (e.g. tests that build
 * advertised-listener URLs, or that pre-create metadata znodes at the broker's would-be address).
 * For everything else, prefer binding to port 0 and reading the kernel-assigned port back.
 */
public class PortManager {

    private static final Set<Integer> PORTS = new HashSet<>();

    /**
     * Return a free port that is reserved for the caller until {@link #releaseLockedPort(int)}
     * is invoked.
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
     * Release a previously locked port.
     *
     * @return true if the port was previously locked by this manager
     */
    public static synchronized boolean releaseLockedPort(int lockedPort) {
        return PORTS.remove(lockedPort);
    }

    /**
     * @return true if the port is currently locked by this manager
     */
    public static synchronized boolean checkPortIfLocked(int lockedPort) {
        return PORTS.contains(lockedPort);
    }
}
