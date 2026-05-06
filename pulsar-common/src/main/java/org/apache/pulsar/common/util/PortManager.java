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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

/**
 * Allocates ports for tests that need a known port number BEFORE binding (e.g. tests that build
 * advertised-listener URLs, or that pre-create metadata znodes at the broker's would-be address).
 * For everything else, prefer binding to port 0 and reading the kernel-assigned port back.
 *
 * <p>This manager allocates from outside the OS ephemeral range (Linux default 32768–60999), so
 * the kernel will not hand out our reserved ports to other processes binding port 0. To coordinate
 * across multiple test JVMs without a shared registry, each JVM claims a 1000-port block by
 * binding a "lock" {@link ServerSocket} on the block's base port for the JVM's lifetime; other
 * JVMs that hit the same range observe the bind failure and pick the next block.
 *
 * <p>Within the block, ports are tracked in a JVM-local set. Released ports are not returned to
 * the pool immediately because closed sockets can sit in {@code TIME_WAIT}; instead they move
 * back to the free list once a fresh bind to the port succeeds.
 */
public class PortManager {

    private static final int BLOCK_SIZE = 1000;
    private static final int FIRST_BLOCK_BASE = 20000;
    private static final int LAST_BLOCK_BASE = 32000;

    private static ServerSocket blockLock;
    private static int blockBase;
    private static final Set<Integer> usedPorts = new HashSet<>();
    private static final Set<Integer> pendingRelease = new HashSet<>();

    /**
     * Return a port that is currently free and is reserved for the caller until
     * {@link #releaseLockedPort(int)} is invoked.
     */
    public static synchronized int nextLockedFreePort() {
        ensureBlockReserved();
        // Reclaim ports whose underlying socket has finished its TIME_WAIT and is bindable again.
        pendingRelease.removeIf(PortManager::isPortBindable);

        for (int offset = 1; offset < BLOCK_SIZE; offset++) {
            int port = blockBase + offset;
            if (usedPorts.contains(port) || pendingRelease.contains(port)) {
                continue;
            }
            if (isPortBindable(port)) {
                usedPorts.add(port);
                return port;
            }
        }
        throw new RuntimeException("No free ports left in block " + blockBase
                + " (used=" + usedPorts.size() + ", pendingRelease=" + pendingRelease.size() + ")");
    }

    /**
     * Mark the given port as released. The port stays in a pending-release state and is not
     * handed out again until a future {@link #nextLockedFreePort()} verifies it can be re-bound.
     *
     * @return true if the port was previously locked by this manager
     */
    public static synchronized boolean releaseLockedPort(int lockedPort) {
        if (!usedPorts.remove(lockedPort)) {
            return false;
        }
        pendingRelease.add(lockedPort);
        return true;
    }

    /**
     * @return true if the port is currently locked by this manager
     */
    public static synchronized boolean checkPortIfLocked(int lockedPort) {
        return usedPorts.contains(lockedPort);
    }

    private static void ensureBlockReserved() {
        if (blockLock != null) {
            return;
        }
        for (int base = FIRST_BLOCK_BASE; base <= LAST_BLOCK_BASE; base += BLOCK_SIZE) {
            try {
                blockLock = new ServerSocket(base);
                blockBase = base;
                return;
            } catch (IOException ignored) {
                // block already taken (likely another test JVM); try the next one
            }
        }
        throw new RuntimeException("Unable to reserve a port block in ["
                + FIRST_BLOCK_BASE + ", " + (LAST_BLOCK_BASE + BLOCK_SIZE) + ")");
    }

    private static boolean isPortBindable(int port) {
        try (ServerSocket socket = new ServerSocket(port)) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
