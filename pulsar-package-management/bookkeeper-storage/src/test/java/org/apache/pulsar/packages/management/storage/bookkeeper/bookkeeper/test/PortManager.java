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
package org.apache.pulsar.packages.management.storage.bookkeeper.bookkeeper.test;

import lombok.Cleanup;

import java.io.IOException;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Port manager allows a base port to be specified on the commandline. Tests will then use ports, counting up from this
 * base port. This allows multiple instances of the bookkeeper tests to run at once.
 */
public class PortManager {

    private static final String lockFilename = System.getProperty("test.lockFilename",
            "/tmp/pulsar-test-port-manager.lock");
    private static final int basePort = Integer.parseInt(System.getProperty("test.basePort", "15000"));

    private static final int maxPort = 32000;

    /**
     * Return a TCP port that is currently unused.
     *
     * Keeps track of assigned ports and avoid race condition between different processes
     */
    public synchronized static int nextFreePort() {
        Path path = Paths.get(lockFilename);

        try {
            @Cleanup
            FileChannel fileChannel = FileChannel.open(path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.READ);

            @Cleanup
            FileLock lock = fileChannel.lock();

            ByteBuffer buffer = ByteBuffer.allocate(32);
            int len = fileChannel.read(buffer, 0L);
            buffer.flip();

            int lastUsedPort = basePort;
            if (len > 0) {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.get(bytes);
                String lastUsedPortStr = new String(bytes);
                lastUsedPort = Integer.parseInt(lastUsedPortStr);
            }

            int freePort = probeFreePort(lastUsedPort + 1);

            buffer.clear();
            buffer.put(Integer.toString(freePort).getBytes());
            buffer.flip();
            fileChannel.write(buffer, 0L);
            fileChannel.truncate(buffer.position());
            fileChannel.force(true);

            return freePort;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final int MAX_PORT_CONFLICTS = 10;

    private synchronized static int probeFreePort(int port) {
        int exceptionCount = 0;
        while (true) {
            if (port == maxPort) {
                // Rollover the port probe
                port = basePort;
            }

            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(Inet4Address.getLoopbackAddress(), port), 100);

                // If we succeed to connect it means the port is being used

            } catch (ConnectException e) {
                return port;
            } catch (Exception e) {
                e.printStackTrace();
            }

            port++;
            exceptionCount++;
            if (exceptionCount > MAX_PORT_CONFLICTS) {
                throw new RuntimeException("Failed to find an open port");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        while (true) {
            System.out.println("Port: " + nextFreePort());
            Thread.sleep(100);
        }
    }
}
