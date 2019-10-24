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
package org.apache.bookkeeper.test;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.CharBuffer;
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
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            FileLock lock = fileChannel.lock();

            try {

                FileReader reader = new FileReader(lockFilename);
                CharBuffer buffer = CharBuffer.allocate(16);
                int len = reader.read(buffer);
                buffer.flip();

                int lastUsedPort = basePort;
                if (len > 0) {
                    String lastUsedPortStr = buffer.toString();
                    lastUsedPort = Integer.parseInt(lastUsedPortStr);
                }

                int freePort = probeFreePort(lastUsedPort + 1);

                FileWriter writer = new FileWriter(lockFilename);
                writer.write(Integer.toString(freePort));

                reader.close();
                writer.close();

                return freePort;

            } finally {
                lock.release();
                fileChannel.close();
            }
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

            try (ServerSocket ss = new ServerSocket()) {
                ss.setReuseAddress(false);
                ss.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
                ss.close();
                // Give it some time to truly close the connection
                Thread.sleep(100);
                return port;

            } catch (Exception e) {
                port++;
                exceptionCount++;
                if (exceptionCount > MAX_PORT_CONFLICTS) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
