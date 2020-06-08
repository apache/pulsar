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
package org.apache.pulsar.metadata;

import static org.testng.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.assertj.core.util.Files;

@Slf4j
public class TestZKServer implements AutoCloseable {

    private final ZooKeeperServer zks;
    private final File zkDataDir;
    private final ServerCnxnFactory serverFactory;

    public TestZKServer() throws Exception {
        this.zkDataDir = Files.newTemporaryFolder();
        this.zkDataDir.deleteOnExit();
        // Allow all commands on ZK control port
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        // disable the admin server as to not have any port conflicts
        System.setProperty("zookeeper.admin.enableServer", "false");
        this.zks = new ZooKeeperServer(zkDataDir, zkDataDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        this.serverFactory = new NIOServerCnxnFactory();
        this.serverFactory.configure(new InetSocketAddress(0), 1000);
        this.serverFactory.startup(zks, true);

        log.info("Started test ZK server on port {}", getPort());

        boolean zkServerReady = waitForServerUp(this.getConnectionString(), 30_000);
        assertTrue(zkServerReady);
    }

    @Override
    public void close() throws Exception {
        zks.shutdown();
        serverFactory.shutdown();
        FileUtils.deleteDirectory(zkDataDir);

        log.info("Stopped test ZK server");
    }

    public int getPort() {
        return serverFactory.getLocalPort();
    }

    public String getConnectionString() {
        return "127.0.0.1:" + getPort();
    }

    public static boolean waitForServerUp(String hp, long timeout) {
        long start = System.currentTimeMillis();
        String split[] = hp.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);
        while (true) {
            try {
                Socket sock = new Socket(host, port);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        log.info("ZK Server UP");
                        return true;
                    }
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException e) {
                // ignore as this is expected
                log.info("ZK server {} not up: {}", hp, e.getMessage());
            }

            if (System.currentTimeMillis() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }
}
