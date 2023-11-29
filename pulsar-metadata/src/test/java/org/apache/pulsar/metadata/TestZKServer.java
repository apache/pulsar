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
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.Properties;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.ContainerManager;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.embedded.ExitHandler;
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded;
import org.assertj.core.util.Files;

@Slf4j
public class TestZKServer implements AutoCloseable {

    public static final int TICK_TIME = 1000;

    private final File zkDataDir;
    private int zkPort; // initially this is zero
    private ZooKeeperServerEmbedded zooKeeperServerEmbedded;

    public TestZKServer() throws Exception {
        this.zkDataDir = Files.newTemporaryFolder();
        this.zkDataDir.deleteOnExit();
        // Allow all commands on ZK control port
        System.setProperty("zookeeper.4lw.commands.whitelist", "*");
        // disable the admin server as to not have any port conflicts
        System.setProperty("zookeeper.admin.enableServer", "false");
        start();
    }

    public void start() throws Exception {
        final Properties configZookeeper = new Properties();
        configZookeeper.put("clientPort", zkPort + "");
        configZookeeper.put("host", "127.0.0.1");
        configZookeeper.put("ticktime", TICK_TIME + "");
        zooKeeperServerEmbedded = ZooKeeperServerEmbedded
                .builder()
                .baseDir(zkDataDir.toPath())
                .configuration(configZookeeper)
                .exitHandler(ExitHandler.LOG_ONLY)
                .build();

        zooKeeperServerEmbedded.start(60_000);
        log.info("Started test ZK server on at {}", zooKeeperServerEmbedded.getConnectionString());

        ZooKeeperServerMain zooKeeperServerMain = getZooKeeperServerMain(zooKeeperServerEmbedded);
        ServerCnxnFactory serverCnxnFactory = getServerCnxnFactory(zooKeeperServerMain);
        // save the port, in order to allow restarting on the same port
        zkPort = serverCnxnFactory.getLocalPort();

        boolean zkServerReady = waitForServerUp(this.getConnectionString(), 30_000);
        assertTrue(zkServerReady);
    }

    @SneakyThrows
    private static ZooKeeperServerMain getZooKeeperServerMain(ZooKeeperServerEmbedded zooKeeperServerEmbedded) {
        ZooKeeperServerMain zooKeeperServerMain = readField(zooKeeperServerEmbedded.getClass(),
                "mainsingle", zooKeeperServerEmbedded);
        return zooKeeperServerMain;
    }

    @SneakyThrows
    private static ContainerManager getContainerManager(ZooKeeperServerMain zooKeeperServerMain) {
        ContainerManager containerManager = readField(ZooKeeperServerMain.class, "containerManager", zooKeeperServerMain);
        return containerManager;
    }

    @SneakyThrows
    private static ZooKeeperServer getZooKeeperServer(ZooKeeperServerMain zooKeeperServerMain) {
        ServerCnxnFactory serverCnxnFactory = getServerCnxnFactory(zooKeeperServerMain);
        ZooKeeperServer zkServer = readField(ServerCnxnFactory.class, "zkServer", serverCnxnFactory);
        return zkServer;
    }

    @SneakyThrows
    private static <T> T readField(Class clazz, String field, Object object) {
        Field declaredField = clazz.getDeclaredField(field);
        boolean accessible = declaredField.isAccessible();
        if (!accessible) {
            declaredField.setAccessible(true);
        }
        try {
            return (T) declaredField.get(object);
        } finally {
            declaredField.setAccessible(accessible);
        }
    }

    private static ServerCnxnFactory getServerCnxnFactory(ZooKeeperServerMain zooKeeperServerMain) throws Exception {
        ServerCnxnFactory serverCnxnFactory = readField(ZooKeeperServerMain.class, "cnxnFactory", zooKeeperServerMain);
        return serverCnxnFactory;
    }

    public void checkContainers() throws Exception {
        // Make sure the container nodes are actually deleted
        Thread.sleep(1000);

        ContainerManager containerManager = getContainerManager(getZooKeeperServerMain(zooKeeperServerEmbedded));
        containerManager.checkContainers();
    }

    public void stop() throws Exception {
        if (zooKeeperServerEmbedded != null) {
            zooKeeperServerEmbedded.close();
        }
        log.info("Stopped test ZK server");
    }

    public void expireSession(long sessionId) {
        getZooKeeperServer(getZooKeeperServerMain(zooKeeperServerEmbedded))
                .expire(sessionId);
    }

    @Override
    public void close() throws Exception {
        stop();
        FileUtils.deleteDirectory(zkDataDir);
    }

    @SneakyThrows
    public String getConnectionString() {
        return zooKeeperServerEmbedded.getConnectionString();
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
